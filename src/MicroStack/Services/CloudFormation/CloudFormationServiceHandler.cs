using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Web;
using MicroStack.Internal;

namespace MicroStack.Services.CloudFormation;

/// <summary>
/// CloudFormation service handler — Query/XML protocol.
/// Orchestrates other MicroStack service handlers to provision resources from templates.
/// </summary>
internal sealed partial class CloudFormationServiceHandler : IServiceHandler
{
    public string ServiceName => "cloudformation";

    private const string CfnNs = "http://cloudformation.amazonaws.com/doc/2010-05-15/";

    private static string Region =>
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";

    private readonly ServiceRegistry _registry;

    // In-memory state
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _stacks = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, string>>> _stackEvents = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, string>> _exports = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _changeSets = new();

    private readonly Lock _lock = new();

    // Sentinel for AWS::NoValue
    private static readonly object NoValue = new();

    internal CloudFormationServiceHandler(ServiceRegistry registry)
    {
        _registry = registry;
    }

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var p = ParseParams(request);
        var contentType = request.GetHeader("content-type") ?? "";
        var target = request.GetHeader("x-amz-target") ?? "";

        // JSON protocol (newer SDKs): X-Amz-Target: CloudFormation_20100515.ActionName
        if (contentType.Contains("amz-json", StringComparison.OrdinalIgnoreCase)
            && target.StartsWith("CloudFormation_20100515.", StringComparison.Ordinal))
        {
            var actionName = target.Split('.')[^1];
            p["Action"] = [actionName];
            if (request.Body.Length > 0)
            {
                try
                {
                    using var doc = JsonDocument.Parse(request.Body);
                    foreach (var prop in doc.RootElement.EnumerateObject())
                    {
                        if (prop.Value.ValueKind == JsonValueKind.Array)
                        {
                            var vals = new List<string>();
                            foreach (var el in prop.Value.EnumerateArray())
                                vals.Add(el.ToString());
                            p[prop.Name] = vals.ToArray();
                        }
                        else
                        {
                            p[prop.Name] = [prop.Value.ToString()];
                        }
                    }
                }
                catch (JsonException)
                {
                    // ignore
                }
            }
        }

        var action = P(p, "Action");

        ServiceResponse response;
        lock (_lock)
        {
            response = action switch
            {
                "CreateStack" => CreateStack(p),
                "DescribeStacks" => DescribeStacks(p),
                "ListStacks" => ListStacks(p),
                "DeleteStack" => DeleteStack(p),
                "UpdateStack" => UpdateStack(p),
                "DescribeStackEvents" => DescribeStackEvents(p),
                "DescribeStackResource" => DescribeStackResource(p),
                "DescribeStackResources" => DescribeStackResources(p),
                "ListStackResources" => ListStackResources(p),
                "GetTemplate" => GetTemplate(p),
                "ValidateTemplate" => ValidateTemplate(p),
                "ListExports" => ListExports(),
                "CreateChangeSet" => CreateChangeSet(p),
                "DescribeChangeSet" => DescribeChangeSet(p),
                "ExecuteChangeSet" => ExecuteChangeSet(p),
                "DeleteChangeSet" => DeleteChangeSet(p),
                "ListChangeSets" => ListChangeSets(p),
                "GetTemplateSummary" => GetTemplateSummary(p),
                "UpdateTerminationProtection" => UpdateTerminationProtection(p),
                "SetStackPolicy" => SetStackPolicy(),
                "GetStackPolicy" => GetStackPolicy(),
                "ListImports" => ListImports(),
                _ => Error("InvalidAction", $"Unknown action: {action}", 400),
            };
        }

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _stacks.Clear();
            _stackEvents.Clear();
            _exports.Clear();
            _changeSets.Clear();
        }
    }

    public object? GetState() => null;
    public void RestoreState(object state) { }

    // ══════════════════════════════════════════════════════════════════════════
    // Stack Events
    // ══════════════════════════════════════════════════════════════════════════

    private void AddEvent(string stackId, string stackName, string logicalId,
        string resourceType, string status, string reason = "", string physicalId = "")
    {
        var ev = new Dictionary<string, string>
        {
            ["StackId"] = stackId,
            ["StackName"] = stackName,
            ["EventId"] = Guid.NewGuid().ToString(),
            ["LogicalResourceId"] = logicalId,
            ["PhysicalResourceId"] = physicalId,
            ["ResourceType"] = resourceType,
            ["ResourceStatus"] = status,
            ["ResourceStatusReason"] = reason,
            ["Timestamp"] = TimeHelpers.NowIso(),
        };
        if (!_stackEvents.ContainsKey(stackId))
            _stackEvents[stackId] = [];
        _stackEvents[stackId].Add(ev);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Template Parsing
    // ══════════════════════════════════════════════════════════════════════════

    private static Dictionary<string, object?> ParseTemplate(string templateBody)
    {
        templateBody = templateBody.Trim();
        // JSON only (YAML not supported in C# port)
        if (!templateBody.StartsWith('{'))
        {
            throw new InvalidOperationException("Template must be JSON (YAML not supported)");
        }

        using var doc = JsonDocument.Parse(templateBody);
        return JsonElementToDict(doc.RootElement);
    }

    private static Dictionary<string, object?> JsonElementToDict(JsonElement el)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var prop in el.EnumerateObject())
        {
            dict[prop.Name] = JsonElementToObject(prop.Value);
        }
        return dict;
    }

    private static object? JsonElementToObject(JsonElement el)
    {
        return el.ValueKind switch
        {
            JsonValueKind.Object => JsonElementToDict(el),
            JsonValueKind.Array => el.EnumerateArray().Select(JsonElementToObject).ToList<object?>(),
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? l : el.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    private static Dictionary<string, object?> GetDict(object? obj)
    {
        if (obj is Dictionary<string, object?> d) return d;
        return new Dictionary<string, object?>(StringComparer.Ordinal);
    }

    private static Dictionary<string, Dictionary<string, object?>> ToResourceDict(Dictionary<string, object?> flat)
    {
        var result = new Dictionary<string, Dictionary<string, object?>>(flat.Count, StringComparer.Ordinal);
        foreach (var (key, value) in flat)
        {
            result[key] = GetDict(value);
        }
        return result;
    }

    private static List<object?> GetList(object? obj)
    {
        if (obj is List<object?> l) return l;
        return [];
    }

    private static string GetString(object? obj)
    {
        if (obj is string s) return s;
        if (obj is null) return "";
        return obj.ToString() ?? "";
    }

    private static bool GetBool(object? obj)
    {
        if (obj is bool b) return b;
        if (obj is string s) return s.Equals("true", StringComparison.OrdinalIgnoreCase);
        return false;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Parameter Resolution
    // ══════════════════════════════════════════════════════════════════════════

    private static Dictionary<string, Dictionary<string, object?>> ResolveParameters(
        Dictionary<string, object?> template, List<Dictionary<string, string>> providedParams)
    {
        var paramDefs = GetDict(template.GetValueOrDefault("Parameters"));
        var providedMap = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var p in providedParams)
        {
            if (p.TryGetValue("Key", out var key))
                providedMap[key] = p.GetValueOrDefault("Value", "");
        }

        var resolved = new Dictionary<string, Dictionary<string, object?>>(StringComparer.Ordinal);

        foreach (var (name, defnObj) in paramDefs)
        {
            var defn = GetDict(defnObj);
            var noEcho = GetString(defn.GetValueOrDefault("NoEcho")).Equals("true", StringComparison.OrdinalIgnoreCase);

            string value;
            if (providedMap.TryGetValue(name, out var provided))
            {
                value = provided;
            }
            else if (defn.ContainsKey("Default"))
            {
                value = GetString(defn["Default"]);
            }
            else
            {
                throw new InvalidOperationException($"Parameter '{name}' has no Default and was not provided");
            }

            // Validate AllowedValues
            if (defn.TryGetValue("AllowedValues", out var allowedObj) && allowedObj is List<object?> allowed)
            {
                var allowedStrs = allowed.Select(a => GetString(a)).ToList();
                if (!allowedStrs.Contains(value))
                    throw new InvalidOperationException(
                        $"Parameter '{name}' value '{value}' is not in AllowedValues: [{string.Join(", ", allowedStrs)}]");
            }

            // Type validation
            var ptype = GetString(defn.GetValueOrDefault("Type", "String"));
            if (ptype == "Number" && !double.TryParse(value, out _))
                throw new InvalidOperationException($"Parameter '{name}' value '{value}' is not a valid Number");

            resolved[name] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Value"] = value,
                ["NoEcho"] = noEcho,
            };
        }

        return resolved;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Condition Evaluation
    // ══════════════════════════════════════════════════════════════════════════

    private static Dictionary<string, bool> EvaluateConditions(
        Dictionary<string, object?> template,
        Dictionary<string, Dictionary<string, object?>> paramValues)
    {
        var condDefs = GetDict(template.GetValueOrDefault("Conditions"));
        var evaluated = new Dictionary<string, bool>(StringComparer.Ordinal);

        bool Eval(object? expr)
        {
            if (expr is Dictionary<string, object?> dict)
            {
                if (dict.TryGetValue("Fn::Equals", out var eqArgs))
                {
                    var args = GetList(eqArgs);
                    var left = ResolveCondValue(args.Count > 0 ? args[0] : null);
                    var right = ResolveCondValue(args.Count > 1 ? args[1] : null);
                    return GetString(left) == GetString(right);
                }
                if (dict.TryGetValue("Fn::And", out var andArgs))
                    return GetList(andArgs).All(Eval);
                if (dict.TryGetValue("Fn::Or", out var orArgs))
                    return GetList(orArgs).Any(Eval);
                if (dict.TryGetValue("Fn::Not", out var notArgs))
                    return !Eval(GetList(notArgs).FirstOrDefault());
                if (dict.TryGetValue("Condition", out var condRef))
                {
                    var cname = GetString(condRef);
                    if (!evaluated.ContainsKey(cname) && condDefs.ContainsKey(cname))
                        evaluated[cname] = Eval(condDefs[cname]);
                    return evaluated.GetValueOrDefault(cname);
                }
                if (dict.TryGetValue("Ref", out _))
                    return GetBool(ResolveCondValue(expr));
            }
            return GetBool(expr);
        }

        object? ResolveCondValue(object? val)
        {
            if (val is Dictionary<string, object?> dict)
            {
                if (dict.TryGetValue("Ref", out var refName))
                {
                    var pname = GetString(refName);
                    if (paramValues.TryGetValue(pname, out var pval))
                        return GetString(pval.GetValueOrDefault("Value"));
                    return pname;
                }
                if (dict.ContainsKey("Fn::Equals"))
                    return Eval(val);
                if (dict.ContainsKey("Condition"))
                    return Eval(val);
            }
            return val;
        }

        foreach (var (name, defn) in condDefs)
        {
            if (!evaluated.ContainsKey(name))
                evaluated[name] = Eval(defn);
        }

        return evaluated;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Intrinsic Function Resolution
    // ══════════════════════════════════════════════════════════════════════════

    private object? ResolveRefs(object? value,
        Dictionary<string, Dictionary<string, object?>> resources,
        Dictionary<string, Dictionary<string, object?>> paramValues,
        Dictionary<string, bool> conditions,
        Dictionary<string, object?> mappings,
        string stackName, string stackId)
    {
        if (value is string) return value;

        if (value is List<object?> list)
        {
            var resolved = list.Select(item =>
                ResolveRefs(item, resources, paramValues, conditions, mappings, stackName, stackId))
                .Where(r => !ReferenceEquals(r, NoValue))
                .ToList<object?>();
            return resolved;
        }

        if (value is not Dictionary<string, object?> dict) return value;

        // Ref
        if (dict.TryGetValue("Ref", out var refVal))
        {
            var refName = GetString(refVal);
            var pseudo = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["AWS::StackName"] = stackName,
                ["AWS::StackId"] = stackId,
                ["AWS::Region"] = Region,
                ["AWS::AccountId"] = AccountContext.GetAccountId(),
                ["AWS::NoValue"] = NoValue,
                ["AWS::URLSuffix"] = "amazonaws.com",
                ["AWS::Partition"] = "aws",
                ["AWS::NotificationARNs"] = new List<object?>(),
            };
            if (pseudo.TryGetValue(refName, out var pseudoVal)) return pseudoVal;
            if (paramValues.TryGetValue(refName, out var paramVal))
                return GetString(paramVal.GetValueOrDefault("Value"));
            if (resources.TryGetValue(refName, out var resDict) && resDict.ContainsKey("PhysicalResourceId"))
                return GetString(resDict["PhysicalResourceId"]);
            return refName;
        }

        // Fn::GetAtt
        if (dict.TryGetValue("Fn::GetAtt", out var getAttVal))
        {
            string logicalId, attr;
            if (getAttVal is string getAttStr)
            {
                var parts = getAttStr.Split('.', 2);
                logicalId = parts[0];
                attr = parts.Length > 1 ? parts[1] : "";
            }
            else
            {
                var args = GetList(getAttVal);
                logicalId = GetString(args.Count > 0 ? args[0] : null);
                attr = GetString(args.Count > 1 ? args[1] : null);
            }
            if (resources.TryGetValue(logicalId, out var res))
            {
                var attrs = GetDict(res.GetValueOrDefault("Attributes"));
                if (attrs.TryGetValue(attr, out var attrVal))
                    return GetString(attrVal);
                return GetString(res.GetValueOrDefault("PhysicalResourceId"));
            }
            return "";
        }

        // Fn::Join
        if (dict.TryGetValue("Fn::Join", out var joinVal))
        {
            var args = GetList(joinVal);
            var delimiter = GetString(args.Count > 0 ? args[0] : null);
            var items = ResolveRefs(args.Count > 1 ? args[1] : null,
                resources, paramValues, conditions, mappings, stackName, stackId);
            if (items is List<object?> itemList)
                return string.Join(delimiter, itemList.Where(i => !ReferenceEquals(i, NoValue)).Select(GetString));
            return GetString(items);
        }

        // Fn::Sub
        if (dict.TryGetValue("Fn::Sub", out var subVal))
        {
            string templateStr;
            Dictionary<string, object?> resolvedMap;
            if (subVal is List<object?> subList)
            {
                templateStr = GetString(subList.Count > 0 ? subList[0] : null);
                var varMap = subList.Count > 1 ? GetDict(subList[1]) : new Dictionary<string, object?>();
                resolvedMap = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var (k, v) in varMap)
                    resolvedMap[k] = ResolveRefs(v, resources, paramValues, conditions, mappings, stackName, stackId);
            }
            else
            {
                templateStr = GetString(subVal);
                resolvedMap = new Dictionary<string, object?>(StringComparer.Ordinal);
            }

            return SubRegex().Replace(templateStr, match =>
            {
                var varName = match.Groups[1].Value;
                if (resolvedMap.TryGetValue(varName, out var mapVal))
                    return GetString(mapVal);

                // Pseudo parameters
                if (varName == "AWS::StackName") return stackName;
                if (varName == "AWS::StackId") return stackId;
                if (varName == "AWS::Region") return Region;
                if (varName == "AWS::AccountId") return AccountContext.GetAccountId();
                if (varName == "AWS::URLSuffix") return "amazonaws.com";
                if (varName == "AWS::Partition") return "aws";

                if (paramValues.TryGetValue(varName, out var pv))
                    return GetString(pv.GetValueOrDefault("Value"));

                // Resource.Attr
                if (varName.Contains('.'))
                {
                    var parts = varName.Split('.', 2);
                    if (resources.TryGetValue(parts[0], out var resData))
                    {
                        var attrs = GetDict(resData.GetValueOrDefault("Attributes"));
                        if (attrs.TryGetValue(parts[1], out var attrVal))
                            return GetString(attrVal);
                        return GetString(resData.GetValueOrDefault("PhysicalResourceId", varName));
                    }
                }

                // Resource physical ID
                if (resources.TryGetValue(varName, out var rd) && rd.ContainsKey("PhysicalResourceId"))
                    return GetString(rd["PhysicalResourceId"]);

                return varName;
            });
        }

        // Fn::Select
        if (dict.TryGetValue("Fn::Select", out var selectVal))
        {
            var args = GetList(selectVal);
            var indexObj = ResolveRefs(args.Count > 0 ? args[0] : null,
                resources, paramValues, conditions, mappings, stackName, stackId);
            var index = int.Parse(GetString(indexObj));
            var items = ResolveRefs(args.Count > 1 ? args[1] : null,
                resources, paramValues, conditions, mappings, stackName, stackId);
            if (items is string itemStr)
            {
                var split = itemStr.Split(',').Select(s => s.Trim()).ToList();
                return index >= 0 && index < split.Count ? split[index] : "";
            }
            if (items is List<object?> itemList)
                return index >= 0 && index < itemList.Count ? itemList[index] : "";
            return "";
        }

        // Fn::Split
        if (dict.TryGetValue("Fn::Split", out var splitVal))
        {
            var args = GetList(splitVal);
            var delimiter = GetString(args.Count > 0 ? args[0] : null);
            var source = ResolveRefs(args.Count > 1 ? args[1] : null,
                resources, paramValues, conditions, mappings, stackName, stackId);
            return GetString(source).Split(delimiter).Select(s => (object?)s).ToList();
        }

        // Fn::If
        if (dict.TryGetValue("Fn::If", out var ifVal))
        {
            var args = GetList(ifVal);
            var condName = GetString(args.Count > 0 ? args[0] : null);
            var condResult = conditions.GetValueOrDefault(condName);
            var branch = condResult ? (args.Count > 1 ? args[1] : null) : (args.Count > 2 ? args[2] : null);
            return ResolveRefs(branch, resources, paramValues, conditions, mappings, stackName, stackId);
        }

        // Fn::Base64
        if (dict.TryGetValue("Fn::Base64", out var b64Val))
        {
            var inner = ResolveRefs(b64Val, resources, paramValues, conditions, mappings, stackName, stackId);
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(GetString(inner)));
        }

        // Fn::FindInMap
        if (dict.TryGetValue("Fn::FindInMap", out var fimVal))
        {
            var args = GetList(fimVal);
            var mapName = GetString(ResolveRefs(args.Count > 0 ? args[0] : null,
                resources, paramValues, conditions, mappings, stackName, stackId));
            var key1 = GetString(ResolveRefs(args.Count > 1 ? args[1] : null,
                resources, paramValues, conditions, mappings, stackName, stackId));
            var key2 = GetString(ResolveRefs(args.Count > 2 ? args[2] : null,
                resources, paramValues, conditions, mappings, stackName, stackId));
            var map = GetDict(mappings.GetValueOrDefault(mapName));
            var subMap = GetDict(map.GetValueOrDefault(key1));
            return GetString(subMap.GetValueOrDefault(key2));
        }

        // Fn::ImportValue
        if (dict.TryGetValue("Fn::ImportValue", out var impVal))
        {
            var exportName = GetString(ResolveRefs(impVal,
                resources, paramValues, conditions, mappings, stackName, stackId));
            if (_exports.TryGetValue(exportName, out var exportData))
                return exportData.GetValueOrDefault("Value", "");
            throw new InvalidOperationException($"Export '{exportName}' not found");
        }

        // Fn::GetAZs
        if (dict.TryGetValue("Fn::GetAZs", out var azVal))
        {
            var region = GetString(ResolveRefs(azVal,
                resources, paramValues, conditions, mappings, stackName, stackId));
            if (string.IsNullOrEmpty(region)) region = Region;
            return new List<object?> { $"{region}a", $"{region}b", $"{region}c" };
        }

        // Fn::Cidr
        if (dict.TryGetValue("Fn::Cidr", out var cidrVal))
        {
            var args = GetList(cidrVal);
            var count = int.Parse(GetString(ResolveRefs(args.Count > 1 ? args[1] : null,
                resources, paramValues, conditions, mappings, stackName, stackId)));
            var cidrBits = int.Parse(GetString(ResolveRefs(args.Count > 2 ? args[2] : null,
                resources, paramValues, conditions, mappings, stackName, stackId)));
            return Enumerable.Range(0, count).Select(i => (object?)$"10.0.{i}.0/{32 - cidrBits}").ToList();
        }

        // Fn::Equals (non-condition context)
        if (dict.TryGetValue("Fn::Equals", out var eqVal))
        {
            var args = GetList(eqVal);
            var left = GetString(ResolveRefs(args.Count > 0 ? args[0] : null,
                resources, paramValues, conditions, mappings, stackName, stackId));
            var right = GetString(ResolveRefs(args.Count > 1 ? args[1] : null,
                resources, paramValues, conditions, mappings, stackName, stackId));
            return left == right;
        }

        // Condition reference
        if (dict.TryGetValue("Condition", out var condRefVal) && dict.Count == 1)
            return conditions.GetValueOrDefault(GetString(condRefVal));

        // Recurse plain dict
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (k, v) in dict)
        {
            var resolved = ResolveRefs(v, resources, paramValues, conditions, mappings, stackName, stackId);
            if (!ReferenceEquals(resolved, NoValue))
                result[k] = resolved;
        }
        return result;
    }

    [GeneratedRegex(@"\$\{([^}]+)\}")]
    private static partial Regex SubRegex();

    // ══════════════════════════════════════════════════════════════════════════
    // Dependency Extraction & Topological Sort
    // ══════════════════════════════════════════════════════════════════════════

    private static HashSet<string> ExtractDeps(Dictionary<string, object?> resourceDef, HashSet<string> allNames)
    {
        var deps = new HashSet<string>(StringComparer.Ordinal);

        void Walk(object? obj)
        {
            if (obj is Dictionary<string, object?> dict)
            {
                if (dict.TryGetValue("Ref", out var refVal))
                {
                    var r = GetString(refVal);
                    if (allNames.Contains(r)) deps.Add(r);
                }
                if (dict.TryGetValue("Fn::GetAtt", out var gaVal))
                {
                    if (gaVal is List<object?> gaList && gaList.Count > 0)
                    {
                        var logId = GetString(gaList[0]);
                        if (allNames.Contains(logId)) deps.Add(logId);
                    }
                    else if (gaVal is string gaStr)
                    {
                        var logId = gaStr.Split('.')[0];
                        if (allNames.Contains(logId)) deps.Add(logId);
                    }
                }
                if (dict.TryGetValue("Fn::Sub", out var subVal))
                {
                    var templateStr = subVal is List<object?> subList
                        ? GetString(subList.Count > 0 ? subList[0] : null)
                        : GetString(subVal);
                    foreach (Match m in SubRegex().Matches(templateStr))
                    {
                        var varName = m.Groups[1].Value;
                        var baseName = varName.Split('.')[0];
                        if (allNames.Contains(baseName)) deps.Add(baseName);
                    }
                }
                if (dict.TryGetValue("Fn::If", out var ifVal))
                {
                    var args = GetList(ifVal);
                    for (var i = 1; i < args.Count; i++) Walk(args[i]);
                }
                foreach (var (k, v) in dict)
                {
                    if (k is not ("Ref" or "Fn::GetAtt" or "Fn::Sub" or "Fn::If"))
                        Walk(v);
                }
            }
            else if (obj is List<object?> list)
            {
                foreach (var item in list) Walk(item);
            }
        }

        // DependsOn
        var dependsOn = resourceDef.GetValueOrDefault("DependsOn");
        if (dependsOn is string depStr)
        {
            if (allNames.Contains(depStr)) deps.Add(depStr);
        }
        else if (dependsOn is List<object?> depList)
        {
            foreach (var d in depList)
            {
                var ds = GetString(d);
                if (allNames.Contains(ds)) deps.Add(ds);
            }
        }

        Walk(GetDict(resourceDef.GetValueOrDefault("Properties")));
        return deps;
    }

    private static List<string> TopologicalSort(Dictionary<string, object?> resources, Dictionary<string, bool> conditions)
    {
        var allNames = new HashSet<string>(resources.Keys, StringComparer.Ordinal);
        var active = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (name, defnObj) in resources)
        {
            var defn = GetDict(defnObj);
            var cond = GetString(defn.GetValueOrDefault("Condition"));
            if (!string.IsNullOrEmpty(cond) && conditions.TryGetValue(cond, out var cv) && !cv)
                continue;
            active.Add(name);
        }

        var inDegree = new Dictionary<string, int>(StringComparer.Ordinal);
        var adj = new Dictionary<string, List<string>>(StringComparer.Ordinal);

        foreach (var name in active)
        {
            inDegree[name] = 0;
            adj[name] = [];
        }

        foreach (var name in active)
        {
            var resDef = GetDict(resources[name]);
            var depsSet = ExtractDeps(resDef, active);
            foreach (var dep in depsSet)
            {
                if (active.Contains(dep) && dep != name)
                {
                    adj[dep].Add(name);
                    inDegree[name]++;
                }
            }
        }

        var queue = new SortedSet<string>(
            active.Where(n => inDegree[n] == 0), StringComparer.Ordinal);
        var result = new List<string>();

        while (queue.Count > 0)
        {
            var node = queue.Min!;
            queue.Remove(node);
            result.Add(node);
            foreach (var neighbor in adj[node])
            {
                inDegree[neighbor]--;
                if (inDegree[neighbor] == 0)
                    queue.Add(neighbor);
            }
        }

        if (result.Count != active.Count)
        {
            var remaining = active.Except(result);
            throw new InvalidOperationException(
                $"Circular dependency detected among resources: {string.Join(", ", remaining.Order())}");
        }

        return result;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Resource Provisioning
    // ══════════════════════════════════════════════════════════════════════════

    private static string PhysicalName(string stackName, string logicalId, bool lowercase = false, int maxLen = 128)
    {
        var suffix = Guid.NewGuid().ToString("N")[..13].ToUpperInvariant();
        var baseName = $"{stackName}-{logicalId}-{suffix}";
        if (lowercase) baseName = baseName.ToLowerInvariant();
        return baseName.Length > maxLen ? baseName[..maxLen] : baseName;
    }

    private (string PhysicalId, Dictionary<string, object?> Attributes) ProvisionResource(
        string resourceType, string logicalId, Dictionary<string, object?> props, string stackName)
    {
        return resourceType switch
        {
            "AWS::S3::Bucket" => ProvisionS3Bucket(logicalId, props, stackName),
            "AWS::S3::BucketPolicy" => ProvisionS3BucketPolicy(logicalId, props, stackName),
            "AWS::SQS::Queue" => ProvisionSqsQueue(logicalId, props, stackName),
            "AWS::SNS::Topic" => ProvisionSnsTopic(logicalId, props, stackName),
            "AWS::SNS::Subscription" => ProvisionSnsSubscription(logicalId, props, stackName),
            "AWS::DynamoDB::Table" => ProvisionDynamoDbTable(logicalId, props, stackName),
            "AWS::Lambda::Function" => ProvisionLambdaFunction(logicalId, props, stackName),
            "AWS::IAM::Role" => ProvisionIamRole(logicalId, props, stackName),
            "AWS::IAM::Policy" => ProvisionIamPolicy(logicalId, props, stackName),
            "AWS::IAM::InstanceProfile" => ProvisionIamInstanceProfile(logicalId, props, stackName),
            "AWS::IAM::ManagedPolicy" => ProvisionIamManagedPolicy(logicalId, props, stackName),
            "AWS::SSM::Parameter" => ProvisionSsmParameter(logicalId, props, stackName),
            "AWS::Logs::LogGroup" => ProvisionLogGroup(logicalId, props, stackName),
            "AWS::Events::Rule" => ProvisionEventBridgeRule(logicalId, props, stackName),
            "AWS::Kinesis::Stream" => ProvisionKinesisStream(logicalId, props, stackName),
            "AWS::Lambda::Permission" => ProvisionLambdaPermission(logicalId, props, stackName),
            "AWS::Lambda::Version" => ProvisionLambdaVersion(logicalId, props, stackName),
            "AWS::Lambda::EventSourceMapping" => ProvisionLambdaEsm(logicalId, props, stackName),
            "AWS::Lambda::Alias" => ProvisionLambdaAlias(logicalId, props, stackName),
            "AWS::SQS::QueuePolicy" => ProvisionSqsQueuePolicy(logicalId, props, stackName),
            "AWS::SNS::TopicPolicy" => ProvisionSnsTopicPolicy(logicalId, props, stackName),
            "AWS::SecretsManager::Secret" => ProvisionSecret(logicalId, props, stackName),
            "AWS::KMS::Key" => ProvisionKmsKey(logicalId, props, stackName),
            "AWS::KMS::Alias" => ProvisionKmsAlias(logicalId, props, stackName),
            "AWS::EC2::VPC" => ProvisionEc2Vpc(logicalId, props, stackName),
            "AWS::EC2::Subnet" => ProvisionEc2Subnet(logicalId, props, stackName),
            "AWS::EC2::SecurityGroup" => ProvisionEc2SecurityGroup(logicalId, props, stackName),
            "AWS::EC2::InternetGateway" => ProvisionEc2InternetGateway(logicalId, props, stackName),
            "AWS::EC2::VPCGatewayAttachment" => ProvisionEc2VpcGwAttachment(logicalId, props, stackName),
            "AWS::EC2::RouteTable" => ProvisionEc2RouteTable(logicalId, props, stackName),
            "AWS::EC2::Route" => ProvisionEc2Route(logicalId, props, stackName),
            "AWS::EC2::SubnetRouteTableAssociation" => ProvisionEc2SubnetRtbAssoc(logicalId, props, stackName),
            "AWS::EC2::LaunchTemplate" => ProvisionEc2LaunchTemplate(logicalId, props, stackName),
            "AWS::ECR::Repository" => ProvisionEcrRepository(logicalId, props, stackName),
            "AWS::ECS::Cluster" => ProvisionEcsCluster(logicalId, props, stackName),
            "AWS::ECS::TaskDefinition" => ProvisionEcsTaskDefinition(logicalId, props, stackName),
            "AWS::ECS::Service" => ProvisionEcsService(logicalId, props, stackName),
            "AWS::ElasticLoadBalancingV2::LoadBalancer" => ProvisionElbv2LoadBalancer(logicalId, props, stackName),
            "AWS::ElasticLoadBalancingV2::Listener" => ProvisionElbv2Listener(logicalId, props, stackName),
            "AWS::Cognito::UserPool" => ProvisionCognitoUserPool(logicalId, props, stackName),
            "AWS::Cognito::UserPoolClient" => ProvisionCognitoUserPoolClient(logicalId, props, stackName),
            "AWS::Cognito::IdentityPool" => ProvisionCognitoIdentityPool(logicalId, props, stackName),
            "AWS::Cognito::UserPoolDomain" => ProvisionCognitoUserPoolDomain(logicalId, props, stackName),
            "AWS::ApiGateway::RestApi" => ProvisionApiGwRestApi(logicalId, props, stackName),
            "AWS::ApiGateway::Resource" => ProvisionApiGwResource(logicalId, props, stackName),
            "AWS::ApiGateway::Method" => ProvisionApiGwMethod(logicalId, props, stackName),
            "AWS::ApiGateway::Deployment" => ProvisionApiGwDeployment(logicalId, props, stackName),
            "AWS::ApiGateway::Stage" => ProvisionApiGwStage(logicalId, props, stackName),
            "AWS::AppSync::GraphQLApi" => ProvisionAppSyncApi(logicalId, props, stackName),
            "AWS::AppSync::DataSource" => ProvisionAppSyncDataSource(logicalId, props, stackName),
            "AWS::AppSync::Resolver" => ProvisionAppSyncResolver(logicalId, props, stackName),
            "AWS::AppSync::GraphQLSchema" => ProvisionAppSyncSchema(logicalId, props, stackName),
            "AWS::AppSync::ApiKey" => ProvisionAppSyncApiKey(logicalId, props, stackName),
            "AWS::CloudFormation::WaitCondition" => ProvisionWaitCondition(logicalId, stackName),
            "AWS::CloudFormation::WaitConditionHandle" => ProvisionWaitConditionHandle(logicalId, stackName),
            "AWS::CDK::Metadata" => ($"CDKMetadata-{logicalId}", new Dictionary<string, object?>()),
            var t when t.StartsWith("AWS::CloudFormation::", StringComparison.Ordinal) =>
                ($"{stackName}-{logicalId}-noop-{Guid.NewGuid().ToString()[..8]}", new Dictionary<string, object?>()),
            var t when t.StartsWith("AWS::AutoScaling::", StringComparison.Ordinal) =>
                ($"as-{logicalId}", new Dictionary<string, object?>()),
            _ => throw new InvalidOperationException($"Unsupported resource type: {resourceType}"),
        };
    }

    private void DeleteResource(string resourceType, string physicalId, Dictionary<string, object?> props)
    {
        // Dispatch delete through the appropriate service handler
        try
        {
            switch (resourceType)
            {
                case "AWS::S3::Bucket": DeleteViaHandler("s3", BuildDeleteBucketRequest(physicalId)); break;
                case "AWS::SQS::Queue": DeleteViaHandler("sqs", BuildDeleteQueueRequest(physicalId)); break;
                case "AWS::SNS::Topic": DeleteViaHandler("sns", BuildDeleteTopicRequest(physicalId)); break;
                case "AWS::DynamoDB::Table": DeleteViaHandler("dynamodb", BuildDeleteTableRequest(physicalId)); break;
                case "AWS::Lambda::Function": DeleteViaHandler("lambda", BuildDeleteFunctionRequest(physicalId)); break;
                case "AWS::IAM::Role": DeleteViaHandler("iam", BuildDeleteRoleRequest(physicalId)); break;
                case "AWS::SSM::Parameter": DeleteViaHandler("ssm", BuildDeleteParameterRequest(physicalId)); break;
                case "AWS::Logs::LogGroup": DeleteViaHandler("logs", BuildDeleteLogGroupRequest(physicalId)); break;
                case "AWS::Events::Rule": DeleteViaHandler("events", BuildDeleteRuleRequest(physicalId, props)); break;
                case "AWS::Kinesis::Stream": DeleteViaHandler("kinesis", BuildDeleteStreamRequest(physicalId)); break;
                case "AWS::SecretsManager::Secret": DeleteViaHandler("secretsmanager", BuildDeleteSecretRequest(physicalId)); break;
                case "AWS::KMS::Key": DeleteViaHandler("kms", BuildDeleteKmsKeyRequest(physicalId)); break;
                case "AWS::EC2::VPC":
                case "AWS::EC2::Subnet":
                case "AWS::EC2::SecurityGroup":
                case "AWS::EC2::InternetGateway":
                case "AWS::EC2::RouteTable":
                case "AWS::EC2::LaunchTemplate":
                    DeleteEc2Resource(resourceType, physicalId, props); break;
                case "AWS::ECR::Repository": DeleteViaHandler("ecr", BuildDeleteEcrRepoRequest(physicalId)); break;
                case "AWS::ECS::Cluster": DeleteViaHandler("ecs", BuildDeleteEcsClusterRequest(physicalId)); break;
                case "AWS::ElasticLoadBalancingV2::LoadBalancer":
                    DeleteViaHandler("elasticloadbalancing", BuildDeleteLbRequest(physicalId)); break;
                case "AWS::ElasticLoadBalancingV2::Listener":
                    DeleteViaHandler("elasticloadbalancing", BuildDeleteListenerRequest(physicalId)); break;
                // No-op for types without delete handlers
                default: break;
            }
        }
        catch
        {
            // swallow delete errors (matching Python behavior)
        }
    }

    private void DeleteViaHandler(string serviceName, ServiceRequest request)
    {
        var handler = _registry.Resolve(serviceName);
        handler?.HandleAsync(request).GetAwaiter().GetResult();
    }

    private ServiceRequest CallHandler(string serviceName, ServiceRequest request)
    {
        var handler = _registry.Resolve(serviceName);
        handler?.HandleAsync(request).GetAwaiter().GetResult();
        return request;
    }

    private ServiceResponse CallHandlerGetResponse(string serviceName, ServiceRequest request)
    {
        var handler = _registry.Resolve(serviceName);
        if (handler is null)
            throw new InvalidOperationException($"Service handler '{serviceName}' not found");
        return handler.HandleAsync(request).GetAwaiter().GetResult();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Stack Deploy / Delete Logic (synchronous in C# port)
    // ══════════════════════════════════════════════════════════════════════════

    private void DeployStack(string stackName, string stackId, Dictionary<string, object?> template,
        Dictionary<string, Dictionary<string, object?>> paramValues, bool disableRollback)
    {
        DeployStack(stackName, stackId, template, paramValues, disableRollback, false, null);
    }

    private void DeployStack(string stackName, string stackId, Dictionary<string, object?> template,
        Dictionary<string, Dictionary<string, object?>> paramValues, bool disableRollback,
        bool isUpdate, Dictionary<string, object?>? previousStack)
    {
        var statusPrefix = isUpdate ? "UPDATE" : "CREATE";
        var stack = _stacks[stackName];

        var mappings = GetDict(template.GetValueOrDefault("Mappings"));
        var conditions = EvaluateConditions(template, paramValues);
        var resourcesDefs = GetDict(template.GetValueOrDefault("Resources"));
        var outputsDefs = GetDict(template.GetValueOrDefault("Outputs"));

        List<string> ordered;
        try
        {
            ordered = TopologicalSort(resourcesDefs, conditions);
        }
        catch (InvalidOperationException exc)
        {
            stack["StackStatus"] = $"{statusPrefix}_FAILED";
            stack["StackStatusReason"] = exc.Message;
            AddEvent(stackId, stackName, stackName, "AWS::CloudFormation::Stack",
                $"{statusPrefix}_FAILED", exc.Message, stackId);
            return;
        }

        var provisionedResources = ToResourceDict(GetDict(stack.GetValueOrDefault("_resources")));
        var createdInThisRun = new List<string>();

        HashSet<string> toRemove;
        if (isUpdate && previousStack is not null)
        {
            var oldResourceNames = GetDict(previousStack.GetValueOrDefault("_resources")).Keys.ToHashSet(StringComparer.Ordinal);
            var newResourceNames = ordered.ToHashSet(StringComparer.Ordinal);
            toRemove = new HashSet<string>(oldResourceNames.Except(newResourceNames), StringComparer.Ordinal);
        }
        else
        {
            toRemove = [];
        }

        var failed = false;
        var failReason = "";

        foreach (var logicalId in ordered)
        {
            var resDef = GetDict(resourcesDefs[logicalId]);
            var cond = GetString(resDef.GetValueOrDefault("Condition"));
            if (!string.IsNullOrEmpty(cond) && conditions.TryGetValue(cond, out var cv) && !cv)
                continue;

            var resourceType = GetString(resDef.GetValueOrDefault("Type", "AWS::CloudFormation::CustomResource"));
            var rawProps = GetDict(resDef.GetValueOrDefault("Properties"));

            try
            {
                var resolvedProps = DeepCloneDict(rawProps);
                resolvedProps = GetDict(ResolveRefs(resolvedProps, provisionedResources,
                    paramValues, conditions, mappings, stackName, stackId));
                // Filter out NoValue
                var filtered = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var (k, v) in resolvedProps)
                {
                    if (!ReferenceEquals(v, NoValue))
                        filtered[k] = v;
                }
                resolvedProps = filtered;

                AddEvent(stackId, stackName, logicalId, resourceType, $"{statusPrefix}_IN_PROGRESS");

                var (physicalId, attrs) = ProvisionResource(resourceType, logicalId, resolvedProps, stackName);

                provisionedResources[logicalId] = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["PhysicalResourceId"] = physicalId,
                    ["ResourceType"] = resourceType,
                    ["ResourceStatus"] = $"{statusPrefix}_COMPLETE",
                    ["LogicalResourceId"] = logicalId,
                    ["Properties"] = resolvedProps,
                    ["Attributes"] = attrs,
                    ["Timestamp"] = TimeHelpers.NowIso(),
                };
                createdInThisRun.Add(logicalId);

                AddEvent(stackId, stackName, logicalId, resourceType,
                    $"{statusPrefix}_COMPLETE", physicalId: physicalId);
            }
            catch (Exception exc)
            {
                AddEvent(stackId, stackName, logicalId, resourceType,
                    $"{statusPrefix}_FAILED", exc.Message);
                failed = true;
                failReason = $"Resource {logicalId} failed: {exc.Message}";
                break;
            }
        }

        // Delete removed resources (update case)
        if (!failed && toRemove.Count > 0 && previousStack is not null)
        {
            var oldResources = GetDict(previousStack.GetValueOrDefault("_resources"));
            foreach (var logicalId in toRemove)
            {
                var oldRes = GetDict(oldResources.GetValueOrDefault(logicalId));
                var rtype = GetString(oldRes.GetValueOrDefault("ResourceType"));
                var pid = GetString(oldRes.GetValueOrDefault("PhysicalResourceId"));
                var oldProps = GetDict(oldRes.GetValueOrDefault("Properties"));
                try { DeleteResource(rtype, pid, oldProps); } catch { /* ignore */ }
                provisionedResources.Remove(logicalId);
            }
        }

        if (failed)
        {
            if (disableRollback)
            {
                stack["StackStatus"] = $"{statusPrefix}_FAILED";
                stack["StackStatusReason"] = failReason;
                AddEvent(stackId, stackName, stackName, "AWS::CloudFormation::Stack",
                    $"{statusPrefix}_FAILED", failReason, stackId);
            }
            else
            {
                var rollbackStatus = isUpdate ? "UPDATE_ROLLBACK_IN_PROGRESS" : "ROLLBACK_IN_PROGRESS";
                stack["StackStatus"] = rollbackStatus;
                AddEvent(stackId, stackName, stackName, "AWS::CloudFormation::Stack",
                    rollbackStatus, "Rollback requested", stackId);

                for (var i = createdInThisRun.Count - 1; i >= 0; i--)
                {
                    var logId = createdInThisRun[i];
                    var res = GetDict(provisionedResources.GetValueOrDefault(logId));
                    var rtype = GetString(res.GetValueOrDefault("ResourceType"));
                    var pid = GetString(res.GetValueOrDefault("PhysicalResourceId"));
                    var resProps = GetDict(res.GetValueOrDefault("Properties"));
                    try
                    {
                        DeleteResource(rtype, pid, resProps);
                        AddEvent(stackId, stackName, logId, rtype, "DELETE_COMPLETE", physicalId: pid);
                    }
                    catch (Exception delExc)
                    {
                        AddEvent(stackId, stackName, logId, rtype, "DELETE_FAILED", delExc.Message, pid);
                    }
                    provisionedResources.Remove(logId);
                }

                if (isUpdate && previousStack is not null)
                {
                    stack["_resources"] = previousStack.GetValueOrDefault("_resources");
                    stack["_template"] = previousStack.GetValueOrDefault("_template");
                    stack["_resolved_params"] = previousStack.GetValueOrDefault("_resolved_params");
                    stack["Outputs"] = previousStack.GetValueOrDefault("Outputs");
                    stack["StackStatus"] = "UPDATE_ROLLBACK_COMPLETE";
                }
                else
                {
                    stack["StackStatus"] = "ROLLBACK_COMPLETE";
                }
                AddEvent(stackId, stackName, stackName, "AWS::CloudFormation::Stack",
                    GetString(stack["StackStatus"]), "Rollback complete", stackId);
            }
            return;
        }

        // Success: resolve outputs
        stack["_resources"] = new Dictionary<string, object?>(
            provisionedResources.Select(kv =>
                new KeyValuePair<string, object?>(kv.Key, kv.Value)),
            StringComparer.Ordinal);
        stack["_template"] = template;
        stack["_resolved_params"] = paramValues;

        var resolvedOutputs = new List<object?>();
        foreach (var (outName, outDefObj) in outputsDefs)
        {
            var outDef = GetDict(outDefObj);
            var outCond = GetString(outDef.GetValueOrDefault("Condition"));
            if (!string.IsNullOrEmpty(outCond) && conditions.TryGetValue(outCond, out var ocv) && !ocv)
                continue;

            var outValue = GetString(ResolveRefs(
                DeepClone(outDef.GetValueOrDefault("Value")),
                provisionedResources, paramValues, conditions, mappings, stackName, stackId));

            var output = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["OutputKey"] = outName,
                ["OutputValue"] = outValue,
                ["Description"] = GetString(outDef.GetValueOrDefault("Description")),
            };

            var exportDef = GetDict(outDef.GetValueOrDefault("Export"));
            if (exportDef.Count > 0)
            {
                var exportName = GetString(ResolveRefs(
                    DeepClone(exportDef.GetValueOrDefault("Name")),
                    provisionedResources, paramValues, conditions, mappings, stackName, stackId));
                output["ExportName"] = exportName;
                _exports[exportName] = new Dictionary<string, string>
                {
                    ["StackId"] = stackId,
                    ["Name"] = exportName,
                    ["Value"] = outValue,
                };
            }
            resolvedOutputs.Add(output);
        }

        stack["Outputs"] = resolvedOutputs;
        stack["StackStatus"] = $"{statusPrefix}_COMPLETE";
        AddEvent(stackId, stackName, stackName, "AWS::CloudFormation::Stack",
            $"{statusPrefix}_COMPLETE", physicalId: stackId);
    }

    private void DeleteStackResources(string stackName, string stackId)
    {
        if (!_stacks.TryGetValue(stackName, out var stack)) return;

        stack["StackStatus"] = "DELETE_IN_PROGRESS";
        AddEvent(stackId, stackName, stackName, "AWS::CloudFormation::Stack",
            "DELETE_IN_PROGRESS", physicalId: stackId);

        var resources = GetDict(stack.GetValueOrDefault("_resources"));
        var template = GetDict(stack.GetValueOrDefault("_template"));
        var resDefs = GetDict(template.GetValueOrDefault("Resources"));
        var conditionsDict = new Dictionary<string, bool>(StringComparer.Ordinal);
        // try to get stored conditions
        if (stack.TryGetValue("_conditions", out var condObj) && condObj is Dictionary<string, bool> storedConds)
            conditionsDict = storedConds;

        List<string> ordered;
        try
        {
            ordered = resDefs.Count > 0 ? TopologicalSort(resDefs, conditionsDict) : [.. resources.Keys];
        }
        catch
        {
            ordered = [.. resources.Keys];
        }

        for (var i = ordered.Count - 1; i >= 0; i--)
        {
            var logicalId = ordered[i];
            if (!resources.TryGetValue(logicalId, out var resObj)) continue;
            var res = GetDict(resObj);
            var rtype = GetString(res.GetValueOrDefault("ResourceType"));
            var pid = GetString(res.GetValueOrDefault("PhysicalResourceId"));
            var resProps = GetDict(res.GetValueOrDefault("Properties"));

            AddEvent(stackId, stackName, logicalId, rtype, "DELETE_IN_PROGRESS", physicalId: pid);
            try
            {
                DeleteResource(rtype, pid, resProps);
                AddEvent(stackId, stackName, logicalId, rtype, "DELETE_COMPLETE", physicalId: pid);
            }
            catch (Exception exc)
            {
                AddEvent(stackId, stackName, logicalId, rtype, "DELETE_FAILED", exc.Message, pid);
            }
        }

        // Remove exports
        var outputs = stack.GetValueOrDefault("Outputs");
        if (outputs is List<object?> outputList)
        {
            foreach (var outObj in outputList)
            {
                var output = GetDict(outObj);
                var exportName = GetString(output.GetValueOrDefault("ExportName"));
                if (!string.IsNullOrEmpty(exportName))
                    _exports.TryRemove(exportName, out _);
            }
        }

        stack["StackStatus"] = "DELETE_COMPLETE";
        AddEvent(stackId, stackName, stackName, "AWS::CloudFormation::Stack",
            "DELETE_COMPLETE", physicalId: stackId);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Action Handlers
    // ══════════════════════════════════════════════════════════════════════════

    private ServiceResponse CreateStack(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        if (string.IsNullOrEmpty(stackName))
            return Error("ValidationError", "StackName is required", 400);

        var templateBody = ResolveTemplateBody(p);
        if (templateBody is null)
            return Error("ValidationError", "TemplateBody or TemplateURL is required", 400);

        // Check uniqueness
        if (_stacks.TryGetValue(stackName, out var existing))
        {
            var existingStatus = GetString(existing.GetValueOrDefault("StackStatus"));
            if (existingStatus is not ("DELETE_COMPLETE" or "ROLLBACK_COMPLETE"))
                return Error("AlreadyExistsException", $"Stack [{stackName}] already exists", 400);
        }

        Dictionary<string, object?> template;
        try { template = ParseTemplate(templateBody); }
        catch (Exception e) { return Error("ValidationError", $"Template format error: {e.Message}", 400); }

        var providedParams = ExtractMembers(p, "Parameters");
        var tags = ExtractMembers(p, "Tags");
        var disableRollback = P(p, "DisableRollback", "false").Equals("true", StringComparison.OrdinalIgnoreCase);

        Dictionary<string, Dictionary<string, object?>> paramValues;
        try { paramValues = ResolveParameters(template, providedParams); }
        catch (Exception exc) { return Error("ValidationError", exc.Message, 400); }

        var stackId = $"arn:aws:cloudformation:{Region}:{AccountContext.GetAccountId()}:stack/{stackName}/{Guid.NewGuid()}";

        var stack = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["StackName"] = stackName,
            ["StackId"] = stackId,
            ["StackStatus"] = "CREATE_IN_PROGRESS",
            ["StackStatusReason"] = "",
            ["CreationTime"] = TimeHelpers.NowIso(),
            ["LastUpdatedTime"] = TimeHelpers.NowIso(),
            ["Description"] = GetString(template.GetValueOrDefault("Description")),
            ["Parameters"] = paramValues.Select(kv => new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["ParameterKey"] = kv.Key,
                ["ParameterValue"] = GetString(kv.Value.GetValueOrDefault("Value")),
                ["NoEcho"] = GetBool(kv.Value.GetValueOrDefault("NoEcho")),
            }).ToList<object?>(),
            ["Tags"] = tags.Select(t => (object?)t).ToList(),
            ["Outputs"] = new List<object?>(),
            ["DisableRollback"] = disableRollback,
            ["_resources"] = new Dictionary<string, object?>(StringComparer.Ordinal),
            ["_template"] = template,
            ["_template_body"] = templateBody,
            ["_resolved_params"] = paramValues,
            ["_conditions"] = EvaluateConditions(template, paramValues),
        };
        _stacks[stackName] = stack;
        _stackEvents[stackId] = [];

        AddEvent(stackId, stackName, stackName, "AWS::CloudFormation::Stack",
            "CREATE_IN_PROGRESS", physicalId: stackId);

        DeployStack(stackName, stackId, template, paramValues, disableRollback);

        return Xml(200, "CreateStackResponse",
            $"<CreateStackResult><StackId>{stackId}</StackId></CreateStackResult>");
    }

    private ServiceResponse DescribeStacks(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");

        List<Dictionary<string, object?>> stacksToDescribe;
        if (!string.IsNullOrEmpty(stackName))
        {
            Dictionary<string, object?>? stack = null;
            if (_stacks.TryGetValue(stackName, out var s))
                stack = s;
            else
            {
                foreach (var sv in _stacks.Values)
                {
                    if (GetString(sv.GetValueOrDefault("StackId")) == stackName)
                    { stack = sv; break; }
                }
            }
            if (stack is null)
                return Error("ValidationError", $"Stack with id {stackName} does not exist", 400);
            stacksToDescribe = [stack];
        }
        else
        {
            stacksToDescribe = _stacks.Values
                .Where(s => GetString(s.GetValueOrDefault("StackStatus")) != "DELETE_COMPLETE")
                .ToList();
        }

        var members = new StringBuilder();
        foreach (var s in stacksToDescribe)
        {
            var paramsXml = new StringBuilder();
            if (s.GetValueOrDefault("Parameters") is List<object?> paramList)
            {
                foreach (var pObj in paramList)
                {
                    var pd = GetDict(pObj);
                    var val = GetBool(pd.GetValueOrDefault("NoEcho"))
                        ? "****"
                        : Esc(GetString(pd.GetValueOrDefault("ParameterValue")));
                    paramsXml.Append($"<member><ParameterKey>{Esc(GetString(pd.GetValueOrDefault("ParameterKey")))}</ParameterKey><ParameterValue>{val}</ParameterValue></member>");
                }
            }

            var outputsXml = new StringBuilder();
            if (s.GetValueOrDefault("Outputs") is List<object?> outputList)
            {
                foreach (var oObj in outputList)
                {
                    var o = GetDict(oObj);
                    var exportXml = "";
                    var exportName = GetString(o.GetValueOrDefault("ExportName"));
                    if (!string.IsNullOrEmpty(exportName))
                        exportXml = $"<ExportName>{Esc(exportName)}</ExportName>";
                    outputsXml.Append($"<member><OutputKey>{Esc(GetString(o.GetValueOrDefault("OutputKey")))}</OutputKey><OutputValue>{Esc(GetString(o.GetValueOrDefault("OutputValue")))}</OutputValue><Description>{Esc(GetString(o.GetValueOrDefault("Description")))}</Description>{exportXml}</member>");
                }
            }

            var tagsXml = new StringBuilder();
            if (s.GetValueOrDefault("Tags") is List<object?> tagList)
            {
                foreach (var tObj in tagList)
                {
                    if (tObj is Dictionary<string, string> t)
                        tagsXml.Append($"<member><Key>{Esc(t.GetValueOrDefault("Key", ""))}</Key><Value>{Esc(t.GetValueOrDefault("Value", ""))}</Value></member>");
                }
            }

            members.Append($"<member><StackName>{Esc(GetString(s.GetValueOrDefault("StackName")))}</StackName><StackId>{Esc(GetString(s.GetValueOrDefault("StackId")))}</StackId><StackStatus>{GetString(s.GetValueOrDefault("StackStatus"))}</StackStatus><StackStatusReason>{Esc(GetString(s.GetValueOrDefault("StackStatusReason")))}</StackStatusReason><CreationTime>{GetString(s.GetValueOrDefault("CreationTime"))}</CreationTime><LastUpdatedTime>{GetString(s.GetValueOrDefault("LastUpdatedTime"))}</LastUpdatedTime><Description>{Esc(GetString(s.GetValueOrDefault("Description")))}</Description><DisableRollback>{(GetBool(s.GetValueOrDefault("DisableRollback")) ? "true" : "false")}</DisableRollback><Parameters>{paramsXml}</Parameters><Outputs>{outputsXml}</Outputs><Tags>{tagsXml}</Tags></member>");
        }

        return Xml(200, "DescribeStacksResponse",
            $"<DescribeStacksResult><Stacks>{members}</Stacks></DescribeStacksResult>");
    }

    private ServiceResponse ListStacks(Dictionary<string, string[]> p)
    {
        var statusFilters = ExtractStackStatusFilters(p);

        var summaries = new StringBuilder();
        foreach (var s in _stacks.Values)
        {
            var status = GetString(s.GetValueOrDefault("StackStatus"));
            if (statusFilters.Count > 0 && !statusFilters.Contains(status))
                continue;
            summaries.Append($"<member><StackName>{Esc(GetString(s.GetValueOrDefault("StackName")))}</StackName><StackId>{Esc(GetString(s.GetValueOrDefault("StackId")))}</StackId><StackStatus>{status}</StackStatus><CreationTime>{GetString(s.GetValueOrDefault("CreationTime"))}</CreationTime></member>");
        }

        return Xml(200, "ListStacksResponse",
            $"<ListStacksResult><StackSummaries>{summaries}</StackSummaries></ListStacksResult>");
    }

    private ServiceResponse DeleteStack(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        if (string.IsNullOrEmpty(stackName))
            return Error("ValidationError", "StackName is required", 400);

        if (!_stacks.TryGetValue(stackName, out var stack))
            return Xml(200, "DeleteStackResponse", "");

        if (GetString(stack.GetValueOrDefault("StackStatus")) == "DELETE_COMPLETE")
            return Xml(200, "DeleteStackResponse", "");

        // Check for active imports
        var outputs = stack.GetValueOrDefault("Outputs") as List<object?> ?? [];
        var stackExports = outputs
            .Select(o => GetString(GetDict(o).GetValueOrDefault("ExportName")))
            .Where(e => !string.IsNullOrEmpty(e))
            .ToList();

        foreach (var exportName in stackExports)
        {
            foreach (var (otherName, otherStack) in _stacks.Items)
            {
                if (otherName == stackName) continue;
                var otherStatus = GetString(otherStack.GetValueOrDefault("StackStatus"));
                if (otherStatus.EndsWith("_COMPLETE", StringComparison.Ordinal)
                    && !otherStatus.Contains("DELETE", StringComparison.Ordinal))
                {
                    var otherTemplate = otherStack.GetValueOrDefault("_template");
                    if (otherTemplate is not null)
                    {
                        var json = JsonSerializer.Serialize(otherTemplate);
                        if (json.Contains(exportName, StringComparison.Ordinal))
                            return Error("ValidationError",
                                $"Export {exportName} is imported by stack {otherName}", 400);
                    }
                }
            }
        }

        var stackId = GetString(stack.GetValueOrDefault("StackId"));
        DeleteStackResources(stackName, stackId);

        return Xml(200, "DeleteStackResponse", "");
    }

    private ServiceResponse UpdateStack(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        if (string.IsNullOrEmpty(stackName))
            return Error("ValidationError", "StackName is required", 400);

        if (!_stacks.TryGetValue(stackName, out var stack))
            return Error("ValidationError", $"Stack [{stackName}] does not exist", 400);

        var currentStatus = GetString(stack.GetValueOrDefault("StackStatus"));
        if (currentStatus is not ("CREATE_COMPLETE" or "UPDATE_COMPLETE" or "UPDATE_ROLLBACK_COMPLETE"))
            return Error("ValidationError",
                $"Stack [{stackName}] is in {currentStatus} state and cannot be updated", 400);

        var templateBody = ResolveTemplateBody(p);
        if (templateBody is null)
        {
            if (P(p, "UsePreviousTemplate", "false").Equals("true", StringComparison.OrdinalIgnoreCase))
                templateBody = GetString(stack.GetValueOrDefault("_template_body", "{}"));
            else
                return Error("ValidationError", "TemplateBody or TemplateURL is required", 400);
        }

        Dictionary<string, object?> template;
        try { template = ParseTemplate(templateBody); }
        catch (Exception e) { return Error("ValidationError", $"Template format error: {e.Message}", 400); }

        var providedParams = ExtractMembers(p, "Parameters");
        var tags = ExtractMembers(p, "Tags");
        var disableRollback = P(p, "DisableRollback", "false").Equals("true", StringComparison.OrdinalIgnoreCase);

        Dictionary<string, Dictionary<string, object?>> paramValues;
        try { paramValues = ResolveParameters(template, providedParams); }
        catch (Exception exc) { return Error("ValidationError", exc.Message, 400); }

        // Save previous state for rollback
        var previousStack = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["_resources"] = DeepClone(stack.GetValueOrDefault("_resources")),
            ["_template"] = DeepClone(stack.GetValueOrDefault("_template")),
            ["_template_body"] = stack.GetValueOrDefault("_template_body"),
            ["_resolved_params"] = DeepClone(stack.GetValueOrDefault("_resolved_params")),
            ["Outputs"] = DeepClone(stack.GetValueOrDefault("Outputs")),
        };

        var stackId = GetString(stack["StackId"]);
        stack["StackStatus"] = "UPDATE_IN_PROGRESS";
        stack["LastUpdatedTime"] = TimeHelpers.NowIso();
        stack["_template_body"] = templateBody;
        if (tags.Count > 0) stack["Tags"] = tags.Select(t => (object?)t).ToList();
        stack["Parameters"] = paramValues.Select(kv => new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ParameterKey"] = kv.Key,
            ["ParameterValue"] = GetString(kv.Value.GetValueOrDefault("Value")),
            ["NoEcho"] = GetBool(kv.Value.GetValueOrDefault("NoEcho")),
        }).ToList<object?>();
        stack["_conditions"] = EvaluateConditions(template, paramValues);

        AddEvent(stackId, stackName, stackName, "AWS::CloudFormation::Stack",
            "UPDATE_IN_PROGRESS", physicalId: stackId);

        DeployStack(stackName, stackId, template, paramValues, disableRollback,
            true, previousStack);

        return Xml(200, "UpdateStackResponse",
            $"<UpdateStackResult><StackId>{stackId}</StackId></UpdateStackResult>");
    }

    private ServiceResponse DescribeStackEvents(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        if (string.IsNullOrEmpty(stackName))
            return Error("ValidationError", "StackName is required", 400);

        Dictionary<string, object?>? stack = null;
        if (_stacks.TryGetValue(stackName, out var s)) stack = s;
        else
        {
            foreach (var sv in _stacks.Values)
            {
                if (GetString(sv.GetValueOrDefault("StackId")) == stackName)
                { stack = sv; break; }
            }
        }
        if (stack is null)
            return Error("ValidationError", $"Stack [{stackName}] does not exist", 400);

        var stackId = GetString(stack["StackId"]);
        var events = _stackEvents.TryGetValue(stackId, out var evList)
            ? evList.OrderByDescending(e => e.GetValueOrDefault("Timestamp", "")).ToList()
            : [];

        var members = new StringBuilder();
        foreach (var e in events)
        {
            members.Append($"<member><StackId>{Esc(e.GetValueOrDefault("StackId", ""))}</StackId><StackName>{Esc(e.GetValueOrDefault("StackName", ""))}</StackName><EventId>{Esc(e.GetValueOrDefault("EventId", ""))}</EventId><LogicalResourceId>{Esc(e.GetValueOrDefault("LogicalResourceId", ""))}</LogicalResourceId><PhysicalResourceId>{Esc(e.GetValueOrDefault("PhysicalResourceId", ""))}</PhysicalResourceId><ResourceType>{Esc(e.GetValueOrDefault("ResourceType", ""))}</ResourceType><ResourceStatus>{e.GetValueOrDefault("ResourceStatus", "")}</ResourceStatus><ResourceStatusReason>{Esc(e.GetValueOrDefault("ResourceStatusReason", ""))}</ResourceStatusReason><Timestamp>{e.GetValueOrDefault("Timestamp", "")}</Timestamp></member>");
        }

        return Xml(200, "DescribeStackEventsResponse",
            $"<DescribeStackEventsResult><StackEvents>{members}</StackEvents></DescribeStackEventsResult>");
    }

    private ServiceResponse DescribeStackResource(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        var logicalId = P(p, "LogicalResourceId");

        if (!_stacks.TryGetValue(stackName, out var stack))
            return Error("ValidationError", $"Stack [{stackName}] does not exist", 400);

        var resources = GetDict(stack.GetValueOrDefault("_resources"));
        if (!resources.TryGetValue(logicalId, out var resObj))
            return Error("ValidationError", $"Resource [{logicalId}] does not exist in stack [{stackName}]", 400);

        var res = GetDict(resObj);
        var detail = $"<LogicalResourceId>{Esc(logicalId)}</LogicalResourceId><PhysicalResourceId>{Esc(GetString(res.GetValueOrDefault("PhysicalResourceId")))}</PhysicalResourceId><ResourceType>{Esc(GetString(res.GetValueOrDefault("ResourceType")))}</ResourceType><ResourceStatus>{GetString(res.GetValueOrDefault("ResourceStatus"))}</ResourceStatus><Timestamp>{GetString(res.GetValueOrDefault("Timestamp"))}</Timestamp><StackName>{Esc(stackName)}</StackName><StackId>{Esc(GetString(stack["StackId"]))}</StackId>";

        return Xml(200, "DescribeStackResourceResponse",
            $"<DescribeStackResourceResult><StackResourceDetail>{detail}</StackResourceDetail></DescribeStackResourceResult>");
    }

    private ServiceResponse DescribeStackResources(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        if (!_stacks.TryGetValue(stackName, out var stack))
            return Error("ValidationError", $"Stack [{stackName}] does not exist", 400);

        var resources = GetDict(stack.GetValueOrDefault("_resources"));
        var members = new StringBuilder();
        foreach (var (logicalId, resObj) in resources)
        {
            var res = GetDict(resObj);
            members.Append($"<member><LogicalResourceId>{Esc(logicalId)}</LogicalResourceId><PhysicalResourceId>{Esc(GetString(res.GetValueOrDefault("PhysicalResourceId")))}</PhysicalResourceId><ResourceType>{Esc(GetString(res.GetValueOrDefault("ResourceType")))}</ResourceType><ResourceStatus>{GetString(res.GetValueOrDefault("ResourceStatus"))}</ResourceStatus><Timestamp>{GetString(res.GetValueOrDefault("Timestamp"))}</Timestamp><StackName>{Esc(stackName)}</StackName><StackId>{Esc(GetString(stack["StackId"]))}</StackId></member>");
        }

        return Xml(200, "DescribeStackResourcesResponse",
            $"<DescribeStackResourcesResult><StackResources>{members}</StackResources></DescribeStackResourcesResult>");
    }

    private ServiceResponse ListStackResources(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        if (string.IsNullOrEmpty(stackName))
            return Error("ValidationError", "StackName is required", 400);

        Dictionary<string, object?>? stack = null;
        if (_stacks.TryGetValue(stackName, out var s)) stack = s;
        else
        {
            foreach (var sv in _stacks.Values)
            {
                if (GetString(sv.GetValueOrDefault("StackId")) == stackName)
                { stack = sv; break; }
            }
        }
        if (stack is null)
            return Error("ValidationError", $"Stack [{stackName}] does not exist", 400);

        var resources = GetDict(stack.GetValueOrDefault("_resources"));
        var members = new StringBuilder();
        foreach (var (logicalId, resObj) in resources)
        {
            var res = GetDict(resObj);
            members.Append($"<member><LogicalResourceId>{Esc(logicalId)}</LogicalResourceId><PhysicalResourceId>{Esc(GetString(res.GetValueOrDefault("PhysicalResourceId")))}</PhysicalResourceId><ResourceType>{Esc(GetString(res.GetValueOrDefault("ResourceType")))}</ResourceType><ResourceStatus>{GetString(res.GetValueOrDefault("ResourceStatus"))}</ResourceStatus><LastUpdatedTimestamp>{GetString(res.GetValueOrDefault("Timestamp"))}</LastUpdatedTimestamp></member>");
        }

        return Xml(200, "ListStackResourcesResponse",
            $"<ListStackResourcesResult><StackResourceSummaries>{members}</StackResourceSummaries></ListStackResourcesResult>");
    }

    private ServiceResponse GetTemplate(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        Dictionary<string, object?>? stack = null;
        if (_stacks.TryGetValue(stackName, out var s)) stack = s;
        else
        {
            foreach (var sv in _stacks.Values)
            {
                if (GetString(sv.GetValueOrDefault("StackId")) == stackName)
                { stack = sv; break; }
            }
        }
        if (stack is null)
            return Error("ValidationError", $"Stack [{stackName}] does not exist", 400);

        var templateBody = GetString(stack.GetValueOrDefault("_template_body", "{}"));
        return Xml(200, "GetTemplateResponse",
            $"<GetTemplateResult><TemplateBody>{Esc(templateBody)}</TemplateBody></GetTemplateResult>");
    }

    private ServiceResponse ValidateTemplate(Dictionary<string, string[]> p)
    {
        var templateBody = P(p, "TemplateBody");
        if (string.IsNullOrEmpty(templateBody))
            return Error("ValidationError", "TemplateBody is required", 400);

        Dictionary<string, object?> template;
        try { template = ParseTemplate(templateBody); }
        catch (Exception e) { return Error("ValidationError", $"Template format error: {e.Message}", 400); }

        if (!template.ContainsKey("Resources"))
            return Error("ValidationError", "Template format error: At least one Resources member must be defined.", 400);

        var description = GetString(template.GetValueOrDefault("Description"));
        var paramDefs = GetDict(template.GetValueOrDefault("Parameters"));

        var paramsXml = new StringBuilder();
        foreach (var (name, defnObj) in paramDefs)
        {
            var defn = GetDict(defnObj);
            paramsXml.Append($"<member><ParameterKey>{Esc(name)}</ParameterKey><DefaultValue>{Esc(GetString(defn.GetValueOrDefault("Default")))}</DefaultValue><NoEcho>{GetString(defn.GetValueOrDefault("NoEcho", "false")).ToLowerInvariant()}</NoEcho><ParameterType>{Esc(GetString(defn.GetValueOrDefault("Type", "String")))}</ParameterType><Description>{Esc(GetString(defn.GetValueOrDefault("Description")))}</Description></member>");
        }

        return Xml(200, "ValidateTemplateResponse",
            $"<ValidateTemplateResult><Description>{Esc(description)}</Description><Parameters>{paramsXml}</Parameters></ValidateTemplateResult>");
    }

    private ServiceResponse ListExports()
    {
        var members = new StringBuilder();
        foreach (var (name, exp) in _exports.Items)
        {
            members.Append($"<member><ExportingStackId>{Esc(exp.GetValueOrDefault("StackId", ""))}</ExportingStackId><Name>{Esc(name)}</Name><Value>{Esc(exp.GetValueOrDefault("Value", ""))}</Value></member>");
        }

        return Xml(200, "ListExportsResponse",
            $"<ListExportsResult><Exports>{members}</Exports></ListExportsResult>");
    }

    private ServiceResponse GetTemplateSummary(Dictionary<string, string[]> p)
    {
        var templateBody = ResolveTemplateBody(p);
        var stackName = P(p, "StackName");

        if (!string.IsNullOrEmpty(stackName) && templateBody is null)
        {
            if (_stacks.TryGetValue(stackName, out var stack))
                templateBody = GetString(stack.GetValueOrDefault("_template_body", "{}"));
            else
                return Error("ValidationError", $"Stack [{stackName}] does not exist", 400);
        }

        if (templateBody is null)
            return Error("ValidationError", "Either TemplateBody, TemplateURL, or StackName must be provided", 400);

        Dictionary<string, object?> template;
        try { template = ParseTemplate(templateBody); }
        catch (Exception e) { return Error("ValidationError", $"Template format error: {e.Message}", 400); }

        var description = GetString(template.GetValueOrDefault("Description"));
        var resources = GetDict(template.GetValueOrDefault("Resources"));
        var paramDefs = GetDict(template.GetValueOrDefault("Parameters"));

        var resourceTypes = resources.Values
            .Select(r => GetString(GetDict(r).GetValueOrDefault("Type")))
            .Distinct()
            .Order()
            .ToList();
        var typesXml = string.Join("", resourceTypes.Select(t => $"<member>{Esc(t)}</member>"));

        var paramsXml = new StringBuilder();
        foreach (var (name, defnObj) in paramDefs)
        {
            var defn = GetDict(defnObj);
            paramsXml.Append($"<member><ParameterKey>{Esc(name)}</ParameterKey><DefaultValue>{Esc(GetString(defn.GetValueOrDefault("Default")))}</DefaultValue><NoEcho>{GetString(defn.GetValueOrDefault("NoEcho", "false")).ToLowerInvariant()}</NoEcho><ParameterType>{Esc(GetString(defn.GetValueOrDefault("Type", "String")))}</ParameterType><Description>{Esc(GetString(defn.GetValueOrDefault("Description")))}</Description></member>");
        }

        return Xml(200, "GetTemplateSummaryResponse",
            $"<GetTemplateSummaryResult><Description>{Esc(description)}</Description><ResourceTypes>{typesXml}</ResourceTypes><Parameters>{paramsXml}</Parameters></GetTemplateSummaryResult>");
    }

    private ServiceResponse UpdateTerminationProtection(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        if (!_stacks.TryGetValue(stackName, out var stack))
            return Error("ValidationError", $"Stack [{stackName}] does not exist", 400);
        var enable = P(p, "EnableTerminationProtection", "false");
        stack["EnableTerminationProtection"] = enable.Equals("true", StringComparison.OrdinalIgnoreCase);
        return Xml(200, "UpdateTerminationProtectionResponse",
            $"<UpdateTerminationProtectionResult><StackId>{Esc(GetString(stack["StackId"]))}</StackId></UpdateTerminationProtectionResult>");
    }

    private static ServiceResponse SetStackPolicy() =>
        Xml(200, "SetStackPolicyResponse", "");

    private static ServiceResponse GetStackPolicy() =>
        Xml(200, "GetStackPolicyResponse", "<GetStackPolicyResult><StackPolicyBody></StackPolicyBody></GetStackPolicyResult>");

    private static ServiceResponse ListImports() =>
        Xml(200, "ListImportsResponse", "<ListImportsResult><Imports></Imports></ListImportsResult>");

    // ══════════════════════════════════════════════════════════════════════════
    // Change Set Actions
    // ══════════════════════════════════════════════════════════════════════════

    private ServiceResponse CreateChangeSet(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        var csName = P(p, "ChangeSetName");
        var csType = P(p, "ChangeSetType", "UPDATE");

        if (string.IsNullOrEmpty(stackName))
            return Error("ValidationError", "StackName is required", 400);
        if (string.IsNullOrEmpty(csName))
            return Error("ValidationError", "ChangeSetName is required", 400);

        var templateBody = ResolveTemplateBody(p);
        var providedParams = ExtractMembers(p, "Parameters");
        var tags = ExtractMembers(p, "Tags");

        _stacks.TryGetValue(stackName, out var stack);

        string stackId;
        if (csType == "CREATE")
        {
            if (stack is not null)
            {
                var sstatus = GetString(stack.GetValueOrDefault("StackStatus"));
                if (sstatus is not ("DELETE_COMPLETE" or "ROLLBACK_COMPLETE" or "REVIEW_IN_PROGRESS"))
                    return Error("AlreadyExistsException", $"Stack [{stackName}] already exists", 400);
            }
            if (templateBody is null)
                return Error("ValidationError", "TemplateBody or TemplateURL is required", 400);

            stackId = $"arn:aws:cloudformation:{Region}:{AccountContext.GetAccountId()}:stack/{stackName}/{Guid.NewGuid()}";
            stack = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["StackName"] = stackName, ["StackId"] = stackId,
                ["StackStatus"] = "REVIEW_IN_PROGRESS", ["StackStatusReason"] = "",
                ["CreationTime"] = TimeHelpers.NowIso(), ["LastUpdatedTime"] = TimeHelpers.NowIso(),
                ["Description"] = "", ["Parameters"] = new List<object?>(),
                ["Tags"] = tags.Select(t => (object?)t).ToList(),
                ["Outputs"] = new List<object?>(), ["DisableRollback"] = false,
                ["_resources"] = new Dictionary<string, object?>(StringComparer.Ordinal),
                ["_template"] = new Dictionary<string, object?>(StringComparer.Ordinal),
                ["_template_body"] = "", ["_resolved_params"] = new Dictionary<string, Dictionary<string, object?>>(),
                ["_conditions"] = new Dictionary<string, bool>(),
            };
            _stacks[stackName] = stack;
            _stackEvents[stackId] = [];
        }
        else
        {
            if (stack is null)
                return Error("ValidationError", $"Stack [{stackName}] does not exist", 400);
            stackId = GetString(stack["StackId"]);
            templateBody ??= GetString(stack.GetValueOrDefault("_template_body", "{}"));
        }

        Dictionary<string, object?> template;
        try { template = ParseTemplate(templateBody); }
        catch (Exception e) { return Error("ValidationError", $"Template format error: {e.Message}", 400); }

        Dictionary<string, Dictionary<string, object?>> paramValues;
        try { paramValues = ResolveParameters(template, providedParams); }
        catch (Exception exc) { return Error("ValidationError", exc.Message, 400); }

        var oldTemplate = csType == "UPDATE" ? GetDict(stack!.GetValueOrDefault("_template")) : new Dictionary<string, object?>();
        var changes = DiffResources(oldTemplate, template);

        var csId = $"arn:aws:cloudformation:{Region}:{AccountContext.GetAccountId()}:changeSet/{csName}/{Guid.NewGuid()}";

        var changeSet = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ChangeSetId"] = csId, ["ChangeSetName"] = csName,
            ["StackId"] = stackId, ["StackName"] = stackName,
            ["Status"] = "CREATE_COMPLETE", ["ExecutionStatus"] = "AVAILABLE",
            ["CreationTime"] = TimeHelpers.NowIso(),
            ["Description"] = P(p, "Description"),
            ["ChangeSetType"] = csType,
            ["Changes"] = changes,
            ["Parameters"] = paramValues.Select(kv => new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["ParameterKey"] = kv.Key,
                ["ParameterValue"] = GetString(kv.Value.GetValueOrDefault("Value")),
            }).ToList<object?>(),
            ["Tags"] = tags.Select(t => (object?)t).ToList(),
            ["_template"] = template, ["_template_body"] = templateBody,
            ["_resolved_params"] = paramValues,
        };
        _changeSets[csId] = changeSet;

        return Xml(200, "CreateChangeSetResponse",
            $"<CreateChangeSetResult><Id>{csId}</Id><StackId>{stackId}</StackId></CreateChangeSetResult>");
    }

    private ServiceResponse DescribeChangeSet(Dictionary<string, string[]> p)
    {
        var csName = P(p, "ChangeSetName");
        var stackName = P(p, "StackName");
        var (_, cs) = FindChangeSet(csName, stackName);
        if (cs is null)
            return Error("ChangeSetNotFoundException", $"ChangeSet [{csName}] does not exist", 400);

        var paramsXml = new StringBuilder();
        if (cs.GetValueOrDefault("Parameters") is List<object?> paramList)
        {
            foreach (var pObj in paramList)
            {
                var pd = GetDict(pObj);
                paramsXml.Append($"<member><ParameterKey>{Esc(GetString(pd.GetValueOrDefault("ParameterKey")))}</ParameterKey><ParameterValue>{Esc(GetString(pd.GetValueOrDefault("ParameterValue")))}</ParameterValue></member>");
            }
        }

        var changesXml = new StringBuilder();
        if (cs.GetValueOrDefault("Changes") is List<object?> changeList)
        {
            foreach (var chObj in changeList)
            {
                var ch = GetDict(chObj);
                var rc = GetDict(ch.GetValueOrDefault("ResourceChange"));
                changesXml.Append($"<member><ResourceChange><Action>{GetString(rc.GetValueOrDefault("Action"))}</Action><LogicalResourceId>{Esc(GetString(rc.GetValueOrDefault("LogicalResourceId")))}</LogicalResourceId><ResourceType>{Esc(GetString(rc.GetValueOrDefault("ResourceType")))}</ResourceType><Replacement>{GetString(rc.GetValueOrDefault("Replacement"))}</Replacement></ResourceChange></member>");
            }
        }

        var tagsXml = new StringBuilder();
        if (cs.GetValueOrDefault("Tags") is List<object?> tagList)
        {
            foreach (var tObj in tagList)
            {
                if (tObj is Dictionary<string, string> t)
                    tagsXml.Append($"<member><Key>{Esc(t.GetValueOrDefault("Key", ""))}</Key><Value>{Esc(t.GetValueOrDefault("Value", ""))}</Value></member>");
            }
        }

        var inner = $"<ChangeSetId>{Esc(GetString(cs["ChangeSetId"]))}</ChangeSetId><ChangeSetName>{Esc(GetString(cs["ChangeSetName"]))}</ChangeSetName><StackId>{Esc(GetString(cs["StackId"]))}</StackId><StackName>{Esc(GetString(cs["StackName"]))}</StackName><Status>{GetString(cs["Status"])}</Status><ExecutionStatus>{GetString(cs["ExecutionStatus"])}</ExecutionStatus><CreationTime>{GetString(cs["CreationTime"])}</CreationTime><Description>{Esc(GetString(cs.GetValueOrDefault("Description")))}</Description><ChangeSetType>{GetString(cs.GetValueOrDefault("ChangeSetType"))}</ChangeSetType><Parameters>{paramsXml}</Parameters><Changes>{changesXml}</Changes><Tags>{tagsXml}</Tags>";

        return Xml(200, "DescribeChangeSetResponse",
            $"<DescribeChangeSetResult>{inner}</DescribeChangeSetResult>");
    }

    private ServiceResponse ExecuteChangeSet(Dictionary<string, string[]> p)
    {
        var csName = P(p, "ChangeSetName");
        var stackName = P(p, "StackName");
        var (_, cs) = FindChangeSet(csName, stackName);
        if (cs is null)
            return Error("ChangeSetNotFoundException", $"ChangeSet [{csName}] does not exist", 400);

        if (GetString(cs["ExecutionStatus"]) != "AVAILABLE")
            return Error("InvalidChangeSetStatusException",
                $"ChangeSet [{csName}] is in {GetString(cs["ExecutionStatus"])} status", 400);

        cs["ExecutionStatus"] = "EXECUTE_IN_PROGRESS";
        var realStackName = GetString(cs["StackName"]);
        if (!_stacks.TryGetValue(realStackName, out var stack))
            return Error("ValidationError", $"Stack [{realStackName}] does not exist", 400);

        var stackId = GetString(stack["StackId"]);
        var template = GetDict(cs["_template"]);
        var templateBody = GetString(cs["_template_body"]);
        var paramValues = cs["_resolved_params"] as Dictionary<string, Dictionary<string, object?>>
            ?? new Dictionary<string, Dictionary<string, object?>>();
        var tags = ExtractMembers(p, "Tags");
        var csType = GetString(cs.GetValueOrDefault("ChangeSetType", "UPDATE"));
        var isUpdate = csType == "UPDATE";

        Dictionary<string, object?>? previousStack = null;
        if (isUpdate)
        {
            previousStack = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["_resources"] = DeepClone(stack.GetValueOrDefault("_resources")),
                ["_template"] = DeepClone(stack.GetValueOrDefault("_template")),
                ["_template_body"] = stack.GetValueOrDefault("_template_body"),
                ["_resolved_params"] = DeepClone(stack.GetValueOrDefault("_resolved_params")),
                ["Outputs"] = DeepClone(stack.GetValueOrDefault("Outputs")),
            };
        }

        var statusPrefix = isUpdate ? "UPDATE" : "CREATE";
        stack["StackStatus"] = $"{statusPrefix}_IN_PROGRESS";
        stack["LastUpdatedTime"] = TimeHelpers.NowIso();
        stack["_template_body"] = templateBody;
        if (cs.GetValueOrDefault("Tags") is List<object?> csTags && csTags.Count > 0)
            stack["Tags"] = csTags;
        stack["Parameters"] = paramValues.Select(kv => new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["ParameterKey"] = kv.Key,
            ["ParameterValue"] = GetString(kv.Value.GetValueOrDefault("Value")),
            ["NoEcho"] = GetBool(kv.Value.GetValueOrDefault("NoEcho")),
        }).ToList<object?>();
        stack["_conditions"] = EvaluateConditions(template, paramValues);

        AddEvent(stackId, realStackName, realStackName, "AWS::CloudFormation::Stack",
            $"{statusPrefix}_IN_PROGRESS", physicalId: stackId);

        DeployStack(realStackName, stackId, template, paramValues, false,
            isUpdate, previousStack);

        cs["ExecutionStatus"] = "EXECUTE_COMPLETE";
        cs["Status"] = "EXECUTE_COMPLETE";

        return Xml(200, "ExecuteChangeSetResponse",
            "<ExecuteChangeSetResult></ExecuteChangeSetResult>");
    }

    private ServiceResponse DeleteChangeSet(Dictionary<string, string[]> p)
    {
        var csName = P(p, "ChangeSetName");
        var stackName = P(p, "StackName");
        var (csId, cs) = FindChangeSet(csName, stackName);
        if (csId is null)
            return Error("ChangeSetNotFoundException", $"ChangeSet [{csName}] does not exist", 400);
        _changeSets.TryRemove(csId, out _);
        return Xml(200, "DeleteChangeSetResponse", "");
    }

    private ServiceResponse ListChangeSets(Dictionary<string, string[]> p)
    {
        var stackName = P(p, "StackName");
        if (string.IsNullOrEmpty(stackName))
            return Error("ValidationError", "StackName is required", 400);

        var members = new StringBuilder();
        foreach (var cs in _changeSets.Values)
        {
            if (GetString(cs["StackName"]) != stackName) continue;
            members.Append($"<member><ChangeSetId>{Esc(GetString(cs["ChangeSetId"]))}</ChangeSetId><ChangeSetName>{Esc(GetString(cs["ChangeSetName"]))}</ChangeSetName><StackId>{Esc(GetString(cs["StackId"]))}</StackId><StackName>{Esc(GetString(cs["StackName"]))}</StackName><Status>{GetString(cs["Status"])}</Status><ExecutionStatus>{GetString(cs["ExecutionStatus"])}</ExecutionStatus><CreationTime>{GetString(cs["CreationTime"])}</CreationTime><Description>{Esc(GetString(cs.GetValueOrDefault("Description")))}</Description></member>");
        }

        return Xml(200, "ListChangeSetsResponse",
            $"<ListChangeSetsResult><Summaries>{members}</Summaries></ListChangeSetsResult>");
    }

    private (string? CsId, Dictionary<string, object?>? Cs) FindChangeSet(string csName, string stackName)
    {
        if (_changeSets.TryGetValue(csName, out var cs))
            return (csName, cs);
        foreach (var (cid, c) in _changeSets.Items)
        {
            if (GetString(c["ChangeSetName"]) == csName)
            {
                if (string.IsNullOrEmpty(stackName) || GetString(c["StackName"]) == stackName)
                    return (cid, c);
            }
        }
        return (null, null);
    }

    private static List<object?> DiffResources(Dictionary<string, object?> oldTemplate, Dictionary<string, object?> newTemplate)
    {
        var oldRes = GetDict(oldTemplate.GetValueOrDefault("Resources"));
        var newRes = GetDict(newTemplate.GetValueOrDefault("Resources"));
        var changes = new List<object?>();

        var allKeys = oldRes.Keys.Union(newRes.Keys).Order();
        foreach (var key in allKeys)
        {
            if (!oldRes.ContainsKey(key))
            {
                changes.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["ResourceChange"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["Action"] = "Add", ["LogicalResourceId"] = key,
                        ["ResourceType"] = GetString(GetDict(newRes[key]).GetValueOrDefault("Type")),
                        ["Replacement"] = "False",
                    }
                });
            }
            else if (!newRes.ContainsKey(key))
            {
                changes.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["ResourceChange"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["Action"] = "Remove", ["LogicalResourceId"] = key,
                        ["ResourceType"] = GetString(GetDict(oldRes[key]).GetValueOrDefault("Type")),
                        ["PhysicalResourceId"] = "", ["Replacement"] = "False",
                    }
                });
            }
            else
            {
                var oldProps = GetDict(GetDict(oldRes[key]).GetValueOrDefault("Properties"));
                var newProps = GetDict(GetDict(newRes[key]).GetValueOrDefault("Properties"));
                if (JsonSerializer.Serialize(oldProps) != JsonSerializer.Serialize(newProps))
                {
                    changes.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["ResourceChange"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                        {
                            ["Action"] = "Modify", ["LogicalResourceId"] = key,
                            ["ResourceType"] = GetString(GetDict(newRes[key]).GetValueOrDefault("Type")),
                            ["Replacement"] = "Conditional",
                        }
                    });
                }
            }
        }
        return changes;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Deep Clone helpers
    // ══════════════════════════════════════════════════════════════════════════

    private static object? DeepClone(object? obj)
    {
        return obj switch
        {
            Dictionary<string, object?> d => DeepCloneDict(d),
            List<object?> l => l.Select(DeepClone).ToList<object?>(),
            Dictionary<string, Dictionary<string, object?>> pd =>
                new Dictionary<string, Dictionary<string, object?>>(
                    pd.Select(kv => new KeyValuePair<string, Dictionary<string, object?>>(
                        kv.Key, DeepCloneDict(kv.Value))), StringComparer.Ordinal),
            _ => obj,
        };
    }

    private static Dictionary<string, object?> DeepCloneDict(Dictionary<string, object?> source)
    {
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (k, v) in source)
            result[k] = DeepClone(v);
        return result;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Parse / XML / Error helpers
    // ══════════════════════════════════════════════════════════════════════════

    private static Dictionary<string, string[]> ParseParams(ServiceRequest request)
    {
        var result = new Dictionary<string, string[]>(StringComparer.Ordinal);

        // Merge query params
        foreach (var (key, values) in request.QueryParams)
            result[key] = values;

        if (request.Body.Length == 0) return result;
        var body = Encoding.UTF8.GetString(request.Body);
        if (string.IsNullOrEmpty(body)) return result;

        // Only parse as form data if it's not JSON
        if (body.TrimStart().StartsWith('{')) return result;

        foreach (var pair in body.Split('&'))
        {
            var eqIdx = pair.IndexOf('=');
            if (eqIdx < 0) continue;
            var key = HttpUtility.UrlDecode(pair[..eqIdx]);
            var val = HttpUtility.UrlDecode(pair[(eqIdx + 1)..]);
            if (result.TryGetValue(key, out var existing))
            {
                var newArr = new string[existing.Length + 1];
                existing.CopyTo(newArr, 0);
                newArr[existing.Length] = val;
                result[key] = newArr;
            }
            else
            {
                result[key] = [val];
            }
        }
        return result;
    }

    private static string P(Dictionary<string, string[]> p, string key)
    {
        return p.TryGetValue(key, out var vals) && vals.Length > 0 ? vals[0] : "";
    }

    private static string P(Dictionary<string, string[]> p, string key, string defaultValue)
    {
        var val = P(p, key);
        return string.IsNullOrEmpty(val) ? defaultValue : val;
    }

    private static List<Dictionary<string, string>> ExtractMembers(Dictionary<string, string[]> p, string prefix)
    {
        var result = new List<Dictionary<string, string>>();
        for (var i = 1; ; i++)
        {
            var key = P(p, $"{prefix}.member.{i}.ParameterKey");
            if (string.IsNullOrEmpty(key))
                key = P(p, $"{prefix}.member.{i}.Key");
            if (string.IsNullOrEmpty(key)) break;
            var value = P(p, $"{prefix}.member.{i}.ParameterValue");
            if (string.IsNullOrEmpty(value))
                value = P(p, $"{prefix}.member.{i}.Value");
            result.Add(new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["Key"] = key, ["Value"] = value
            });
        }
        return result;
    }

    private static List<string> ExtractStackStatusFilters(Dictionary<string, string[]> p)
    {
        var filters = new List<string>();
        for (var i = 1; ; i++)
        {
            var val = P(p, $"StackStatusFilter.member.{i}");
            if (string.IsNullOrEmpty(val)) break;
            filters.Add(val);
        }
        return filters;
    }

    private static string? ResolveTemplateBody(Dictionary<string, string[]> p)
    {
        var body = P(p, "TemplateBody");
        if (!string.IsNullOrEmpty(body)) return body;
        // TemplateURL not supported in C# port — would need S3 handler
        return null;
    }

    private static readonly IReadOnlyDictionary<string, string> XmlHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "application/xml" };

    private static ServiceResponse Xml(int status, string rootTag, string inner)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><{rootTag} xmlns=\"{CfnNs}\">{inner}<ResponseMetadata><RequestId>{requestId}</RequestId></ResponseMetadata></{rootTag}>";
        return new ServiceResponse(status, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }

    private static ServiceResponse Error(string code, string message, int status)
    {
        var t = status < 500 ? "Sender" : "Receiver";
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?><ErrorResponse xmlns=\"{CfnNs}\"><Error><Type>{t}</Type><Code>{code}</Code><Message>{Esc(message)}</Message></Error><RequestId>{requestId}</RequestId></ErrorResponse>";
        return new ServiceResponse(status, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }

    private static string Esc(string value)
    {
        if (string.IsNullOrEmpty(value)) return "";
        return value
            .Replace("&", "&amp;")
            .Replace("<", "&lt;")
            .Replace(">", "&gt;")
            .Replace("\"", "&quot;")
            .Replace("'", "&apos;");
    }
}
