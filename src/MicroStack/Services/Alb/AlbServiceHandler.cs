using System.Text;
using System.Web;
using MicroStack.Internal;

namespace MicroStack.Services.Alb;

internal sealed class AlbServiceHandler : IServiceHandler
{
    public string ServiceName => "elasticloadbalancing";

    private const string Ns = "http://elasticloadbalancing.amazonaws.com/doc/2015-12-01/";

    private static string Region =>
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";

    // State stores (account-scoped)
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _lbs = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _tgs = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _listeners = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object>> _rules = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object>>> _targets = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, string>>> _tags = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, string>>> _lbAttrs = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, string>>> _tgAttrs = new();

    private readonly Lock _lock = new();

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var p = ParseParams(request);
        var action = P(p, "Action");

        ServiceResponse response;
        lock (_lock) { response = DispatchAction(action, p); }

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _lbs.Clear();
            _tgs.Clear();
            _listeners.Clear();
            _rules.Clear();
            _targets.Clear();
            _tags.Clear();
            _lbAttrs.Clear();
            _tgAttrs.Clear();
        }
    }

    public object? GetState() => null;
    public void RestoreState(object state) { }

    // ── Action dispatch ────────────────────────────────────────────────────────

    private ServiceResponse DispatchAction(string action, Dictionary<string, string[]> p)
    {
        return action switch
        {
            "CreateLoadBalancer" => CreateLoadBalancer(p),
            "DescribeLoadBalancers" => DescribeLoadBalancers(p),
            "DeleteLoadBalancer" => DeleteLoadBalancer(p),
            "DescribeLoadBalancerAttributes" => DescribeLoadBalancerAttributes(p),
            "ModifyLoadBalancerAttributes" => ModifyLoadBalancerAttributes(p),
            "SetSecurityGroups" => SetSecurityGroups(p),
            "SetSubnets" => SetSubnets(p),
            "CreateTargetGroup" => CreateTargetGroup(p),
            "DescribeTargetGroups" => DescribeTargetGroups(p),
            "ModifyTargetGroup" => ModifyTargetGroup(p),
            "DeleteTargetGroup" => DeleteTargetGroup(p),
            "DescribeTargetGroupAttributes" => DescribeTargetGroupAttributes(p),
            "ModifyTargetGroupAttributes" => ModifyTargetGroupAttributes(p),
            "CreateListener" => CreateListener(p),
            "DescribeListeners" => DescribeListeners(p),
            "ModifyListener" => ModifyListener(p),
            "DeleteListener" => DeleteListener(p),
            "CreateRule" => CreateRule(p),
            "DescribeRules" => DescribeRules(p),
            "ModifyRule" => ModifyRule(p),
            "DeleteRule" => DeleteRule(p),
            "SetRulePriorities" => SetRulePriorities(p),
            "RegisterTargets" => RegisterTargets(p),
            "DeregisterTargets" => DeregisterTargets(p),
            "DescribeTargetHealth" => DescribeTargetHealth(p),
            "AddTags" => AddTags(p),
            "RemoveTags" => RemoveTags(p),
            "DescribeTags" => DescribeTags(p),
            _ => Error("InvalidAction", $"Unknown ELBv2 action: {action}", 400),
        };
    }

    // ── Load Balancer handlers ─────────────────────────────────────────────────

    private ServiceResponse CreateLoadBalancer(Dictionary<string, string[]> p)
    {
        var name = P(p, "Name");
        if (string.IsNullOrEmpty(name))
            return Error("ValidationError", "Name is required", 400);

        foreach (var lb in _lbs.Values)
        {
            if ((string)lb["LoadBalancerName"] == name)
                return Error("DuplicateLoadBalancerName",
                    $"A load balancer with name '{name}' already exists.", 400);
        }

        var lid = ShortId();
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:elasticloadbalancing:{Region}:{accountId}:loadbalancer/app/{name}/{lid}";

        var lb2 = new Dictionary<string, object>
        {
            ["LoadBalancerArn"] = arn,
            ["LoadBalancerName"] = name,
            ["DNSName"] = $"{name}-{lid[..8]}.{Region}.elb.amazonaws.com",
            ["Scheme"] = P(p, "Scheme", "internet-facing"),
            ["VpcId"] = P(p, "VpcId", "vpc-00000001"),
            ["State"] = "active",
            ["Type"] = P(p, "Type", "application"),
            ["Subnets"] = ParseMemberList(p, "Subnets"),
            ["SecurityGroups"] = ParseMemberList(p, "SecurityGroups"),
            ["IpAddressType"] = P(p, "IpAddressType", "ipv4"),
            ["CreatedTime"] = NowIso(),
        };

        _lbs[arn] = lb2;
        _tags[arn] = ParseTags(p);
        _lbAttrs[arn] =
        [
            new() { ["Key"] = "access_logs.s3.enabled", ["Value"] = "false" },
            new() { ["Key"] = "deletion_protection.enabled", ["Value"] = "false" },
            new() { ["Key"] = "idle_timeout.timeout_seconds", ["Value"] = "60" },
        ];

        return Xml(200, "CreateLoadBalancer", $"<LoadBalancers>{LbXml(lb2)}</LoadBalancers>");
    }

    private ServiceResponse DescribeLoadBalancers(Dictionary<string, string[]> p)
    {
        var arnFilter = ParseMemberList(p, "LoadBalancerArns");
        var nameFilter = ParseMemberList(p, "Names");
        var results = _lbs.Values.ToList();

        if (arnFilter.Count > 0)
            results = results.Where(lb => arnFilter.Contains((string)lb["LoadBalancerArn"])).ToList();
        if (nameFilter.Count > 0)
            results = results.Where(lb => nameFilter.Contains((string)lb["LoadBalancerName"])).ToList();

        var sb = new StringBuilder();
        foreach (var lb in results)
            sb.Append(LbXml(lb));

        return Xml(200, "DescribeLoadBalancers", $"<LoadBalancers>{sb}</LoadBalancers>");
    }

    private ServiceResponse DeleteLoadBalancer(Dictionary<string, string[]> p)
    {
        var arn = P(p, "LoadBalancerArn");
        _lbs.TryRemove(arn, out _);
        _lbAttrs.TryRemove(arn, out _);
        _tags.TryRemove(arn, out _);
        return Empty("DeleteLoadBalancer");
    }

    private ServiceResponse DescribeLoadBalancerAttributes(Dictionary<string, string[]> p)
    {
        var arn = P(p, "LoadBalancerArn");
        if (!_lbs.ContainsKey(arn))
            return Error("LoadBalancerNotFound", $"Load balancer '{arn}' not found.", 400);

        var attrs = _lbAttrs.TryGetValue(arn, out var a) ? a : [];
        return Xml(200, "DescribeLoadBalancerAttributes",
            $"<Attributes>{AttrsXml(attrs)}</Attributes>");
    }

    private ServiceResponse ModifyLoadBalancerAttributes(Dictionary<string, string[]> p)
    {
        var arn = P(p, "LoadBalancerArn");
        if (!_lbs.ContainsKey(arn))
            return Error("LoadBalancerNotFound", $"Load balancer '{arn}' not found.", 400);

        if (!_lbAttrs.TryGetValue(arn, out var attrs))
        {
            attrs = [];
            _lbAttrs[arn] = attrs;
        }

        var idx = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i2 = 0; i2 < attrs.Count; i2++)
            idx[attrs[i2]["Key"]] = i2;

        for (var i = 1; ; i++)
        {
            var key = P(p, $"Attributes.member.{i}.Key");
            if (string.IsNullOrEmpty(key)) break;
            var val = P(p, $"Attributes.member.{i}.Value");
            if (idx.TryGetValue(key, out var existingIdx))
            {
                attrs[existingIdx]["Value"] = val;
            }
            else
            {
                attrs.Add(new Dictionary<string, string> { ["Key"] = key, ["Value"] = val });
                idx[key] = attrs.Count - 1;
            }
        }

        return Xml(200, "ModifyLoadBalancerAttributes",
            $"<Attributes>{AttrsXml(attrs)}</Attributes>");
    }

    private ServiceResponse SetSecurityGroups(Dictionary<string, string[]> p)
    {
        var arn = P(p, "LoadBalancerArn");
        if (!_lbs.TryGetValue(arn, out var lb))
            return Error("LoadBalancerNotFound", $"Load balancer '{arn}' not found.", 400);

        var sgs = ParseMemberList(p, "SecurityGroups");
        lb["SecurityGroups"] = sgs;

        var sgXml = new StringBuilder();
        foreach (var sg in sgs)
            sgXml.Append($"<member>{sg}</member>");

        return Xml(200, "SetSecurityGroups", $"<SecurityGroupIds>{sgXml}</SecurityGroupIds>");
    }

    private ServiceResponse SetSubnets(Dictionary<string, string[]> p)
    {
        var arn = P(p, "LoadBalancerArn");
        if (!_lbs.TryGetValue(arn, out var lb))
            return Error("LoadBalancerNotFound", $"Load balancer '{arn}' not found.", 400);

        var subnets = ParseMemberList(p, "Subnets");
        lb["Subnets"] = subnets;

        var azXml = new StringBuilder();
        foreach (var s in subnets)
        {
            azXml.Append($"<member><ZoneName>{Region}a</ZoneName><SubnetId>{s}</SubnetId>"
                         + "<LoadBalancerAddresses/></member>");
        }

        return Xml(200, "SetSubnets", $"<AvailabilityZones>{azXml}</AvailabilityZones>");
    }

    // ── Target Group handlers ──────────────────────────────────────────────────

    private ServiceResponse CreateTargetGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "Name");
        if (string.IsNullOrEmpty(name))
            return Error("ValidationError", "Name is required", 400);

        foreach (var tg in _tgs.Values)
        {
            if ((string)tg["TargetGroupName"] == name)
                return Error("DuplicateTargetGroupName",
                    $"A target group with name '{name}' already exists.", 400);
        }

        var tid = ShortId();
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:elasticloadbalancing:{Region}:{accountId}:targetgroup/{name}/{tid}";

        var portStr = P(p, "Port", "80");
        var tg2 = new Dictionary<string, object>
        {
            ["TargetGroupArn"] = arn,
            ["TargetGroupName"] = name,
            ["Protocol"] = P(p, "Protocol", "HTTP"),
            ["Port"] = int.Parse(string.IsNullOrEmpty(portStr) ? "80" : portStr),
            ["VpcId"] = P(p, "VpcId"),
            ["HealthCheckProtocol"] = P(p, "HealthCheckProtocol", "HTTP"),
            ["HealthCheckPort"] = P(p, "HealthCheckPort", "traffic-port"),
            ["HealthCheckEnabled"] = string.Equals(P(p, "HealthCheckEnabled", "true"), "true", StringComparison.OrdinalIgnoreCase),
            ["HealthCheckPath"] = P(p, "HealthCheckPath", "/"),
            ["HealthCheckIntervalSeconds"] = ParseIntOrDefault(P(p, "HealthCheckIntervalSeconds"), 30),
            ["HealthCheckTimeoutSeconds"] = ParseIntOrDefault(P(p, "HealthCheckTimeoutSeconds"), 5),
            ["HealthyThresholdCount"] = ParseIntOrDefault(P(p, "HealthyThresholdCount"), 5),
            ["UnhealthyThresholdCount"] = ParseIntOrDefault(P(p, "UnhealthyThresholdCount"), 2),
            ["Matcher"] = new Dictionary<string, string> { ["HttpCode"] = P(p, "Matcher.HttpCode", "200") },
            ["LoadBalancerArns"] = new List<string>(),
            ["TargetType"] = P(p, "TargetType", "instance"),
        };

        _tgs[arn] = tg2;
        _targets[arn] = [];
        _tags[arn] = ParseTags(p);
        _tgAttrs[arn] =
        [
            new() { ["Key"] = "deregistration_delay.timeout_seconds", ["Value"] = "300" },
            new() { ["Key"] = "stickiness.enabled", ["Value"] = "false" },
            new() { ["Key"] = "stickiness.type", ["Value"] = "lb_cookie" },
        ];

        return Xml(200, "CreateTargetGroup", $"<TargetGroups>{TgXml(tg2)}</TargetGroups>");
    }

    private ServiceResponse DescribeTargetGroups(Dictionary<string, string[]> p)
    {
        var arnFilter = ParseMemberList(p, "TargetGroupArns");
        var nameFilter = ParseMemberList(p, "Names");
        var lbArn = P(p, "LoadBalancerArn");
        var results = _tgs.Values.ToList();

        if (arnFilter.Count > 0)
            results = results.Where(tg => arnFilter.Contains((string)tg["TargetGroupArn"])).ToList();
        if (nameFilter.Count > 0)
            results = results.Where(tg => nameFilter.Contains((string)tg["TargetGroupName"])).ToList();
        if (!string.IsNullOrEmpty(lbArn))
            results = results.Where(tg => ((List<string>)tg["LoadBalancerArns"]).Contains(lbArn)).ToList();

        var sb = new StringBuilder();
        foreach (var tg in results)
            sb.Append(TgXml(tg));

        return Xml(200, "DescribeTargetGroups", $"<TargetGroups>{sb}</TargetGroups>");
    }

    private ServiceResponse ModifyTargetGroup(Dictionary<string, string[]> p)
    {
        var arn = P(p, "TargetGroupArn");
        if (!_tgs.TryGetValue(arn, out var tg))
            return Error("TargetGroupNotFound", $"Target group '{arn}' not found.", 400);

        var hcProto = P(p, "HealthCheckProtocol");
        if (!string.IsNullOrEmpty(hcProto)) tg["HealthCheckProtocol"] = hcProto;

        var hcPort = P(p, "HealthCheckPort");
        if (!string.IsNullOrEmpty(hcPort)) tg["HealthCheckPort"] = hcPort;

        var hcPath = P(p, "HealthCheckPath");
        if (!string.IsNullOrEmpty(hcPath)) tg["HealthCheckPath"] = hcPath;

        var hcEnabled = P(p, "HealthCheckEnabled");
        if (!string.IsNullOrEmpty(hcEnabled))
            tg["HealthCheckEnabled"] = string.Equals(hcEnabled, "true", StringComparison.OrdinalIgnoreCase);

        var hcInterval = P(p, "HealthCheckIntervalSeconds");
        if (!string.IsNullOrEmpty(hcInterval)) tg["HealthCheckIntervalSeconds"] = int.Parse(hcInterval);

        var hcTimeout = P(p, "HealthCheckTimeoutSeconds");
        if (!string.IsNullOrEmpty(hcTimeout)) tg["HealthCheckTimeoutSeconds"] = int.Parse(hcTimeout);

        var healthyCount = P(p, "HealthyThresholdCount");
        if (!string.IsNullOrEmpty(healthyCount)) tg["HealthyThresholdCount"] = int.Parse(healthyCount);

        var unhealthyCount = P(p, "UnhealthyThresholdCount");
        if (!string.IsNullOrEmpty(unhealthyCount)) tg["UnhealthyThresholdCount"] = int.Parse(unhealthyCount);

        var httpCode = P(p, "Matcher.HttpCode");
        if (!string.IsNullOrEmpty(httpCode))
            ((Dictionary<string, string>)tg["Matcher"])["HttpCode"] = httpCode;

        return Xml(200, "ModifyTargetGroup", $"<TargetGroups>{TgXml(tg)}</TargetGroups>");
    }

    private ServiceResponse DeleteTargetGroup(Dictionary<string, string[]> p)
    {
        var arn = P(p, "TargetGroupArn");
        _tgs.TryRemove(arn, out _);
        _targets.TryRemove(arn, out _);
        _tgAttrs.TryRemove(arn, out _);
        _tags.TryRemove(arn, out _);
        return Empty("DeleteTargetGroup");
    }

    private ServiceResponse DescribeTargetGroupAttributes(Dictionary<string, string[]> p)
    {
        var arn = P(p, "TargetGroupArn");
        if (!_tgs.ContainsKey(arn))
            return Error("TargetGroupNotFound", $"Target group '{arn}' not found.", 400);

        var attrs = _tgAttrs.TryGetValue(arn, out var a) ? a : [];
        return Xml(200, "DescribeTargetGroupAttributes",
            $"<Attributes>{AttrsXml(attrs)}</Attributes>");
    }

    private ServiceResponse ModifyTargetGroupAttributes(Dictionary<string, string[]> p)
    {
        var arn = P(p, "TargetGroupArn");
        if (!_tgs.ContainsKey(arn))
            return Error("TargetGroupNotFound", $"Target group '{arn}' not found.", 400);

        if (!_tgAttrs.TryGetValue(arn, out var attrs))
        {
            attrs = [];
            _tgAttrs[arn] = attrs;
        }

        var idx = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i2 = 0; i2 < attrs.Count; i2++)
            idx[attrs[i2]["Key"]] = i2;

        for (var i = 1; ; i++)
        {
            var key = P(p, $"Attributes.member.{i}.Key");
            if (string.IsNullOrEmpty(key)) break;
            var val = P(p, $"Attributes.member.{i}.Value");
            if (idx.TryGetValue(key, out var existingIdx))
            {
                attrs[existingIdx]["Value"] = val;
            }
            else
            {
                attrs.Add(new Dictionary<string, string> { ["Key"] = key, ["Value"] = val });
                idx[key] = attrs.Count - 1;
            }
        }

        return Xml(200, "ModifyTargetGroupAttributes",
            $"<Attributes>{AttrsXml(attrs)}</Attributes>");
    }

    // ── Listener handlers ──────────────────────────────────────────────────────

    private ServiceResponse CreateListener(Dictionary<string, string[]> p)
    {
        var lbArn = P(p, "LoadBalancerArn");
        if (!_lbs.TryGetValue(lbArn, out var lb))
            return Error("LoadBalancerNotFound", $"Load balancer '{lbArn}' not found.", 400);

        var lid = ShortId();
        var lbName = (string)lb["LoadBalancerName"];
        var lbId = lbArn.Split('/')[^1];

        var lArn = $"arn:aws:elasticloadbalancing:{Region}:{AccountContext.GetAccountId()}"
                   + $":listener/app/{lbName}/{lbId}/{lid}";

        var actions = ParseActions(p, "DefaultActions");

        // Link target groups to the LB
        foreach (var action in actions)
        {
            if (action.TryGetValue("TargetGroupArn", out var tgArnObj))
            {
                var tgArn = (string)tgArnObj;
                if (_tgs.TryGetValue(tgArn, out var tg))
                {
                    var lbArns = (List<string>)tg["LoadBalancerArns"];
                    if (!lbArns.Contains(lbArn))
                        lbArns.Add(lbArn);
                }
            }
        }

        var portStr = P(p, "Port", "80");
        var listener = new Dictionary<string, object>
        {
            ["ListenerArn"] = lArn,
            ["LoadBalancerArn"] = lbArn,
            ["Port"] = int.Parse(string.IsNullOrEmpty(portStr) ? "80" : portStr),
            ["Protocol"] = P(p, "Protocol", "HTTP"),
            ["DefaultActions"] = actions,
        };

        _listeners[lArn] = listener;
        _tags[lArn] = ParseTags(p);

        // Auto-create default rule
        var ruleId = ShortId();
        var ruleArn = $"arn:aws:elasticloadbalancing:{Region}:{AccountContext.GetAccountId()}"
                      + $":listener-rule/app/{lbName}/{lbId}/{lid}/{ruleId}";

        _rules[ruleArn] = new Dictionary<string, object>
        {
            ["RuleArn"] = ruleArn,
            ["ListenerArn"] = lArn,
            ["Priority"] = "default",
            ["Conditions"] = new List<Dictionary<string, object>>(),
            ["Actions"] = actions,
            ["IsDefault"] = true,
        };

        return Xml(200, "CreateListener", $"<Listeners>{ListenerXml(listener)}</Listeners>");
    }

    private ServiceResponse DescribeListeners(Dictionary<string, string[]> p)
    {
        var lbArn = P(p, "LoadBalancerArn");
        var arnFilter = ParseMemberList(p, "ListenerArns");
        var results = _listeners.Values.ToList();

        if (!string.IsNullOrEmpty(lbArn))
            results = results.Where(l => (string)l["LoadBalancerArn"] == lbArn).ToList();
        if (arnFilter.Count > 0)
            results = results.Where(l => arnFilter.Contains((string)l["ListenerArn"])).ToList();

        var sb = new StringBuilder();
        foreach (var l in results)
            sb.Append(ListenerXml(l));

        return Xml(200, "DescribeListeners", $"<Listeners>{sb}</Listeners>");
    }

    private ServiceResponse ModifyListener(Dictionary<string, string[]> p)
    {
        var arn = P(p, "ListenerArn");
        if (!_listeners.TryGetValue(arn, out var listener))
            return Error("ListenerNotFound", $"Listener '{arn}' not found.", 400);

        var port = P(p, "Port");
        if (!string.IsNullOrEmpty(port)) listener["Port"] = int.Parse(port);

        var protocol = P(p, "Protocol");
        if (!string.IsNullOrEmpty(protocol)) listener["Protocol"] = protocol;

        var actions = ParseActions(p, "DefaultActions");
        if (actions.Count > 0) listener["DefaultActions"] = actions;

        return Xml(200, "ModifyListener", $"<Listeners>{ListenerXml(listener)}</Listeners>");
    }

    private ServiceResponse DeleteListener(Dictionary<string, string[]> p)
    {
        var arn = P(p, "ListenerArn");
        _listeners.TryRemove(arn, out _);
        _tags.TryRemove(arn, out _);

        // Remove all rules for this listener
        var ruleArnsToRemove = _rules.Items
            .Where(kv => kv.Value.TryGetValue("ListenerArn", out var la) && (string)la == arn)
            .Select(kv => kv.Key)
            .ToList();
        foreach (var rArn in ruleArnsToRemove)
            _rules.TryRemove(rArn, out _);

        return Empty("DeleteListener");
    }

    // ── Rule handlers ──────────────────────────────────────────────────────────

    private ServiceResponse CreateRule(Dictionary<string, string[]> p)
    {
        var lArn = P(p, "ListenerArn");
        if (!_listeners.TryGetValue(lArn, out var listener))
            return Error("ListenerNotFound", $"Listener '{lArn}' not found.", 400);

        var lbArn = (string)listener["LoadBalancerArn"];
        var lbName = (string)_lbs[lbArn]["LoadBalancerName"];
        var lbId = lbArn.Split('/')[^1];
        var lId = lArn.Split('/')[^1];
        var ruleId = ShortId();

        var ruleArn = $"arn:aws:elasticloadbalancing:{Region}:{AccountContext.GetAccountId()}"
                      + $":listener-rule/app/{lbName}/{lbId}/{lId}/{ruleId}";

        var rule = new Dictionary<string, object>
        {
            ["RuleArn"] = ruleArn,
            ["ListenerArn"] = lArn,
            ["Priority"] = P(p, "Priority", "1"),
            ["Conditions"] = ParseConditions(p),
            ["Actions"] = ParseActions(p, "Actions"),
            ["IsDefault"] = false,
        };

        _rules[ruleArn] = rule;
        _tags[ruleArn] = ParseTags(p);

        return Xml(200, "CreateRule", $"<Rules>{RuleXml(rule)}</Rules>");
    }

    private ServiceResponse DescribeRules(Dictionary<string, string[]> p)
    {
        var lArn = P(p, "ListenerArn");
        var arnFilter = ParseMemberList(p, "RuleArns");
        var results = _rules.Values.ToList();

        if (!string.IsNullOrEmpty(lArn))
            results = results.Where(r => r.TryGetValue("ListenerArn", out var la) && (string)la == lArn).ToList();
        if (arnFilter.Count > 0)
            results = results.Where(r => arnFilter.Contains((string)r["RuleArn"])).ToList();

        var sb = new StringBuilder();
        foreach (var r in results)
            sb.Append(RuleXml(r));

        return Xml(200, "DescribeRules", $"<Rules>{sb}</Rules>");
    }

    private ServiceResponse ModifyRule(Dictionary<string, string[]> p)
    {
        var arn = P(p, "RuleArn");
        if (!_rules.TryGetValue(arn, out var rule))
            return Error("RuleNotFound", $"Rule '{arn}' not found.", 400);

        var conds = ParseConditions(p);
        if (conds.Count > 0) rule["Conditions"] = conds;

        var acts = ParseActions(p, "Actions");
        if (acts.Count > 0) rule["Actions"] = acts;

        return Xml(200, "ModifyRule", $"<Rules>{RuleXml(rule)}</Rules>");
    }

    private ServiceResponse DeleteRule(Dictionary<string, string[]> p)
    {
        var arn = P(p, "RuleArn");
        if (_rules.TryGetValue(arn, out var rule) && rule.TryGetValue("IsDefault", out var isDefault) && (bool)isDefault)
            return Error("OperationNotPermitted", "Cannot delete a default rule.", 400);

        _rules.TryRemove(arn, out _);
        _tags.TryRemove(arn, out _);
        return Empty("DeleteRule");
    }

    private ServiceResponse SetRulePriorities(Dictionary<string, string[]> p)
    {
        var updated = new List<Dictionary<string, object>>();
        for (var i = 1; ; i++)
        {
            var arn = P(p, $"RulePriorities.member.{i}.RuleArn");
            if (string.IsNullOrEmpty(arn)) break;
            var priority = P(p, $"RulePriorities.member.{i}.Priority");
            if (_rules.TryGetValue(arn, out var rule))
            {
                rule["Priority"] = priority;
                updated.Add(rule);
            }
        }

        var sb = new StringBuilder();
        foreach (var r in updated)
            sb.Append(RuleXml(r));

        return Xml(200, "SetRulePriorities", $"<Rules>{sb}</Rules>");
    }

    // ── Target registration handlers ───────────────────────────────────────────

    private ServiceResponse RegisterTargets(Dictionary<string, string[]> p)
    {
        var tgArn = P(p, "TargetGroupArn");
        if (!_tgs.ContainsKey(tgArn))
            return Error("TargetGroupNotFound", $"Target group '{tgArn}' not found.", 400);

        var newTargets = ParseTargetsParam(p);

        if (!_targets.TryGetValue(tgArn, out var existing))
        {
            existing = [];
            _targets[tgArn] = existing;
        }

        var existingIds = new HashSet<string>(existing.Select(t => (string)t["Id"]));
        foreach (var t in newTargets)
        {
            if (existingIds.Add((string)t["Id"]))
                existing.Add(t);
        }

        return Empty("RegisterTargets");
    }

    private ServiceResponse DeregisterTargets(Dictionary<string, string[]> p)
    {
        var tgArn = P(p, "TargetGroupArn");
        if (!_tgs.ContainsKey(tgArn))
            return Error("TargetGroupNotFound", $"Target group '{tgArn}' not found.", 400);

        var toRemove = new HashSet<string>(ParseTargetsParam(p).Select(t => (string)t["Id"]));

        if (_targets.TryGetValue(tgArn, out var existing))
            _targets[tgArn] = existing.Where(t => !toRemove.Contains((string)t["Id"])).ToList();

        return Empty("DeregisterTargets");
    }

    private ServiceResponse DescribeTargetHealth(Dictionary<string, string[]> p)
    {
        var tgArn = P(p, "TargetGroupArn");
        if (!_tgs.TryGetValue(tgArn, out var tg))
            return Error("TargetGroupNotFound", $"Target group '{tgArn}' not found.", 400);

        var registered = _targets.TryGetValue(tgArn, out var tgts) ? tgts : [];
        var targetFilter = new HashSet<string>(ParseTargetsParam(p).Select(t => (string)t["Id"]));
        if (targetFilter.Count > 0)
            registered = registered.Where(t => targetFilter.Contains((string)t["Id"])).ToList();

        var defaultPort = (int)tg["Port"];
        var sb = new StringBuilder();
        foreach (var t in registered)
        {
            var port = t.TryGetValue("Port", out var portObj) ? (int)portObj : defaultPort;
            sb.Append("<member>"
                      + $"<Target><Id>{t["Id"]}</Id><Port>{port}</Port></Target>"
                      + "<HealthStatus>healthy</HealthStatus>"
                      + "<TargetHealth><State>healthy</State></TargetHealth>"
                      + "</member>");
        }

        return Xml(200, "DescribeTargetHealth",
            $"<TargetHealthDescriptions>{sb}</TargetHealthDescriptions>");
    }

    // ── Tag handlers ───────────────────────────────────────────────────────────

    private ServiceResponse AddTags(Dictionary<string, string[]> p)
    {
        var arns = ParseMemberList(p, "ResourceArns");
        var newTags = ParseTags(p);

        foreach (var arn in arns)
        {
            if (!_tags.TryGetValue(arn, out var existing))
            {
                existing = [];
                _tags[arn] = existing;
            }

            var idx = new Dictionary<string, int>(StringComparer.Ordinal);
            for (var i2 = 0; i2 < existing.Count; i2++)
                idx[existing[i2]["Key"]] = i2;

            foreach (var tag in newTags)
            {
                if (idx.TryGetValue(tag["Key"], out var existingIdx))
                {
                    existing[existingIdx]["Value"] = tag["Value"];
                }
                else
                {
                    existing.Add(new Dictionary<string, string> { ["Key"] = tag["Key"], ["Value"] = tag["Value"] });
                    idx[tag["Key"]] = existing.Count - 1;
                }
            }
        }

        return Empty("AddTags");
    }

    private ServiceResponse RemoveTags(Dictionary<string, string[]> p)
    {
        var arns = ParseMemberList(p, "ResourceArns");
        var keySet = new HashSet<string>(ParseMemberList(p, "TagKeys"));

        foreach (var arn in arns)
        {
            if (_tags.TryGetValue(arn, out var existing))
                _tags[arn] = existing.Where(t => !keySet.Contains(t["Key"])).ToList();
        }

        return Empty("RemoveTags");
    }

    private ServiceResponse DescribeTags(Dictionary<string, string[]> p)
    {
        var arns = ParseMemberList(p, "ResourceArns");
        var sb = new StringBuilder();

        foreach (var arn in arns)
        {
            var tagsXml = new StringBuilder();
            if (_tags.TryGetValue(arn, out var tags))
            {
                foreach (var t in tags)
                    tagsXml.Append($"<member><Key>{t["Key"]}</Key><Value>{t["Value"]}</Value></member>");
            }
            sb.Append($"<member><ResourceArn>{arn}</ResourceArn><Tags>{tagsXml}</Tags></member>");
        }

        return Xml(200, "DescribeTags", $"<TagDescriptions>{sb}</TagDescriptions>");
    }

    // ── XML serializers ────────────────────────────────────────────────────────

    private static string LbXml(Dictionary<string, object> lb)
    {
        var subnets = (List<string>)lb["Subnets"];
        var azs = new StringBuilder();
        foreach (var s in subnets)
        {
            azs.Append($"<member><ZoneName>{Region}a</ZoneName><SubnetId>{s}</SubnetId>"
                       + "<LoadBalancerAddresses/></member>");
        }

        var sgs = (List<string>)lb["SecurityGroups"];
        var sgXml = new StringBuilder();
        foreach (var sg in sgs)
            sgXml.Append($"<member>{sg}</member>");

        return "<member>"
               + $"<LoadBalancerArn>{lb["LoadBalancerArn"]}</LoadBalancerArn>"
               + $"<LoadBalancerName>{lb["LoadBalancerName"]}</LoadBalancerName>"
               + $"<DNSName>{lb["DNSName"]}</DNSName>"
               + "<CanonicalHostedZoneId>Z35SXDOTRQ7X7K</CanonicalHostedZoneId>"
               + $"<CreatedTime>{lb["CreatedTime"]}</CreatedTime>"
               + $"<Scheme>{lb["Scheme"]}</Scheme>"
               + $"<VpcId>{lb.GetValueOrDefault("VpcId", "")}</VpcId>"
               + $"<State><Code>{lb["State"]}</Code></State>"
               + $"<Type>{lb["Type"]}</Type>"
               + $"<AvailabilityZones>{azs}</AvailabilityZones>"
               + $"<SecurityGroups>{sgXml}</SecurityGroups>"
               + $"<IpAddressType>{lb.GetValueOrDefault("IpAddressType", "ipv4")}</IpAddressType>"
               + "</member>";
    }

    private static string TgXml(Dictionary<string, object> tg)
    {
        var lbArns = (List<string>)tg["LoadBalancerArns"];
        var lbArnXml = new StringBuilder();
        foreach (var a in lbArns)
            lbArnXml.Append($"<member>{a}</member>");

        var matcher = (Dictionary<string, string>)tg["Matcher"];
        var hcEnabled = (bool)tg["HealthCheckEnabled"] ? "true" : "false";

        return "<member>"
               + $"<TargetGroupArn>{tg["TargetGroupArn"]}</TargetGroupArn>"
               + $"<TargetGroupName>{tg["TargetGroupName"]}</TargetGroupName>"
               + $"<Protocol>{tg.GetValueOrDefault("Protocol", "HTTP")}</Protocol>"
               + $"<Port>{tg.GetValueOrDefault("Port", 80)}</Port>"
               + $"<VpcId>{tg.GetValueOrDefault("VpcId", "")}</VpcId>"
               + $"<HealthCheckProtocol>{tg.GetValueOrDefault("HealthCheckProtocol", "HTTP")}</HealthCheckProtocol>"
               + $"<HealthCheckPort>{tg.GetValueOrDefault("HealthCheckPort", "traffic-port")}</HealthCheckPort>"
               + $"<HealthCheckEnabled>{hcEnabled}</HealthCheckEnabled>"
               + $"<HealthCheckPath>{tg.GetValueOrDefault("HealthCheckPath", "/")}</HealthCheckPath>"
               + $"<HealthCheckIntervalSeconds>{tg.GetValueOrDefault("HealthCheckIntervalSeconds", 30)}</HealthCheckIntervalSeconds>"
               + $"<HealthCheckTimeoutSeconds>{tg.GetValueOrDefault("HealthCheckTimeoutSeconds", 5)}</HealthCheckTimeoutSeconds>"
               + $"<HealthyThresholdCount>{tg.GetValueOrDefault("HealthyThresholdCount", 5)}</HealthyThresholdCount>"
               + $"<UnhealthyThresholdCount>{tg.GetValueOrDefault("UnhealthyThresholdCount", 2)}</UnhealthyThresholdCount>"
               + $"<Matcher><HttpCode>{matcher.GetValueOrDefault("HttpCode", "200")}</HttpCode></Matcher>"
               + $"<LoadBalancerArns>{lbArnXml}</LoadBalancerArns>"
               + $"<TargetType>{tg.GetValueOrDefault("TargetType", "instance")}</TargetType>"
               + "</member>";
    }

    private static string ActionXml(Dictionary<string, object> a)
    {
        var sb = new StringBuilder();
        sb.Append($"<Type>{a["Type"]}</Type>");
        sb.Append($"<Order>{a.GetValueOrDefault("Order", 1)}</Order>");

        if (a.TryGetValue("TargetGroupArn", out var tga))
            sb.Append($"<TargetGroupArn>{tga}</TargetGroupArn>");

        if (a.TryGetValue("RedirectConfig", out var rcObj))
        {
            var rc = (Dictionary<string, string>)rcObj;
            sb.Append("<RedirectConfig>"
                      + $"<Protocol>{rc.GetValueOrDefault("Protocol", "#{protocol}")}</Protocol>"
                      + $"<Port>{rc.GetValueOrDefault("Port", "#{port}")}</Port>"
                      + $"<Host>{rc.GetValueOrDefault("Host", "#{host}")}</Host>"
                      + $"<Path>{rc.GetValueOrDefault("Path", "/#{path}")}</Path>"
                      + $"<StatusCode>{rc.GetValueOrDefault("StatusCode", "HTTP_301")}</StatusCode>"
                      + "</RedirectConfig>");
        }

        if (a.TryGetValue("FixedResponseConfig", out var frcObj))
        {
            var frc = (Dictionary<string, string>)frcObj;
            sb.Append("<FixedResponseConfig>"
                      + $"<StatusCode>{frc.GetValueOrDefault("StatusCode", "200")}</StatusCode>"
                      + $"<ContentType>{frc.GetValueOrDefault("ContentType", "text/plain")}</ContentType>"
                      + $"<MessageBody>{frc.GetValueOrDefault("MessageBody", "")}</MessageBody>"
                      + "</FixedResponseConfig>");
        }

        return $"<member>{sb}</member>";
    }

    private static string ListenerXml(Dictionary<string, object> listener)
    {
        var actions = (List<Dictionary<string, object>>)listener["DefaultActions"];
        var actXml = new StringBuilder();
        foreach (var a in actions)
            actXml.Append(ActionXml(a));

        return "<member>"
               + $"<ListenerArn>{listener["ListenerArn"]}</ListenerArn>"
               + $"<LoadBalancerArn>{listener["LoadBalancerArn"]}</LoadBalancerArn>"
               + $"<Port>{listener.GetValueOrDefault("Port", 80)}</Port>"
               + $"<Protocol>{listener.GetValueOrDefault("Protocol", "HTTP")}</Protocol>"
               + $"<DefaultActions>{actXml}</DefaultActions>"
               + "</member>";
    }

    private static string RuleXml(Dictionary<string, object> r)
    {
        var conditions = (List<Dictionary<string, object>>)r["Conditions"];
        var condXml = new StringBuilder();
        foreach (var c in conditions)
        {
            var vals = (List<string>)c["Values"];
            var valsXml = new StringBuilder();
            foreach (var v in vals)
                valsXml.Append($"<member>{v}</member>");
            condXml.Append($"<member><Field>{c["Field"]}</Field><Values>{valsXml}</Values></member>");
        }

        var actions = (List<Dictionary<string, object>>)r["Actions"];
        var actXml = new StringBuilder();
        foreach (var a in actions)
            actXml.Append(ActionXml(a));

        var isDefault = r.TryGetValue("IsDefault", out var def) && (bool)def ? "true" : "false";

        return "<member>"
               + $"<RuleArn>{r["RuleArn"]}</RuleArn>"
               + $"<Priority>{r["Priority"]}</Priority>"
               + $"<Conditions>{condXml}</Conditions>"
               + $"<Actions>{actXml}</Actions>"
               + $"<IsDefault>{isDefault}</IsDefault>"
               + "</member>";
    }

    private static string AttrsXml(List<Dictionary<string, string>> attrs)
    {
        var sb = new StringBuilder();
        foreach (var a in attrs)
            sb.Append($"<member><Key>{a["Key"]}</Key><Value>{a["Value"]}</Value></member>");
        return sb.ToString();
    }

    // ── Parse helpers ──────────────────────────────────────────────────────────

    private static Dictionary<string, string[]> ParseParams(ServiceRequest request)
    {
        var result = new Dictionary<string, string[]>(StringComparer.Ordinal);
        if (request.Body.Length == 0) return result;
        var body = Encoding.UTF8.GetString(request.Body);
        if (string.IsNullOrEmpty(body)) return result;

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

    private static List<string> ParseMemberList(Dictionary<string, string[]> p, string prefix)
    {
        var items = new List<string>();
        for (var i = 1; ; i++)
        {
            var val = P(p, $"{prefix}.member.{i}");
            if (string.IsNullOrEmpty(val)) break;
            items.Add(val);
        }

        return items;
    }

    private static List<Dictionary<string, string>> ParseTags(Dictionary<string, string[]> p)
    {
        var tags = new List<Dictionary<string, string>>();
        for (var i = 1; ; i++)
        {
            var k = P(p, $"Tags.member.{i}.Key");
            if (string.IsNullOrEmpty(k)) break;
            tags.Add(new Dictionary<string, string>
            {
                ["Key"] = k,
                ["Value"] = P(p, $"Tags.member.{i}.Value"),
            });
        }

        return tags;
    }

    private static List<Dictionary<string, object>> ParseActions(Dictionary<string, string[]> p, string prefix)
    {
        var actions = new List<Dictionary<string, object>>();
        for (var i = 1; ; i++)
        {
            var t = P(p, $"{prefix}.member.{i}.Type");
            if (string.IsNullOrEmpty(t)) break;

            var orderStr = P(p, $"{prefix}.member.{i}.Order", i.ToString());
            var action = new Dictionary<string, object>
            {
                ["Type"] = t,
                ["Order"] = int.Parse(orderStr),
            };

            var tgArn = P(p, $"{prefix}.member.{i}.TargetGroupArn");
            if (!string.IsNullOrEmpty(tgArn))
                action["TargetGroupArn"] = tgArn;

            // RedirectConfig
            var rcCode = P(p, $"{prefix}.member.{i}.RedirectConfig.StatusCode");
            if (!string.IsNullOrEmpty(rcCode))
            {
                action["RedirectConfig"] = new Dictionary<string, string>
                {
                    ["Protocol"] = P(p, $"{prefix}.member.{i}.RedirectConfig.Protocol", "#{protocol}"),
                    ["Port"] = P(p, $"{prefix}.member.{i}.RedirectConfig.Port", "#{port}"),
                    ["Host"] = P(p, $"{prefix}.member.{i}.RedirectConfig.Host", "#{host}"),
                    ["Path"] = P(p, $"{prefix}.member.{i}.RedirectConfig.Path", "/#{path}"),
                    ["StatusCode"] = rcCode,
                };
            }

            // FixedResponseConfig
            var frCode = P(p, $"{prefix}.member.{i}.FixedResponseConfig.StatusCode");
            if (!string.IsNullOrEmpty(frCode))
            {
                action["FixedResponseConfig"] = new Dictionary<string, string>
                {
                    ["StatusCode"] = frCode,
                    ["ContentType"] = P(p, $"{prefix}.member.{i}.FixedResponseConfig.ContentType", "text/plain"),
                    ["MessageBody"] = P(p, $"{prefix}.member.{i}.FixedResponseConfig.MessageBody"),
                };
            }

            actions.Add(action);
        }

        return actions;
    }

    private static List<Dictionary<string, object>> ParseConditions(Dictionary<string, string[]> p)
    {
        return ParseConditions(p, "Conditions");
    }

    private static List<Dictionary<string, object>> ParseConditions(Dictionary<string, string[]> p, string prefix)
    {
        var conditions = new List<Dictionary<string, object>>();
        for (var i = 1; ; i++)
        {
            var field = P(p, $"{prefix}.member.{i}.Field");
            if (string.IsNullOrEmpty(field)) break;

            var values = new List<string>();
            for (var j = 1; ; j++)
            {
                var v = P(p, $"{prefix}.member.{i}.Values.member.{j}");
                if (string.IsNullOrEmpty(v)) break;
                values.Add(v);
            }

            conditions.Add(new Dictionary<string, object>
            {
                ["Field"] = field,
                ["Values"] = values,
            });
        }

        return conditions;
    }

    private static List<Dictionary<string, object>> ParseTargetsParam(Dictionary<string, string[]> p)
    {
        return ParseTargetsParam(p, "Targets");
    }

    private static List<Dictionary<string, object>> ParseTargetsParam(Dictionary<string, string[]> p, string prefix)
    {
        var targets = new List<Dictionary<string, object>>();
        for (var i = 1; ; i++)
        {
            var tid = P(p, $"{prefix}.member.{i}.Id");
            if (string.IsNullOrEmpty(tid)) break;

            var t = new Dictionary<string, object> { ["Id"] = tid };
            var port = P(p, $"{prefix}.member.{i}.Port");
            if (!string.IsNullOrEmpty(port))
                t["Port"] = int.Parse(port);

            targets.Add(t);
        }

        return targets;
    }

    // ── XML / Error response builders ─────────────────────────────────────────

    private static readonly IReadOnlyDictionary<string, string> XmlHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "text/xml" };

    private static ServiceResponse Xml(int status, string action, string inner)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                   + $"<{action}Response xmlns=\"{Ns}\">"
                   + $"<{action}Result>{inner}</{action}Result>"
                   + $"<ResponseMetadata><RequestId>{requestId}</RequestId></ResponseMetadata>"
                   + $"</{action}Response>";
        return new ServiceResponse(status, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }

    private static ServiceResponse Empty(string action)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                   + $"<{action}Response xmlns=\"{Ns}\">"
                   + $"<{action}Result/>"
                   + $"<ResponseMetadata><RequestId>{requestId}</RequestId></ResponseMetadata>"
                   + $"</{action}Response>";
        return new ServiceResponse(200, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }

    private static ServiceResponse Error(string code, string message, int status)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                   + $"<ErrorResponse xmlns=\"{Ns}\">"
                   + $"<Error><Code>{code}</Code><Message>{message}</Message></Error>"
                   + $"<RequestId>{requestId}</RequestId>"
                   + "</ErrorResponse>";
        return new ServiceResponse(status, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }

    // ── Utility helpers ────────────────────────────────────────────────────────

    private static string ShortId()
    {
        return Guid.NewGuid().ToString("N")[..16];
    }

    private static string NowIso()
    {
        return DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.000Z");
    }

    private static int ParseIntOrDefault(string value, int defaultValue)
    {
        if (string.IsNullOrEmpty(value)) return defaultValue;
        return int.TryParse(value, out var result) ? result : defaultValue;
    }
}
