using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Emr;

/// <summary>
/// EMR (Elastic MapReduce) service handler -- JSON protocol via X-Amz-Target: ElasticMapReduce.{Action}.
///
/// Port of ministack/services/emr.py.
///
/// Supports:
///   Clusters:        RunJobFlow, DescribeCluster, ListClusters, TerminateJobFlows,
///                    ModifyCluster, SetTerminationProtection, SetVisibleToAllUsers
///   Steps:           AddJobFlowSteps, DescribeStep, ListSteps, CancelSteps
///   Instance Fleets: AddInstanceFleet, ListInstanceFleets, ModifyInstanceFleet
///   Instance Groups: AddInstanceGroups, ListInstanceGroups, ModifyInstanceGroups
///   Bootstrap:       ListBootstrapActions
///   Tags:            AddTags, RemoveTags
///   Block Public Access: GetBlockPublicAccessConfiguration, PutBlockPublicAccessConfiguration
/// </summary>
internal sealed class EmrServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    private static readonly char[] IdChars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();

    // In-memory state
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _clusters = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, object?>>> _steps = new();
    private bool _blockPublicSecurityGroupRules;
    private List<Dictionary<string, object?>> _permittedRanges = [];

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "elasticmapreduce";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var target = request.GetHeader("x-amz-target") ?? "";
        var action = target.Contains('.', StringComparison.Ordinal)
            ? target[(target.LastIndexOf('.') + 1)..]
            : "";

        JsonElement data;
        if (request.Body.Length > 0)
        {
            try
            {
                using var doc = JsonDocument.Parse(request.Body);
                data = doc.RootElement.Clone();
            }
            catch (JsonException)
            {
                return Task.FromResult(
                    AwsResponseHelpers.ErrorResponseJson("SerializationException", "Invalid JSON", 400));
            }
        }
        else
        {
            data = JsonDocument.Parse("{}").RootElement.Clone();
        }

        var response = action switch
        {
            "RunJobFlow" => ActRunJobFlow(data),
            "DescribeCluster" => ActDescribeCluster(data),
            "ListClusters" => ActListClusters(data),
            "TerminateJobFlows" => ActTerminateJobFlows(data),
            "ModifyCluster" => ActModifyCluster(data),
            "SetTerminationProtection" => ActSetTerminationProtection(data),
            "SetVisibleToAllUsers" => ActSetVisibleToAllUsers(data),
            "AddJobFlowSteps" => ActAddJobFlowSteps(data),
            "DescribeStep" => ActDescribeStep(data),
            "ListSteps" => ActListSteps(data),
            "CancelSteps" => ActCancelSteps(data),
            "AddInstanceFleet" => ActAddInstanceFleet(data),
            "ListInstanceFleets" => ActListInstanceFleets(data),
            "ModifyInstanceFleet" => ActModifyInstanceFleet(data),
            "AddInstanceGroups" => ActAddInstanceGroups(data),
            "ListInstanceGroups" => ActListInstanceGroups(data),
            "ModifyInstanceGroups" => ActModifyInstanceGroups(data),
            "ListBootstrapActions" => ActListBootstrapActions(data),
            "AddTags" => ActAddTags(data),
            "RemoveTags" => ActRemoveTags(data),
            "GetBlockPublicAccessConfiguration" => ActGetBlockPublicAccessConfig(data),
            "PutBlockPublicAccessConfiguration" => ActPutBlockPublicAccessConfig(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown EMR action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _clusters.Clear();
            _steps.Clear();
            _blockPublicSecurityGroupRules = false;
            _permittedRanges = [];
        }
    }

    public JsonElement? GetState() => null;

    public void RestoreState(JsonElement state) { }

    // -- ID generators ---------------------------------------------------------

    private static string GenerateClusterId()
    {
        return "j-" + GenerateRandomId(13);
    }

    private static string GenerateStepId()
    {
        return "s-" + GenerateRandomId(13);
    }

    private static string GenerateFleetId()
    {
        return "if-" + GenerateRandomId(13);
    }

    private static string GenerateGroupId()
    {
        return "ig-" + GenerateRandomId(13);
    }

    private static string GenerateRandomId(int length)
    {
        return string.Create(length, (object?)null, static (span, _) =>
        {
            for (var i = 0; i < span.Length; i++)
            {
                span[i] = IdChars[Random.Shared.Next(IdChars.Length)];
            }
        });
    }

    // -- JSON helpers ----------------------------------------------------------

    private static string? GetString(JsonElement el, string prop)
    {
        return el.TryGetProperty(prop, out var p) && p.ValueKind == JsonValueKind.String
            ? p.GetString()
            : null;
    }

    private static int GetInt(JsonElement el, string prop, int defaultValue)
    {
        if (!el.TryGetProperty(prop, out var p))
        {
            return defaultValue;
        }

        return p.TryGetInt32(out var val) ? val : defaultValue;
    }

    private static bool GetBool(JsonElement el, string prop, bool defaultValue)
    {
        if (!el.TryGetProperty(prop, out var p))
        {
            return defaultValue;
        }

        return p.ValueKind == JsonValueKind.True || (p.ValueKind != JsonValueKind.False && defaultValue);
    }

    private static List<Dictionary<string, object?>> GetArrayOfObjects(JsonElement el, string prop)
    {
        if (!el.TryGetProperty(prop, out var arr) || arr.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        return arr.EnumerateArray().Select(JsonElementToDict).ToList();
    }

    private static Dictionary<string, object?> JsonElementToDict(JsonElement el)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (el.ValueKind != JsonValueKind.Object)
        {
            return dict;
        }

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
            JsonValueKind.Array => el.EnumerateArray().Select(JsonElementToObject).ToList(),
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? l : el.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };
    }

    // -- RunJobFlow ------------------------------------------------------------

    private ServiceResponse ActRunJobFlow(JsonElement data)
    {
        var name = GetString(data, "Name");
        if (string.IsNullOrEmpty(name))
        {
            return AwsResponseHelpers.ErrorResponseJson("ValidationException", "Name is required", 400);
        }

        lock (_lock)
        {
            var clusterId = GenerateClusterId();
            var arn = $"arn:aws:elasticmapreduce:{Region}:{AccountContext.GetAccountId()}:cluster/{clusterId}";
            var now = TimeHelpers.NowIso();

            var instances = data.TryGetProperty("Instances", out var instEl) ? instEl : default;
            var keepAlive = instances.ValueKind == JsonValueKind.Object && GetBool(instances, "KeepJobFlowAliveWhenNoSteps", false);
            var terminationProtected = instances.ValueKind == JsonValueKind.Object && GetBool(instances, "TerminationProtected", false);
            var tags = GetArrayOfObjects(data, "Tags");
            var applications = GetArrayOfObjects(data, "Applications");
            var bootstrapActions = GetArrayOfObjects(data, "BootstrapActions");
            var releaseLabel = GetString(data, "ReleaseLabel") ?? "emr-6.10.0";
            var logUri = GetString(data, "LogUri") ?? "";
            var serviceRole = GetString(data, "ServiceRole") ?? "EMR_DefaultRole";
            var jobFlowRole = GetString(data, "JobFlowRole") ?? "EMR_EC2_DefaultRole";
            var visibleToAll = GetBool(data, "VisibleToAllUsers", true);
            var stepConcurrency = GetInt(data, "StepConcurrencyLevel", 1);
            var initialState = keepAlive ? "WAITING" : "TERMINATED";

            var instanceFleets = new List<Dictionary<string, object?>>();
            var instanceGroups = new List<Dictionary<string, object?>>();

            if (instances.ValueKind == JsonValueKind.Object && instances.TryGetProperty("InstanceFleets", out var fleetsEl) && fleetsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var fleet in fleetsEl.EnumerateArray())
                {
                    var fleetType = GetString(fleet, "InstanceFleetType") ?? "MASTER";
                    var onDemand = GetInt(fleet, "TargetOnDemandCapacity", 0);
                    var spot = GetInt(fleet, "TargetSpotCapacity", 0);
                    instanceFleets.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["Id"] = GenerateFleetId(),
                        ["Name"] = GetString(fleet, "Name") ?? fleetType,
                        ["Status"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                        {
                            ["State"] = "RUNNING",
                            ["StateChangeReason"] = new Dictionary<string, object?>(StringComparer.Ordinal),
                            ["Timeline"] = new Dictionary<string, object?>(StringComparer.Ordinal) { ["CreationDateTime"] = now },
                        },
                        ["InstanceFleetType"] = fleetType,
                        ["TargetOnDemandCapacity"] = onDemand,
                        ["TargetSpotCapacity"] = spot,
                        ["ProvisionedOnDemandCapacity"] = onDemand,
                        ["ProvisionedSpotCapacity"] = spot,
                        ["InstanceTypeSpecifications"] = GetArrayOfObjects(fleet, "InstanceTypeConfigs"),
                    });
                }
            }
            else if (instances.ValueKind == JsonValueKind.Object && instances.TryGetProperty("InstanceGroups", out var groupsEl) && groupsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var ig in groupsEl.EnumerateArray())
                {
                    var role = GetString(ig, "InstanceRole") ?? "MASTER";
                    var count = GetInt(ig, "InstanceCount", 1);
                    instanceGroups.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["Id"] = GenerateGroupId(),
                        ["Name"] = GetString(ig, "Name") ?? role,
                        ["Market"] = GetString(ig, "Market") ?? "ON_DEMAND",
                        ["InstanceGroupType"] = role,
                        ["InstanceType"] = GetString(ig, "InstanceType") ?? "m5.xlarge",
                        ["RequestedInstanceCount"] = count,
                        ["RunningInstanceCount"] = count,
                        ["Status"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                        {
                            ["State"] = "RUNNING",
                            ["StateChangeReason"] = new Dictionary<string, object?>(StringComparer.Ordinal),
                            ["Timeline"] = new Dictionary<string, object?>(StringComparer.Ordinal) { ["CreationDateTime"] = now },
                        },
                    });
                }
            }
            else
            {
                var masterType = instances.ValueKind == JsonValueKind.Object ? (GetString(instances, "MasterInstanceType") ?? "m5.xlarge") : "m5.xlarge";
                var slaveType = instances.ValueKind == JsonValueKind.Object ? (GetString(instances, "SlaveInstanceType") ?? "m5.xlarge") : "m5.xlarge";
                var instanceCount = instances.ValueKind == JsonValueKind.Object ? GetInt(instances, "InstanceCount", 1) : 1;

                instanceGroups.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["Id"] = GenerateGroupId(),
                    ["Name"] = "Master",
                    ["Market"] = "ON_DEMAND",
                    ["InstanceGroupType"] = "MASTER",
                    ["InstanceType"] = masterType,
                    ["RequestedInstanceCount"] = 1,
                    ["RunningInstanceCount"] = 1,
                    ["Status"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["State"] = "RUNNING",
                        ["StateChangeReason"] = new Dictionary<string, object?>(StringComparer.Ordinal),
                        ["Timeline"] = new Dictionary<string, object?>(StringComparer.Ordinal) { ["CreationDateTime"] = now },
                    },
                });

                if (instanceCount > 1)
                {
                    instanceGroups.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["Id"] = GenerateGroupId(),
                        ["Name"] = "Core",
                        ["Market"] = "ON_DEMAND",
                        ["InstanceGroupType"] = "CORE",
                        ["InstanceType"] = slaveType,
                        ["RequestedInstanceCount"] = instanceCount - 1,
                        ["RunningInstanceCount"] = instanceCount - 1,
                        ["Status"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                        {
                            ["State"] = "RUNNING",
                            ["StateChangeReason"] = new Dictionary<string, object?>(StringComparer.Ordinal),
                            ["Timeline"] = new Dictionary<string, object?>(StringComparer.Ordinal) { ["CreationDateTime"] = now },
                        },
                    });
                }
            }

            var collectionType = instanceFleets.Count > 0 ? "INSTANCE_FLEET" : "INSTANCE_GROUP";

            var ec2SubnetId = instances.ValueKind == JsonValueKind.Object ? (GetString(instances, "Ec2SubnetId") ?? "") : "";
            var ec2KeyName = instances.ValueKind == JsonValueKind.Object ? (GetString(instances, "Ec2KeyName") ?? "") : "";
            var masterSg = instances.ValueKind == JsonValueKind.Object ? (GetString(instances, "EmrManagedMasterSecurityGroup") ?? "") : "";
            var slaveSg = instances.ValueKind == JsonValueKind.Object ? (GetString(instances, "EmrManagedSlaveSecurityGroup") ?? "") : "";

            var cluster = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Id"] = clusterId,
                ["Name"] = name,
                ["ClusterArn"] = arn,
                ["Status"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["State"] = initialState,
                    ["StateChangeReason"] = new Dictionary<string, object?>(StringComparer.Ordinal) { ["Code"] = "", ["Message"] = "" },
                    ["Timeline"] = new Dictionary<string, object?>(StringComparer.Ordinal) { ["CreationDateTime"] = now, ["ReadyDateTime"] = now },
                },
                ["Ec2InstanceAttributes"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["Ec2KeyName"] = ec2KeyName,
                    ["Ec2SubnetId"] = ec2SubnetId,
                    ["Ec2AvailabilityZone"] = $"{Region}a",
                    ["IamInstanceProfile"] = jobFlowRole,
                    ["EmrManagedMasterSecurityGroup"] = masterSg,
                    ["EmrManagedSlaveSecurityGroup"] = slaveSg,
                },
                ["InstanceCollectionType"] = collectionType,
                ["LogUri"] = logUri,
                ["ReleaseLabel"] = releaseLabel,
                ["AutoTerminate"] = !keepAlive,
                ["TerminationProtected"] = terminationProtected,
                ["VisibleToAllUsers"] = visibleToAll,
                ["Applications"] = applications,
                ["Tags"] = tags,
                ["ServiceRole"] = serviceRole,
                ["NormalizedInstanceHours"] = 0,
                ["MasterPublicDnsName"] = "ec2-0-0-0-0.compute-1.amazonaws.com",
                ["StepConcurrencyLevel"] = stepConcurrency,
                ["BootstrapActions"] = bootstrapActions,
                ["InstanceFleets"] = instanceFleets,
                ["InstanceGroups"] = instanceGroups,
            };

            _clusters[clusterId] = cluster;

            // Steps passed at creation time
            var stepsIn = GetArrayOfObjects(data, "Steps");
            var stepRecords = new List<Dictionary<string, object?>>();
            foreach (var step in stepsIn)
            {
                stepRecords.Add(MakeStep(step));
            }

            _steps[clusterId] = stepRecords;

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["JobFlowId"] = clusterId,
                ["ClusterArn"] = arn,
            });
        }
    }

    // -- Describe/List/Terminate -----------------------------------------------

    private ServiceResponse ActDescribeCluster(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        lock (_lock)
        {
            if (clusterId is null || !_clusters.TryGetValue(clusterId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Cluster id '{clusterId}' is not valid.", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Cluster"] = cluster });
        }
    }

    private ServiceResponse ActListClusters(JsonElement data)
    {
        lock (_lock)
        {
            var stateFilter = new HashSet<string>(StringComparer.Ordinal);
            if (data.TryGetProperty("ClusterStates", out var statesEl) && statesEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var s in statesEl.EnumerateArray())
                {
                    var val = s.GetString();
                    if (val is not null)
                    {
                        stateFilter.Add(val);
                    }
                }
            }

            var result = new List<Dictionary<string, object?>>();
            foreach (var c in _clusters.Values)
            {
                var status = c["Status"] as Dictionary<string, object?>;
                var state = status?["State"] as string ?? "";
                if (stateFilter.Count > 0 && !stateFilter.Contains(state))
                {
                    continue;
                }

                result.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["Id"] = c["Id"],
                    ["Name"] = c["Name"],
                    ["Status"] = c["Status"],
                    ["NormalizedInstanceHours"] = c["NormalizedInstanceHours"],
                    ["ClusterArn"] = c["ClusterArn"],
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Clusters"] = result });
        }
    }

    private ServiceResponse ActTerminateJobFlows(JsonElement data)
    {
        lock (_lock)
        {
            if (!data.TryGetProperty("JobFlowIds", out var idsEl) || idsEl.ValueKind != JsonValueKind.Array)
            {
                return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
            }

            foreach (var idEl in idsEl.EnumerateArray())
            {
                var cid = idEl.GetString();
                if (cid is null || !_clusters.TryGetValue(cid, out var cluster))
                {
                    continue;
                }

                if (cluster.TryGetValue("TerminationProtected", out var tp) && tp is true)
                {
                    return AwsResponseHelpers.ErrorResponseJson("ValidationException",
                        $"Cluster {cid} is protected from termination. Disable termination protection first.", 400);
                }

                var status = cluster["Status"] as Dictionary<string, object?>;
                if (status is not null)
                {
                    status["State"] = "TERMINATED";
                    status["StateChangeReason"] = new Dictionary<string, object?>(StringComparer.Ordinal) { ["Code"] = "USER_REQUEST", ["Message"] = "User request" };
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActModifyCluster(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        lock (_lock)
        {
            if (clusterId is null || !_clusters.TryGetValue(clusterId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Cluster id '{clusterId}' is not valid.", 400);
            }

            if (data.TryGetProperty("StepConcurrencyLevel", out var scl) && scl.TryGetInt32(out var level))
            {
                cluster["StepConcurrencyLevel"] = level;
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["StepConcurrencyLevel"] = cluster["StepConcurrencyLevel"],
            });
        }
    }

    private ServiceResponse ActSetTerminationProtection(JsonElement data)
    {
        lock (_lock)
        {
            var protectedVal = GetBool(data, "TerminationProtected", false);
            if (data.TryGetProperty("JobFlowIds", out var idsEl) && idsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var idEl in idsEl.EnumerateArray())
                {
                    var cid = idEl.GetString();
                    if (cid is not null && _clusters.TryGetValue(cid, out var cluster))
                    {
                        cluster["TerminationProtected"] = protectedVal;
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActSetVisibleToAllUsers(JsonElement data)
    {
        lock (_lock)
        {
            var visible = GetBool(data, "VisibleToAllUsers", true);
            if (data.TryGetProperty("JobFlowIds", out var idsEl) && idsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var idEl in idsEl.EnumerateArray())
                {
                    var cid = idEl.GetString();
                    if (cid is not null && _clusters.TryGetValue(cid, out var cluster))
                    {
                        cluster["VisibleToAllUsers"] = visible;
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Steps -----------------------------------------------------------------

    private static Dictionary<string, object?> MakeStep(Dictionary<string, object?> stepConfig)
    {
        var now = TimeHelpers.NowIso();
        var hadoopJarStep = stepConfig.TryGetValue("HadoopJarStep", out var hjs) ? hjs as Dictionary<string, object?> : null;
        var properties = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (hadoopJarStep is not null && hadoopJarStep.TryGetValue("Properties", out var propsObj) && propsObj is List<object?> propsList)
        {
            foreach (var p in propsList)
            {
                if (p is Dictionary<string, object?> pd && pd.TryGetValue("Key", out var k) && pd.TryGetValue("Value", out var v))
                {
                    properties[k?.ToString() ?? ""] = v;
                }
            }
        }

        return new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Id"] = GenerateStepId(),
            ["Name"] = stepConfig.TryGetValue("Name", out var n) ? n : "",
            ["Config"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Jar"] = hadoopJarStep?.TryGetValue("Jar", out var jar) == true ? jar : "",
                ["Properties"] = properties,
                ["MainClass"] = hadoopJarStep?.TryGetValue("MainClass", out var mc) == true ? mc : "",
                ["Args"] = hadoopJarStep?.TryGetValue("Args", out var args) == true ? args : new List<object?>(),
            },
            ["ActionOnFailure"] = stepConfig.TryGetValue("ActionOnFailure", out var aof) ? aof : "CONTINUE",
            ["Status"] = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["State"] = "COMPLETED",
                ["StateChangeReason"] = new Dictionary<string, object?>(StringComparer.Ordinal),
                ["Timeline"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["CreationDateTime"] = now,
                    ["StartDateTime"] = now,
                    ["EndDateTime"] = now,
                },
            },
        };
    }

    private ServiceResponse ActAddJobFlowSteps(JsonElement data)
    {
        var clusterId = GetString(data, "JobFlowId");
        lock (_lock)
        {
            if (clusterId is null || !_clusters.ContainsKey(clusterId))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Cluster id '{clusterId}' is not valid.", 400);
            }

            var stepIds = new List<string>();
            var stepsIn = GetArrayOfObjects(data, "Steps");
            if (!_steps.TryGetValue(clusterId, out var stepList))
            {
                stepList = [];
                _steps[clusterId] = stepList;
            }

            foreach (var stepConfig in stepsIn)
            {
                var step = MakeStep(stepConfig);
                stepList.Add(step);
                stepIds.Add(step["Id"]?.ToString() ?? "");
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["StepIds"] = stepIds });
        }
    }

    private ServiceResponse ActDescribeStep(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        var stepId = GetString(data, "StepId");
        lock (_lock)
        {
            if (clusterId is not null && _steps.TryGetValue(clusterId, out var stepList))
            {
                foreach (var step in stepList)
                {
                    if ((step["Id"] as string) == stepId)
                    {
                        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Step"] = step });
                    }
                }
            }

            return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                $"Step id '{stepId}' is not valid.", 400);
        }
    }

    private ServiceResponse ActListSteps(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        lock (_lock)
        {
            var stateFilter = new HashSet<string>(StringComparer.Ordinal);
            if (data.TryGetProperty("StepStates", out var statesEl) && statesEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var s in statesEl.EnumerateArray())
                {
                    var val = s.GetString();
                    if (val is not null)
                    {
                        stateFilter.Add(val);
                    }
                }
            }

            var steps = clusterId is not null && _steps.TryGetValue(clusterId, out var sl) ? sl : [];
            if (stateFilter.Count > 0)
            {
                steps = steps.Where(s =>
                {
                    var status = s["Status"] as Dictionary<string, object?>;
                    var state = status?["State"] as string ?? "";
                    return stateFilter.Contains(state);
                }).ToList();
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["Steps"] = steps });
        }
    }

    private ServiceResponse ActCancelSteps(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        lock (_lock)
        {
            var stepIds = new HashSet<string>(StringComparer.Ordinal);
            if (data.TryGetProperty("StepIds", out var idsEl) && idsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var id in idsEl.EnumerateArray())
                {
                    var val = id.GetString();
                    if (val is not null)
                    {
                        stepIds.Add(val);
                    }
                }
            }

            var cancelled = new List<Dictionary<string, object?>>();
            var steps = clusterId is not null && _steps.TryGetValue(clusterId, out var sl) ? sl : [];
            foreach (var step in steps)
            {
                var sid = step["Id"] as string ?? "";
                if (!stepIds.Contains(sid))
                {
                    continue;
                }

                var status = step["Status"] as Dictionary<string, object?>;
                var state = status?["State"] as string ?? "";
                if (state is "PENDING" or "RUNNING")
                {
                    if (status is not null)
                    {
                        status["State"] = "CANCELLED";
                    }

                    cancelled.Add(new Dictionary<string, object?>(StringComparer.Ordinal) { ["StepId"] = sid, ["Status"] = "SUBMITTED" });
                }
                else
                {
                    cancelled.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["StepId"] = sid,
                        ["Status"] = "FAILED_TO_CANCEL",
                        ["Reason"] = $"Step in state {state} cannot be cancelled",
                    });
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["CancelStepsInfoList"] = cancelled });
        }
    }

    // -- Instance Fleets -------------------------------------------------------

    private ServiceResponse ActAddInstanceFleet(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        lock (_lock)
        {
            if (clusterId is null || !_clusters.TryGetValue(clusterId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Cluster id '{clusterId}' is not valid.", 400);
            }

            var fleet = data.TryGetProperty("InstanceFleet", out var fleetEl) ? fleetEl : default;
            var now = TimeHelpers.NowIso();
            var fleetId = GenerateFleetId();
            var fleetType = fleet.ValueKind == JsonValueKind.Object ? (GetString(fleet, "InstanceFleetType") ?? "TASK") : "TASK";
            var onDemand = fleet.ValueKind == JsonValueKind.Object ? GetInt(fleet, "TargetOnDemandCapacity", 0) : 0;
            var spot = fleet.ValueKind == JsonValueKind.Object ? GetInt(fleet, "TargetSpotCapacity", 0) : 0;

            var record = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["Id"] = fleetId,
                ["Name"] = fleet.ValueKind == JsonValueKind.Object ? (GetString(fleet, "Name") ?? fleetType) : fleetType,
                ["Status"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["State"] = "RUNNING",
                    ["StateChangeReason"] = new Dictionary<string, object?>(StringComparer.Ordinal),
                    ["Timeline"] = new Dictionary<string, object?>(StringComparer.Ordinal) { ["CreationDateTime"] = now },
                },
                ["InstanceFleetType"] = fleetType,
                ["TargetOnDemandCapacity"] = onDemand,
                ["TargetSpotCapacity"] = spot,
                ["ProvisionedOnDemandCapacity"] = onDemand,
                ["ProvisionedSpotCapacity"] = spot,
                ["InstanceTypeSpecifications"] = fleet.ValueKind == JsonValueKind.Object
                    ? GetArrayOfObjects(fleet, "InstanceTypeConfigs")
                    : new List<Dictionary<string, object?>>(),
            };

            if (cluster["InstanceFleets"] is List<Dictionary<string, object?>> fleets)
            {
                fleets.Add(record);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["ClusterArn"] = cluster["ClusterArn"],
                ["InstanceFleetId"] = fleetId,
            });
        }
    }

    private ServiceResponse ActListInstanceFleets(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        lock (_lock)
        {
            if (clusterId is null || !_clusters.TryGetValue(clusterId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Cluster id '{clusterId}' is not valid.", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["InstanceFleets"] = cluster.TryGetValue("InstanceFleets", out var fleets) ? fleets : new List<Dictionary<string, object?>>(),
            });
        }
    }

    private ServiceResponse ActModifyInstanceFleet(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        lock (_lock)
        {
            if (clusterId is null || !_clusters.TryGetValue(clusterId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Cluster id '{clusterId}' is not valid.", 400);
            }

            var fleetMod = data.TryGetProperty("InstanceFleet", out var fleetEl) ? fleetEl : default;
            var fleetId = fleetMod.ValueKind == JsonValueKind.Object ? GetString(fleetMod, "InstanceFleetId") : null;

            if (cluster["InstanceFleets"] is List<Dictionary<string, object?>> fleets)
            {
                foreach (var fleet in fleets)
                {
                    if ((fleet["Id"] as string) == fleetId)
                    {
                        if (fleetMod.TryGetProperty("TargetOnDemandCapacity", out var tod) && tod.TryGetInt32(out var todVal))
                        {
                            fleet["TargetOnDemandCapacity"] = todVal;
                            fleet["ProvisionedOnDemandCapacity"] = todVal;
                        }

                        if (fleetMod.TryGetProperty("TargetSpotCapacity", out var tsc) && tsc.TryGetInt32(out var tscVal))
                        {
                            fleet["TargetSpotCapacity"] = tscVal;
                            fleet["ProvisionedSpotCapacity"] = tscVal;
                        }

                        break;
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Instance Groups -------------------------------------------------------

    private ServiceResponse ActAddInstanceGroups(JsonElement data)
    {
        var clusterId = GetString(data, "JobFlowId");
        lock (_lock)
        {
            if (clusterId is null || !_clusters.TryGetValue(clusterId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Cluster id '{clusterId}' is not valid.", 400);
            }

            var now = TimeHelpers.NowIso();
            var groupIds = new List<string>();

            foreach (var ig in GetArrayOfObjects(data, "InstanceGroups"))
            {
                var gid = GenerateGroupId();
                var role = ig.TryGetValue("InstanceRole", out var r) ? r?.ToString() ?? "TASK" : "TASK";
                var count = ig.TryGetValue("InstanceCount", out var c) && c is long lc ? (int)lc : 1;
                var record = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["Id"] = gid,
                    ["Name"] = ig.TryGetValue("Name", out var n) ? n : role,
                    ["Market"] = ig.TryGetValue("Market", out var m) ? m : "ON_DEMAND",
                    ["InstanceGroupType"] = role,
                    ["InstanceType"] = ig.TryGetValue("InstanceType", out var it) ? it : "m5.xlarge",
                    ["RequestedInstanceCount"] = count,
                    ["RunningInstanceCount"] = count,
                    ["Status"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["State"] = "RUNNING",
                        ["StateChangeReason"] = new Dictionary<string, object?>(StringComparer.Ordinal),
                        ["Timeline"] = new Dictionary<string, object?>(StringComparer.Ordinal) { ["CreationDateTime"] = now },
                    },
                };

                if (cluster["InstanceGroups"] is List<Dictionary<string, object?>> groups)
                {
                    groups.Add(record);
                }

                groupIds.Add(gid);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["JobFlowId"] = clusterId,
                ["InstanceGroupIds"] = groupIds,
            });
        }
    }

    private ServiceResponse ActListInstanceGroups(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        lock (_lock)
        {
            if (clusterId is null || !_clusters.TryGetValue(clusterId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Cluster id '{clusterId}' is not valid.", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["InstanceGroups"] = cluster.TryGetValue("InstanceGroups", out var groups) ? groups : new List<Dictionary<string, object?>>(),
            });
        }
    }

    private ServiceResponse ActModifyInstanceGroups(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        lock (_lock)
        {
            if (clusterId is null || !_clusters.TryGetValue(clusterId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Cluster id '{clusterId}' is not valid.", 400);
            }

            var mods = GetArrayOfObjects(data, "InstanceGroups");
            if (cluster["InstanceGroups"] is List<Dictionary<string, object?>> groups)
            {
                foreach (var mod in mods)
                {
                    var gid = mod.TryGetValue("InstanceGroupId", out var g) ? g?.ToString() : null;
                    foreach (var ig in groups)
                    {
                        if ((ig["Id"] as string) == gid && mod.TryGetValue("InstanceCount", out var ic))
                        {
                            var count = ic is long lc ? (int)lc : 1;
                            ig["RequestedInstanceCount"] = count;
                            ig["RunningInstanceCount"] = count;
                            break;
                        }
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Bootstrap Actions -----------------------------------------------------

    private ServiceResponse ActListBootstrapActions(JsonElement data)
    {
        var clusterId = GetString(data, "ClusterId");
        lock (_lock)
        {
            if (clusterId is null || !_clusters.TryGetValue(clusterId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Cluster id '{clusterId}' is not valid.", 400);
            }

            var actions = new List<Dictionary<string, object?>>();
            if (cluster.TryGetValue("BootstrapActions", out var bas) && bas is List<Dictionary<string, object?>> baList)
            {
                foreach (var ba in baList)
                {
                    var script = ba.TryGetValue("ScriptBootstrapAction", out var sba) ? sba as Dictionary<string, object?> : null;
                    actions.Add(new Dictionary<string, object?>(StringComparer.Ordinal)
                    {
                        ["Name"] = ba.TryGetValue("Name", out var n) ? n : "",
                        ["ScriptPath"] = script?.TryGetValue("Path", out var p) == true ? p : "",
                        ["Args"] = script?.TryGetValue("Args", out var a) == true ? a : new List<object?>(),
                    });
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?> { ["BootstrapActions"] = actions });
        }
    }

    // -- Tags ------------------------------------------------------------------

    private ServiceResponse ActAddTags(JsonElement data)
    {
        var resourceId = GetString(data, "ResourceId");
        lock (_lock)
        {
            if (resourceId is null || !_clusters.TryGetValue(resourceId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Resource id '{resourceId}' is not valid.", 400);
            }

            var newTags = GetArrayOfObjects(data, "Tags");
            if (cluster["Tags"] is List<Dictionary<string, object?>> existingTags)
            {
                var existingByKey = new Dictionary<string, int>(StringComparer.Ordinal);
                for (var i = 0; i < existingTags.Count; i++)
                {
                    if (existingTags[i].TryGetValue("Key", out var k) && k is string key)
                    {
                        existingByKey[key] = i;
                    }
                }

                foreach (var tag in newTags)
                {
                    var key = tag.TryGetValue("Key", out var k) ? k?.ToString() : null;
                    if (key is not null && existingByKey.TryGetValue(key, out var idx))
                    {
                        existingTags[idx] = tag;
                    }
                    else
                    {
                        existingTags.Add(tag);
                        if (key is not null)
                        {
                            existingByKey[key] = existingTags.Count - 1;
                        }
                    }
                }
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActRemoveTags(JsonElement data)
    {
        var resourceId = GetString(data, "ResourceId");
        lock (_lock)
        {
            if (resourceId is null || !_clusters.TryGetValue(resourceId, out var cluster))
            {
                return AwsResponseHelpers.ErrorResponseJson("InvalidRequestException",
                    $"Resource id '{resourceId}' is not valid.", 400);
            }

            var keys = new HashSet<string>(StringComparer.Ordinal);
            if (data.TryGetProperty("TagKeys", out var keysEl) && keysEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var k in keysEl.EnumerateArray())
                {
                    var val = k.GetString();
                    if (val is not null)
                    {
                        keys.Add(val);
                    }
                }
            }

            if (cluster["Tags"] is List<Dictionary<string, object?>> existingTags)
            {
                cluster["Tags"] = existingTags
                    .Where(t => !(t.TryGetValue("Key", out var k) && k is string key && keys.Contains(key)))
                    .ToList();
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Block Public Access ---------------------------------------------------

    private ServiceResponse ActGetBlockPublicAccessConfig(JsonElement data)
    {
        lock (_lock)
        {
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["BlockPublicAccessConfiguration"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["BlockPublicSecurityGroupRules"] = _blockPublicSecurityGroupRules,
                    ["PermittedPublicSecurityGroupRuleRanges"] = _permittedRanges,
                },
                ["BlockPublicAccessConfigurationMetadata"] = new Dictionary<string, object?>(StringComparer.Ordinal)
                {
                    ["CreationDateTime"] = TimeHelpers.NowIso(),
                    ["CreatedByArn"] = $"arn:aws:iam::{AccountContext.GetAccountId()}:root",
                },
            });
        }
    }

    private ServiceResponse ActPutBlockPublicAccessConfig(JsonElement data)
    {
        lock (_lock)
        {
            if (data.TryGetProperty("BlockPublicAccessConfiguration", out var config) && config.ValueKind == JsonValueKind.Object)
            {
                _blockPublicSecurityGroupRules = GetBool(config, "BlockPublicSecurityGroupRules", false);
                _permittedRanges = GetArrayOfObjects(config, "PermittedPublicSecurityGroupRuleRanges");
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }
}
