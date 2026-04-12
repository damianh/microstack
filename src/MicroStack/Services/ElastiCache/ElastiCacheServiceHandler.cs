using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using MicroStack.Internal;

namespace MicroStack.Services.ElastiCache;

/// <summary>
/// ElastiCache service handler -- Query/XML protocol (Action=...).
///
/// Port of ministack/services/elasticache.py.
///
/// Supports: CreateCacheCluster, DeleteCacheCluster, DescribeCacheClusters, ModifyCacheCluster,
///           RebootCacheCluster, CreateReplicationGroup, DeleteReplicationGroup,
///           DescribeReplicationGroups, ModifyReplicationGroup, IncreaseReplicaCount,
///           DecreaseReplicaCount, CreateCacheSubnetGroup, DescribeCacheSubnetGroups,
///           DeleteCacheSubnetGroup, ModifyCacheSubnetGroup,
///           CreateCacheParameterGroup, DescribeCacheParameterGroups, DeleteCacheParameterGroup,
///           DescribeCacheParameters, ModifyCacheParameterGroup, ResetCacheParameterGroup,
///           CreateUser, DescribeUsers, DeleteUser, ModifyUser,
///           CreateUserGroup, DescribeUserGroups, DeleteUserGroup, ModifyUserGroup,
///           DescribeCacheEngineVersions,
///           ListTagsForResource, AddTagsToResource, RemoveTagsFromResource,
///           CreateSnapshot, DeleteSnapshot, DescribeSnapshots,
///           DescribeEvents.
/// </summary>
internal sealed class ElastiCacheServiceHandler : IServiceHandler
{
    public string ServiceName => "elasticache";

    private const string ElastiCacheNs = "http://elasticache.amazonaws.com/doc/2015-02-02/";

    private static string Region =>
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";

    // ── State stores ──────────────────────────────────────────────────────────

    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _clusters = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _replicationGroups = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _subnetGroups = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _paramGroups = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, Dictionary<string, string>>> _paramGroupParams = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, string>>> _tags = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _snapshots = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _users = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _userGroups = new();
    private readonly List<Dictionary<string, object>> _events = [];

    private readonly Lock _lock = new();

    // ── IServiceHandler ───────────────────────────────────────────────────────

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
            _clusters.Clear();
            _replicationGroups.Clear();
            _subnetGroups.Clear();
            _paramGroups.Clear();
            _paramGroupParams.Clear();
            _tags.Clear();
            _snapshots.Clear();
            _users.Clear();
            _userGroups.Clear();
            _events.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // ── Action dispatch ───────────────────────────────────────────────────────

    private ServiceResponse DispatchAction(string action, Dictionary<string, string[]> p)
    {
        return action switch
        {
            // Cache Clusters
            "CreateCacheCluster" => CreateCacheCluster(p),
            "DeleteCacheCluster" => DeleteCacheCluster(p),
            "DescribeCacheClusters" => DescribeCacheClusters(p),
            "ModifyCacheCluster" => ModifyCacheCluster(p),
            "RebootCacheCluster" => RebootCacheCluster(p),
            // Replication Groups
            "CreateReplicationGroup" => CreateReplicationGroup(p),
            "DeleteReplicationGroup" => DeleteReplicationGroup(p),
            "DescribeReplicationGroups" => DescribeReplicationGroups(p),
            "ModifyReplicationGroup" => ModifyReplicationGroup(p),
            "IncreaseReplicaCount" => IncreaseReplicaCount(p),
            "DecreaseReplicaCount" => DecreaseReplicaCount(p),
            // Subnet Groups
            "CreateCacheSubnetGroup" => CreateCacheSubnetGroup(p),
            "DescribeCacheSubnetGroups" => DescribeCacheSubnetGroups(p),
            "DeleteCacheSubnetGroup" => DeleteCacheSubnetGroup(p),
            "ModifyCacheSubnetGroup" => ModifyCacheSubnetGroup(p),
            // Parameter Groups
            "CreateCacheParameterGroup" => CreateCacheParameterGroup(p),
            "DescribeCacheParameterGroups" => DescribeCacheParameterGroups(p),
            "DeleteCacheParameterGroup" => DeleteCacheParameterGroup(p),
            "DescribeCacheParameters" => DescribeCacheParameters(p),
            "ModifyCacheParameterGroup" => ModifyCacheParameterGroup(p),
            "ResetCacheParameterGroup" => ResetCacheParameterGroup(p),
            // Engine Versions
            "DescribeCacheEngineVersions" => DescribeCacheEngineVersions(p),
            // Users
            "CreateUser" => CreateUser(p),
            "DescribeUsers" => DescribeUsers(p),
            "DeleteUser" => DeleteUser(p),
            "ModifyUser" => ModifyUser(p),
            // User Groups
            "CreateUserGroup" => CreateUserGroup(p),
            "DescribeUserGroups" => DescribeUserGroups(p),
            "DeleteUserGroup" => DeleteUserGroup(p),
            "ModifyUserGroup" => ModifyUserGroup(p),
            // Tags
            "ListTagsForResource" => ListTagsForResource(p),
            "AddTagsToResource" => AddTagsToResource(p),
            "RemoveTagsFromResource" => RemoveTagsFromResource(p),
            // Snapshots
            "CreateSnapshot" => CreateSnapshot(p),
            "DeleteSnapshot" => DeleteSnapshot(p),
            "DescribeSnapshots" => DescribeSnapshots(p),
            // Events
            "DescribeEvents" => DescribeEvents(p),
            _ => Error("InvalidAction", $"Unknown ElastiCache action: {action}", 400),
        };
    }

    // ── ARN helpers ───────────────────────────────────────────────────────────

    private static string ArnCluster(string clusterId)
    {
        return $"arn:aws:elasticache:{Region}:{AccountContext.GetAccountId()}:cluster:{clusterId}";
    }

    private static string ArnReplicationGroup(string rgId)
    {
        return $"arn:aws:elasticache:{Region}:{AccountContext.GetAccountId()}:replicationgroup:{rgId}";
    }

    private static string ArnSubnetGroup(string name)
    {
        return $"arn:aws:elasticache:{Region}:{AccountContext.GetAccountId()}:subnetgroup:{name}";
    }

    private static string ArnParamGroup(string name)
    {
        return $"arn:aws:elasticache:{Region}:{AccountContext.GetAccountId()}:parametergroup:{name}";
    }

    private static string ArnSnapshot(string name)
    {
        return $"arn:aws:elasticache:{Region}:{AccountContext.GetAccountId()}:snapshot:{name}";
    }

    private static string ArnUser(string userId)
    {
        return $"arn:aws:elasticache:{Region}:{AccountContext.GetAccountId()}:user:{userId}";
    }

    private static string ArnUserGroup(string groupId)
    {
        return $"arn:aws:elasticache:{Region}:{AccountContext.GetAccountId()}:usergroup:{groupId}";
    }

    // ── Events ────────────────────────────────────────────────────────────────

    private void RecordEvent(string sourceId, string sourceType, string message)
    {
        _events.Add(new Dictionary<string, object>
        {
            ["SourceIdentifier"] = sourceId,
            ["SourceType"] = sourceType,
            ["Message"] = message,
            ["Date"] = TimeHelpers.NowEpoch(),
        });
        if (_events.Count > 500)
        {
            _events.RemoveRange(0, _events.Count - 500);
        }
    }

    // ── Cache Clusters ────────────────────────────────────────────────────────

    private ServiceResponse CreateCacheCluster(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "CacheClusterId");
        var engine = P(p, "Engine", "redis");
        var engineVersion = P(p, "EngineVersion");
        if (string.IsNullOrEmpty(engineVersion))
            engineVersion = engine == "redis" ? "7.0.12" : "1.6.17";
        var nodeType = P(p, "CacheNodeType", "cache.t3.micro");
        var numNodes = int.Parse(P(p, "NumCacheNodes", "1"));

        if (_clusters.ContainsKey(clusterId))
            return Error("CacheClusterAlreadyExists", $"Cluster {clusterId} already exists", 400);

        var arn = ArnCluster(clusterId);
        var endpointHost = "localhost";
        var endpointPort = engine == "redis" ? 6379 : 11211;

        var subnetGroup = P(p, "CacheSubnetGroupName", "default");
        var paramGroupName = P(p, "CacheParameterGroupName");
        if (string.IsNullOrEmpty(paramGroupName))
            paramGroupName = $"default.{engine}{engineVersion[..3]}";

        var now = TimeHelpers.NowIso();
        var nodes = new List<Dictionary<string, object?>>();
        for (var i = 1; i <= numNodes; i++)
        {
            nodes.Add(new Dictionary<string, object?>
            {
                ["CacheNodeId"] = $"{i:D4}",
                ["CacheNodeStatus"] = "available",
                ["CacheNodeCreateTime"] = now,
                ["Endpoint"] = new Dictionary<string, object?> { ["Address"] = endpointHost, ["Port"] = endpointPort },
                ["ParameterGroupStatus"] = "in-sync",
                ["SourceCacheNodeId"] = "",
            });
        }

        var cluster = new Dictionary<string, object?>
        {
            ["CacheClusterId"] = clusterId,
            ["CacheClusterArn"] = arn,
            ["CacheClusterStatus"] = "available",
            ["Engine"] = engine,
            ["EngineVersion"] = engineVersion,
            ["CacheNodeType"] = nodeType,
            ["NumCacheNodes"] = numNodes,
            ["CacheClusterCreateTime"] = now,
            ["PreferredAvailabilityZone"] = $"{Region}a",
            ["CacheParameterGroup"] = new Dictionary<string, object?>
            {
                ["CacheParameterGroupName"] = paramGroupName,
                ["ParameterApplyStatus"] = "in-sync",
            },
            ["CacheSubnetGroupName"] = subnetGroup,
            ["AutoMinorVersionUpgrade"] = true,
            ["SecurityGroups"] = new List<object>(),
            ["ReplicationGroupId"] = P(p, "ReplicationGroupId"),
            ["SnapshotRetentionLimit"] = int.Parse(P(p, "SnapshotRetentionLimit", "0")),
            ["SnapshotWindow"] = P(p, "SnapshotWindow", "05:00-06:00"),
            ["PreferredMaintenanceWindow"] = P(p, "PreferredMaintenanceWindow", "sun:05:00-sun:06:00"),
            ["CacheNodes"] = nodes,
            ["_endpoint"] = new Dictionary<string, object?> { ["Address"] = endpointHost, ["Port"] = endpointPort },
        };

        _clusters[clusterId] = cluster;

        var tags = ExtractTags(p);
        if (tags.Count > 0)
            _tags[arn] = tags;

        RecordEvent(clusterId, "cache-cluster", "Cache cluster created");
        return SingleClusterResponse("CreateCacheClusterResponse", "CreateCacheClusterResult", cluster);
    }

    private ServiceResponse DeleteCacheCluster(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "CacheClusterId");
        if (!_clusters.TryGetValue(clusterId, out var cluster))
            return Error("CacheClusterNotFound", $"Cluster {clusterId} not found", 404);

        cluster["CacheClusterStatus"] = "deleting";
        _clusters.TryRemove(clusterId, out _);
        _tags.TryRemove(cluster.GetValueOrDefault("CacheClusterArn")?.ToString() ?? "", out _);
        RecordEvent(clusterId, "cache-cluster", "Cache cluster deleted");
        return SingleClusterResponse("DeleteCacheClusterResponse", "DeleteCacheClusterResult", cluster);
    }

    private ServiceResponse DescribeCacheClusters(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "CacheClusterId");
        if (!string.IsNullOrEmpty(clusterId))
        {
            if (!_clusters.TryGetValue(clusterId, out var cluster))
                return Error("CacheClusterNotFound", $"Cluster {clusterId} not found", 404);
            var memberXml = ClusterXml(cluster);
            return Xml(200, "DescribeCacheClustersResponse",
                $"<DescribeCacheClustersResult><CacheClusters>{memberXml}</CacheClusters></DescribeCacheClustersResult>");
        }

        var sb = new StringBuilder();
        foreach (var c in _clusters.Values)
            sb.Append(ClusterXml(c));
        return Xml(200, "DescribeCacheClustersResponse",
            $"<DescribeCacheClustersResult><CacheClusters>{sb}</CacheClusters></DescribeCacheClustersResult>");
    }

    private ServiceResponse ModifyCacheCluster(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "CacheClusterId");
        if (!_clusters.TryGetValue(clusterId, out var cluster))
            return Error("CacheClusterNotFound", $"Cluster {clusterId} not found", 404);

        var numNodesStr = P(p, "NumCacheNodes");
        if (!string.IsNullOrEmpty(numNodesStr))
        {
            var newCount = int.Parse(numNodesStr);
            var oldCount = Convert.ToInt32(cluster["NumCacheNodes"]);
            cluster["NumCacheNodes"] = newCount;
            var nodes = (List<Dictionary<string, object?>>)cluster["CacheNodes"]!;
            var ep = (Dictionary<string, object?>?)cluster.GetValueOrDefault("_endpoint");
            if (newCount > oldCount)
            {
                for (var i = oldCount + 1; i <= newCount; i++)
                {
                    nodes.Add(new Dictionary<string, object?>
                    {
                        ["CacheNodeId"] = $"{i:D4}",
                        ["CacheNodeStatus"] = "available",
                        ["CacheNodeCreateTime"] = TimeHelpers.NowIso(),
                        ["Endpoint"] = new Dictionary<string, object?>
                        {
                            ["Address"] = ep?.GetValueOrDefault("Address")?.ToString() ?? "localhost",
                            ["Port"] = ep?.GetValueOrDefault("Port") ?? 6379,
                        },
                        ["ParameterGroupStatus"] = "in-sync",
                        ["SourceCacheNodeId"] = "",
                    });
                }
            }
            else if (newCount < oldCount)
            {
                while (nodes.Count > newCount)
                    nodes.RemoveAt(nodes.Count - 1);
            }
        }

        if (!string.IsNullOrEmpty(P(p, "CacheNodeType")))
            cluster["CacheNodeType"] = P(p, "CacheNodeType");
        if (!string.IsNullOrEmpty(P(p, "EngineVersion")))
            cluster["EngineVersion"] = P(p, "EngineVersion");
        if (!string.IsNullOrEmpty(P(p, "SnapshotRetentionLimit")))
            cluster["SnapshotRetentionLimit"] = int.Parse(P(p, "SnapshotRetentionLimit"));
        if (!string.IsNullOrEmpty(P(p, "SnapshotWindow")))
            cluster["SnapshotWindow"] = P(p, "SnapshotWindow");
        if (!string.IsNullOrEmpty(P(p, "PreferredMaintenanceWindow")))
            cluster["PreferredMaintenanceWindow"] = P(p, "PreferredMaintenanceWindow");
        if (!string.IsNullOrEmpty(P(p, "CacheParameterGroupName")))
        {
            var pg = (Dictionary<string, object?>)cluster["CacheParameterGroup"]!;
            pg["CacheParameterGroupName"] = P(p, "CacheParameterGroupName");
        }

        RecordEvent(clusterId, "cache-cluster", "Cache cluster modified");
        return SingleClusterResponse("ModifyCacheClusterResponse", "ModifyCacheClusterResult", cluster);
    }

    private ServiceResponse RebootCacheCluster(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "CacheClusterId");
        if (!_clusters.TryGetValue(clusterId, out var cluster))
            return Error("CacheClusterNotFound", $"Cluster {clusterId} not found", 404);
        RecordEvent(clusterId, "cache-cluster", "Cache cluster rebooted");
        return SingleClusterResponse("RebootCacheClusterResponse", "RebootCacheClusterResult", cluster);
    }

    // ── Replication Groups ────────────────────────────────────────────────────

    private ServiceResponse CreateReplicationGroup(Dictionary<string, string[]> p)
    {
        var rgId = P(p, "ReplicationGroupId");
        var desc = P(p, "ReplicationGroupDescription");
        var nodeType = P(p, "CacheNodeType", "cache.t3.micro");
        var numNodeGroups = int.Parse(P(p, "NumNodeGroups", "1"));
        var replicasPerNodeGroup = int.Parse(P(p, "ReplicasPerNodeGroup", "1"));
        var arn = ArnReplicationGroup(rgId);
        var endpointHost = "localhost";
        var endpointPort = 6379;

        if (_replicationGroups.ContainsKey(rgId))
            return Error("ReplicationGroupAlreadyExistsFault", $"Replication group {rgId} already exists", 400);

        var nodeGroups = new List<Dictionary<string, object?>>();
        for (var ngIdx = 1; ngIdx <= numNodeGroups; ngIdx++)
        {
            var ngId = $"{ngIdx:D4}";
            var members = new List<Dictionary<string, object?>>();
            for (var r = 0; r <= replicasPerNodeGroup; r++)
            {
                var role = r == 0 ? "primary" : "replica";
                members.Add(new Dictionary<string, object?>
                {
                    ["CacheClusterId"] = $"{rgId}-{ngId}-{r + 1:D3}",
                    ["CacheNodeId"] = "0001",
                    ["CurrentRole"] = role,
                    ["PreferredAvailabilityZone"] = $"{Region}{"abcdef"[r % 6]}",
                    ["ReadEndpoint"] = new Dictionary<string, object?> { ["Address"] = endpointHost, ["Port"] = endpointPort },
                });
            }
            nodeGroups.Add(new Dictionary<string, object?>
            {
                ["NodeGroupId"] = ngId,
                ["Status"] = "available",
                ["PrimaryEndpoint"] = new Dictionary<string, object?> { ["Address"] = endpointHost, ["Port"] = endpointPort },
                ["ReaderEndpoint"] = new Dictionary<string, object?> { ["Address"] = endpointHost, ["Port"] = endpointPort },
                ["NodeGroupMembers"] = members,
            });
        }

        var rg = new Dictionary<string, object?>
        {
            ["ReplicationGroupId"] = rgId,
            ["Description"] = desc,
            ["Status"] = "available",
            ["MemberClusters"] = new List<string>(),
            ["NodeGroups"] = nodeGroups,
            ["SnapshottingClusterId"] = "",
            ["SnapshotRetentionLimit"] = int.Parse(P(p, "SnapshotRetentionLimit", "0")),
            ["SnapshotWindow"] = P(p, "SnapshotWindow", "05:00-06:00"),
            ["ClusterEnabled"] = numNodeGroups > 1,
            ["CacheNodeType"] = nodeType,
            ["AuthTokenEnabled"] = !string.IsNullOrEmpty(P(p, "AuthToken")),
            ["TransitEncryptionEnabled"] = string.Equals(P(p, "TransitEncryptionEnabled"), "true", StringComparison.OrdinalIgnoreCase),
            ["AtRestEncryptionEnabled"] = string.Equals(P(p, "AtRestEncryptionEnabled"), "true", StringComparison.OrdinalIgnoreCase),
            ["AutomaticFailover"] = P(p, "AutomaticFailoverEnabled", "false"),
            ["MultiAZ"] = P(p, "MultiAZEnabled", "false"),
            ["ConfigurationEndpoint"] = numNodeGroups > 1 ? new Dictionary<string, object?> { ["Address"] = endpointHost, ["Port"] = endpointPort } : null,
            ["ARN"] = arn,
            ["_num_node_groups"] = numNodeGroups,
            ["_replicas_per_node_group"] = replicasPerNodeGroup,
        };

        _replicationGroups[rgId] = rg;

        var tags = ExtractTags(p);
        if (tags.Count > 0)
            _tags[arn] = tags;

        RecordEvent(rgId, "replication-group", "Replication group created");
        return Xml(200, "CreateReplicationGroupResponse",
            $"<CreateReplicationGroupResult><ReplicationGroup>{RgXml(rg)}</ReplicationGroup></CreateReplicationGroupResult>");
    }

    private ServiceResponse DeleteReplicationGroup(Dictionary<string, string[]> p)
    {
        var rgId = P(p, "ReplicationGroupId");
        if (!_replicationGroups.TryGetValue(rgId, out var rg))
            return Error("ReplicationGroupNotFoundFault", $"Replication group {rgId} not found", 404);

        _replicationGroups.TryRemove(rgId, out _);
        _tags.TryRemove(rg.GetValueOrDefault("ARN")?.ToString() ?? "", out _);
        RecordEvent(rgId, "replication-group", "Replication group deleted");
        return Xml(200, "DeleteReplicationGroupResponse",
            $"<DeleteReplicationGroupResult><ReplicationGroup>{RgXml(rg)}</ReplicationGroup></DeleteReplicationGroupResult>");
    }

    private ServiceResponse DescribeReplicationGroups(Dictionary<string, string[]> p)
    {
        var rgId = P(p, "ReplicationGroupId");
        if (!string.IsNullOrEmpty(rgId))
        {
            if (!_replicationGroups.TryGetValue(rgId, out var rg))
                return Error("ReplicationGroupNotFoundFault", $"Replication group {rgId} not found", 404);
            return Xml(200, "DescribeReplicationGroupsResponse",
                $"<DescribeReplicationGroupsResult><ReplicationGroups><ReplicationGroup>{RgXml(rg)}</ReplicationGroup></ReplicationGroups></DescribeReplicationGroupsResult>");
        }

        var sb = new StringBuilder();
        foreach (var g in _replicationGroups.Values)
            sb.Append($"<ReplicationGroup>{RgXml(g)}</ReplicationGroup>");
        return Xml(200, "DescribeReplicationGroupsResponse",
            $"<DescribeReplicationGroupsResult><ReplicationGroups>{sb}</ReplicationGroups></DescribeReplicationGroupsResult>");
    }

    private ServiceResponse ModifyReplicationGroup(Dictionary<string, string[]> p)
    {
        var rgId = P(p, "ReplicationGroupId");
        if (!_replicationGroups.TryGetValue(rgId, out var rg))
            return Error("ReplicationGroupNotFoundFault", $"Replication group {rgId} not found", 404);

        if (!string.IsNullOrEmpty(P(p, "ReplicationGroupDescription")))
            rg["Description"] = P(p, "ReplicationGroupDescription");
        if (!string.IsNullOrEmpty(P(p, "CacheNodeType")))
            rg["CacheNodeType"] = P(p, "CacheNodeType");
        if (!string.IsNullOrEmpty(P(p, "SnapshotRetentionLimit")))
            rg["SnapshotRetentionLimit"] = int.Parse(P(p, "SnapshotRetentionLimit"));
        if (!string.IsNullOrEmpty(P(p, "SnapshotWindow")))
            rg["SnapshotWindow"] = P(p, "SnapshotWindow");
        if (!string.IsNullOrEmpty(P(p, "AutomaticFailoverEnabled")))
            rg["AutomaticFailover"] = P(p, "AutomaticFailoverEnabled");
        if (!string.IsNullOrEmpty(P(p, "MultiAZEnabled")))
            rg["MultiAZ"] = P(p, "MultiAZEnabled");
        if (!string.IsNullOrEmpty(P(p, "EngineVersion")))
            rg["EngineVersion"] = P(p, "EngineVersion");
        if (!string.IsNullOrEmpty(P(p, "CacheParameterGroupName")))
            rg["CacheParameterGroupName"] = P(p, "CacheParameterGroupName");

        RecordEvent(rgId, "replication-group", "Replication group modified");
        return Xml(200, "ModifyReplicationGroupResponse",
            $"<ModifyReplicationGroupResult><ReplicationGroup>{RgXml(rg)}</ReplicationGroup></ModifyReplicationGroupResult>");
    }

    private ServiceResponse IncreaseReplicaCount(Dictionary<string, string[]> p)
    {
        var rgId = P(p, "ReplicationGroupId");
        if (!_replicationGroups.TryGetValue(rgId, out var rg))
            return Error("ReplicationGroupNotFoundFault", $"Replication group {rgId} not found", 404);

        var newCount = int.Parse(P(p, "NewReplicaCount", "0"));
        if (newCount <= 0)
            return Error("InvalidParameterValue", "NewReplicaCount must be positive", 400);

        var nodeGroups = (List<Dictionary<string, object?>>)rg["NodeGroups"]!;
        foreach (var ng in nodeGroups)
        {
            var members = (List<Dictionary<string, object?>>)ng["NodeGroupMembers"]!;
            var target = newCount + 1; // +1 for primary
            while (members.Count < target)
            {
                var idx = members.Count + 1;
                members.Add(new Dictionary<string, object?>
                {
                    ["CacheClusterId"] = $"{rgId}-{ng["NodeGroupId"]}-{idx:D3}",
                    ["CacheNodeId"] = "0001",
                    ["CurrentRole"] = "replica",
                    ["PreferredAvailabilityZone"] = $"{Region}a",
                    ["ReadEndpoint"] = new Dictionary<string, object?> { ["Address"] = "localhost", ["Port"] = 6379 },
                });
            }
        }
        rg["_replicas_per_node_group"] = newCount;

        RecordEvent(rgId, "replication-group", "Replica count increased");
        return Xml(200, "IncreaseReplicaCountResponse",
            $"<IncreaseReplicaCountResult><ReplicationGroup>{RgXml(rg)}</ReplicationGroup></IncreaseReplicaCountResult>");
    }

    private ServiceResponse DecreaseReplicaCount(Dictionary<string, string[]> p)
    {
        var rgId = P(p, "ReplicationGroupId");
        if (!_replicationGroups.TryGetValue(rgId, out var rg))
            return Error("ReplicationGroupNotFoundFault", $"Replication group {rgId} not found", 404);

        var newCount = int.Parse(P(p, "NewReplicaCount", "0"));
        if (newCount < 0)
            return Error("InvalidParameterValue", "NewReplicaCount must be non-negative", 400);

        var nodeGroups = (List<Dictionary<string, object?>>)rg["NodeGroups"]!;
        foreach (var ng in nodeGroups)
        {
            var members = (List<Dictionary<string, object?>>)ng["NodeGroupMembers"]!;
            var target = newCount + 1;
            while (members.Count > target)
                members.RemoveAt(members.Count - 1);
        }
        rg["_replicas_per_node_group"] = newCount;

        RecordEvent(rgId, "replication-group", "Replica count decreased");
        return Xml(200, "DecreaseReplicaCountResponse",
            $"<DecreaseReplicaCountResult><ReplicationGroup>{RgXml(rg)}</ReplicationGroup></DecreaseReplicaCountResult>");
    }

    // ── Subnet Groups ─────────────────────────────────────────────────────────

    private ServiceResponse CreateCacheSubnetGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "CacheSubnetGroupName");
        var desc = P(p, "CacheSubnetGroupDescription");
        var arn = ArnSubnetGroup(name);

        var subnets = new List<Dictionary<string, object?>>();
        for (var idx = 1; ; idx++)
        {
            var subnetId = P(p, $"SubnetIds.member.{idx}");
            if (string.IsNullOrEmpty(subnetId))
            {
                // Fallback botocore serializer format
                subnetId = P(p, $"SubnetIds.SubnetIdentifier.{idx}");
                if (string.IsNullOrEmpty(subnetId)) break;
            }
            subnets.Add(new Dictionary<string, object?>
            {
                ["SubnetIdentifier"] = subnetId,
                ["SubnetAvailabilityZone"] = new Dictionary<string, object?> { ["Name"] = $"{Region}{"abcdef"[(idx - 1) % 6]}" },
            });
        }

        _subnetGroups[name] = new Dictionary<string, object?>
        {
            ["CacheSubnetGroupName"] = name,
            ["CacheSubnetGroupDescription"] = desc,
            ["VpcId"] = "vpc-00000000",
            ["Subnets"] = subnets,
            ["ARN"] = arn,
        };

        return Xml(200, "CreateCacheSubnetGroupResponse",
            $"<CreateCacheSubnetGroupResult><CacheSubnetGroup>"
            + $"<CacheSubnetGroupName>{Esc(name)}</CacheSubnetGroupName>"
            + $"<CacheSubnetGroupDescription>{Esc(desc)}</CacheSubnetGroupDescription>"
            + $"<ARN>{Esc(arn)}</ARN>"
            + "</CacheSubnetGroup></CreateCacheSubnetGroupResult>");
    }

    private ServiceResponse DescribeCacheSubnetGroups(Dictionary<string, string[]> p)
    {
        var name = P(p, "CacheSubnetGroupName");
        IEnumerable<Dictionary<string, object?>> groups;
        if (!string.IsNullOrEmpty(name) && _subnetGroups.ContainsKey(name))
            groups = [_subnetGroups[name]];
        else
            groups = _subnetGroups.Values;

        var sb = new StringBuilder();
        foreach (var g in groups)
        {
            sb.Append("<CacheSubnetGroup>");
            sb.Append($"<CacheSubnetGroupName>{Esc(g.GetValueOrDefault("CacheSubnetGroupName")?.ToString() ?? "")}</CacheSubnetGroupName>");
            sb.Append($"<CacheSubnetGroupDescription>{Esc(g.GetValueOrDefault("CacheSubnetGroupDescription")?.ToString() ?? "")}</CacheSubnetGroupDescription>");
            sb.Append($"<ARN>{Esc(g.GetValueOrDefault("ARN")?.ToString() ?? "")}</ARN>");
            sb.Append("</CacheSubnetGroup>");
        }
        return Xml(200, "DescribeCacheSubnetGroupsResponse",
            $"<DescribeCacheSubnetGroupsResult><CacheSubnetGroups>{sb}</CacheSubnetGroups></DescribeCacheSubnetGroupsResult>");
    }

    private ServiceResponse DeleteCacheSubnetGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "CacheSubnetGroupName");
        if (_subnetGroups.TryGetValue(name, out var sg))
        {
            _tags.TryRemove(sg.GetValueOrDefault("ARN")?.ToString() ?? "", out _);
            _subnetGroups.TryRemove(name, out _);
        }
        return Xml(200, "DeleteCacheSubnetGroupResponse", "");
    }

    private ServiceResponse ModifyCacheSubnetGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "CacheSubnetGroupName");
        if (!_subnetGroups.TryGetValue(name, out var sg))
            return Error("CacheSubnetGroupNotFoundFault", $"Subnet group {name} not found", 404);

        if (!string.IsNullOrEmpty(P(p, "CacheSubnetGroupDescription")))
            sg["CacheSubnetGroupDescription"] = P(p, "CacheSubnetGroupDescription");

        var subnets = new List<Dictionary<string, object?>>();
        for (var idx = 1; ; idx++)
        {
            var subnetId = P(p, $"SubnetIds.member.{idx}");
            if (string.IsNullOrEmpty(subnetId))
            {
                subnetId = P(p, $"SubnetIds.SubnetIdentifier.{idx}");
                if (string.IsNullOrEmpty(subnetId)) break;
            }
            subnets.Add(new Dictionary<string, object?>
            {
                ["SubnetIdentifier"] = subnetId,
                ["SubnetAvailabilityZone"] = new Dictionary<string, object?> { ["Name"] = $"{Region}{"abcdef"[(idx - 1) % 6]}" },
            });
        }
        if (subnets.Count > 0)
            sg["Subnets"] = subnets;

        var arn = sg.GetValueOrDefault("ARN")?.ToString() ?? ArnSubnetGroup(name);
        return Xml(200, "ModifyCacheSubnetGroupResponse",
            $"<ModifyCacheSubnetGroupResult><CacheSubnetGroup>"
            + $"<CacheSubnetGroupName>{Esc(name)}</CacheSubnetGroupName>"
            + $"<CacheSubnetGroupDescription>{Esc(sg.GetValueOrDefault("CacheSubnetGroupDescription")?.ToString() ?? "")}</CacheSubnetGroupDescription>"
            + $"<ARN>{Esc(arn)}</ARN>"
            + "</CacheSubnetGroup></ModifyCacheSubnetGroupResult>");
    }

    // ── Parameter Groups ──────────────────────────────────────────────────────

    private ServiceResponse CreateCacheParameterGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "CacheParameterGroupName");
        var family = P(p, "CacheParameterGroupFamily", "redis7.0");
        var desc = P(p, "Description");
        var arn = ArnParamGroup(name);

        _paramGroups[name] = new Dictionary<string, object?>
        {
            ["CacheParameterGroupName"] = name,
            ["CacheParameterGroupFamily"] = family,
            ["Description"] = desc,
            ["IsGlobal"] = false,
            ["ARN"] = arn,
        };
        _paramGroupParams[name] = DefaultParamsForFamily(family);

        return Xml(200, "CreateCacheParameterGroupResponse",
            $"<CreateCacheParameterGroupResult><CacheParameterGroup>"
            + $"<CacheParameterGroupName>{Esc(name)}</CacheParameterGroupName>"
            + $"<CacheParameterGroupFamily>{Esc(family)}</CacheParameterGroupFamily>"
            + $"<Description>{Esc(desc)}</Description>"
            + $"<ARN>{Esc(arn)}</ARN>"
            + "</CacheParameterGroup></CreateCacheParameterGroupResult>");
    }

    private ServiceResponse DescribeCacheParameterGroups(Dictionary<string, string[]> p)
    {
        var name = P(p, "CacheParameterGroupName");
        IEnumerable<Dictionary<string, object?>> groups;
        if (!string.IsNullOrEmpty(name) && _paramGroups.ContainsKey(name))
            groups = [_paramGroups[name]];
        else
            groups = _paramGroups.Values;

        var sb = new StringBuilder();
        foreach (var g in groups)
        {
            sb.Append("<CacheParameterGroup>");
            sb.Append($"<CacheParameterGroupName>{Esc(g.GetValueOrDefault("CacheParameterGroupName")?.ToString() ?? "")}</CacheParameterGroupName>");
            sb.Append($"<CacheParameterGroupFamily>{Esc(g.GetValueOrDefault("CacheParameterGroupFamily")?.ToString() ?? "")}</CacheParameterGroupFamily>");
            sb.Append($"<Description>{Esc(g.GetValueOrDefault("Description")?.ToString() ?? "")}</Description>");
            sb.Append($"<ARN>{Esc(g.GetValueOrDefault("ARN")?.ToString() ?? "")}</ARN>");
            sb.Append("</CacheParameterGroup>");
        }
        return Xml(200, "DescribeCacheParameterGroupsResponse",
            $"<DescribeCacheParameterGroupsResult><CacheParameterGroups>{sb}</CacheParameterGroups></DescribeCacheParameterGroupsResult>");
    }

    private ServiceResponse DeleteCacheParameterGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "CacheParameterGroupName");
        if (_paramGroups.TryGetValue(name, out var pg))
        {
            _tags.TryRemove(pg.GetValueOrDefault("ARN")?.ToString() ?? "", out _);
            _paramGroups.TryRemove(name, out _);
        }
        _paramGroupParams.TryRemove(name, out _);
        return Xml(200, "DeleteCacheParameterGroupResponse", "");
    }

    private ServiceResponse DescribeCacheParameters(Dictionary<string, string[]> p)
    {
        var name = P(p, "CacheParameterGroupName");
        if (!_paramGroups.ContainsKey(name))
            return Error("CacheParameterGroupNotFound", $"Parameter group {name} not found", 404);

        _paramGroupParams.TryGetValue(name, out var parms);
        parms ??= new Dictionary<string, Dictionary<string, string>>();

        var sb = new StringBuilder();
        foreach (var (pname, pval) in parms)
        {
            sb.Append("<Parameter>");
            sb.Append($"<ParameterName>{Esc(pname)}</ParameterName>");
            sb.Append($"<ParameterValue>{Esc(pval.GetValueOrDefault("Value") ?? "")}</ParameterValue>");
            sb.Append($"<Description>{Esc(pval.GetValueOrDefault("Description") ?? "")}</Description>");
            sb.Append($"<Source>{Esc(pval.GetValueOrDefault("Source") ?? "system")}</Source>");
            sb.Append($"<DataType>{Esc(pval.GetValueOrDefault("DataType") ?? "string")}</DataType>");
            sb.Append($"<AllowedValues>{Esc(pval.GetValueOrDefault("AllowedValues") ?? "")}</AllowedValues>");
            sb.Append($"<IsModifiable>{(pval.GetValueOrDefault("IsModifiable") ?? "true").ToLowerInvariant()}</IsModifiable>");
            sb.Append($"<MinimumEngineVersion>{Esc(pval.GetValueOrDefault("MinimumEngineVersion") ?? "5.0.0")}</MinimumEngineVersion>");
            sb.Append("</Parameter>");
        }
        return Xml(200, "DescribeCacheParametersResponse",
            $"<DescribeCacheParametersResult><Parameters>{sb}</Parameters></DescribeCacheParametersResult>");
    }

    private ServiceResponse ModifyCacheParameterGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "CacheParameterGroupName");
        if (!_paramGroups.ContainsKey(name))
            return Error("CacheParameterGroupNotFound", $"Parameter group {name} not found", 404);

        if (!_paramGroupParams.ContainsKey(name))
            _paramGroupParams[name] = new Dictionary<string, Dictionary<string, string>>();
        var parms = _paramGroupParams[name];

        for (var idx = 1; ; idx++)
        {
            var pname = P(p, $"ParameterNameValues.ParameterNameValue.{idx}.ParameterName");
            if (string.IsNullOrEmpty(pname))
            {
                // Fallback botocore format
                pname = P(p, $"ParameterNameValues.member.{idx}.ParameterName");
                if (string.IsNullOrEmpty(pname)) break;
            }
            var pvalue = P(p, $"ParameterNameValues.ParameterNameValue.{idx}.ParameterValue");
            if (string.IsNullOrEmpty(pvalue))
                pvalue = P(p, $"ParameterNameValues.member.{idx}.ParameterValue");

            if (parms.TryGetValue(pname, out var existing))
            {
                existing["Value"] = pvalue;
                existing["Source"] = "user";
            }
            else
            {
                parms[pname] = new Dictionary<string, string>
                {
                    ["Value"] = pvalue,
                    ["Source"] = "user",
                    ["DataType"] = "string",
                    ["Description"] = "",
                    ["IsModifiable"] = "true",
                };
            }
        }

        return Xml(200, "ModifyCacheParameterGroupResponse",
            $"<ModifyCacheParameterGroupResult><CacheParameterGroupName>{Esc(name)}</CacheParameterGroupName></ModifyCacheParameterGroupResult>");
    }

    private ServiceResponse ResetCacheParameterGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "CacheParameterGroupName");
        if (!_paramGroups.ContainsKey(name))
            return Error("CacheParameterGroupNotFound", $"Parameter group {name} not found", 404);

        var resetAll = string.Equals(P(p, "ResetAllParameters"), "true", StringComparison.OrdinalIgnoreCase);
        var family = _paramGroups[name].GetValueOrDefault("CacheParameterGroupFamily")?.ToString() ?? "redis7.0";

        if (resetAll)
        {
            _paramGroupParams[name] = DefaultParamsForFamily(family);
        }
        else
        {
            var defaults = DefaultParamsForFamily(family);
            if (!_paramGroupParams.ContainsKey(name))
                _paramGroupParams[name] = new Dictionary<string, Dictionary<string, string>>();
            var parms = _paramGroupParams[name];

            for (var idx = 1; ; idx++)
            {
                var pname = P(p, $"ParameterNameValues.ParameterNameValue.{idx}.ParameterName");
                if (string.IsNullOrEmpty(pname))
                {
                    pname = P(p, $"ParameterNameValues.member.{idx}.ParameterName");
                    if (string.IsNullOrEmpty(pname)) break;
                }
                if (defaults.TryGetValue(pname, out var defVal))
                    parms[pname] = new Dictionary<string, string>(defVal);
            }
        }

        return Xml(200, "ResetCacheParameterGroupResponse",
            $"<ResetCacheParameterGroupResult><CacheParameterGroupName>{Esc(name)}</CacheParameterGroupName></ResetCacheParameterGroupResult>");
    }

    // ── Engine Versions ───────────────────────────────────────────────────────

    private static ServiceResponse DescribeCacheEngineVersions(Dictionary<string, string[]> p)
    {
        var engine = P(p, "Engine", "redis");
        string[] versions;
        if (engine == "redis")
            versions = ["7.1.0", "7.0.12", "6.2.14", "5.0.6"];
        else if (engine == "memcached")
            versions = ["1.6.22", "1.6.17", "1.6.12"];
        else
            versions = ["7.0.12"];

        var sb = new StringBuilder();
        foreach (var v in versions)
        {
            sb.Append($"<CacheEngineVersion><Engine>{Esc(engine)}</Engine><EngineVersion>{v}</EngineVersion>");
            sb.Append($"<CacheParameterGroupFamily>{Esc(engine)}{v[..3]}</CacheParameterGroupFamily></CacheEngineVersion>");
        }
        return Xml(200, "DescribeCacheEngineVersionsResponse",
            $"<DescribeCacheEngineVersionsResult><CacheEngineVersions>{sb}</CacheEngineVersions></DescribeCacheEngineVersionsResult>");
    }

    // ── Users ─────────────────────────────────────────────────────────────────

    private ServiceResponse CreateUser(Dictionary<string, string[]> p)
    {
        var userId = P(p, "UserId");
        if (string.IsNullOrEmpty(userId))
            return Error("InvalidParameterValue", "UserId is required", 400);
        if (_users.ContainsKey(userId))
            return Error("UserAlreadyExistsFault", $"User {userId} already exists", 400);

        var arn = ArnUser(userId);
        var hasPassword = !string.IsNullOrEmpty(P(p, "Passwords.member.1"));
        var user = new Dictionary<string, object?>
        {
            ["UserId"] = userId,
            ["UserName"] = P(p, "UserName", userId),
            ["Engine"] = P(p, "Engine", "redis"),
            ["Status"] = "active",
            ["AccessString"] = P(p, "AccessString", "on ~* +@all"),
            ["UserGroupIds"] = new List<string>(),
            ["Authentication"] = hasPassword
                ? new Dictionary<string, object?> { ["Type"] = "password", ["PasswordCount"] = 1 }
                : new Dictionary<string, object?> { ["Type"] = "no-password", ["PasswordCount"] = 0 },
            ["ARN"] = arn,
        };
        _users[userId] = user;

        var tags = ExtractTags(p);
        if (tags.Count > 0)
            _tags[arn] = tags;

        return Xml(200, "CreateUserResponse", $"<CreateUserResult>{UserXml(user)}</CreateUserResult>");
    }

    private ServiceResponse DescribeUsers(Dictionary<string, string[]> p)
    {
        var userId = P(p, "UserId");
        var engine = P(p, "Engine");

        List<Dictionary<string, object?>> users;
        if (!string.IsNullOrEmpty(userId))
        {
            if (!_users.TryGetValue(userId, out var user))
                return Error("UserNotFound", $"User {userId} not found", 404);
            users = [user];
        }
        else
        {
            users = [.. _users.Values];
            if (!string.IsNullOrEmpty(engine))
                users = users.Where(u => u.GetValueOrDefault("Engine")?.ToString() == engine).ToList();
        }

        var sb = new StringBuilder();
        foreach (var u in users)
            sb.Append($"<member>{UserXml(u)}</member>");
        return Xml(200, "DescribeUsersResponse",
            $"<DescribeUsersResult><Users>{sb}</Users></DescribeUsersResult>");
    }

    private ServiceResponse DeleteUser(Dictionary<string, string[]> p)
    {
        var userId = P(p, "UserId");
        if (!_users.TryGetValue(userId, out var user))
            return Error("UserNotFound", $"User {userId} not found", 404);

        _users.TryRemove(userId, out _);
        _tags.TryRemove(user.GetValueOrDefault("ARN")?.ToString() ?? "", out _);
        user["Status"] = "deleting";
        return Xml(200, "DeleteUserResponse", $"<DeleteUserResult>{UserXml(user)}</DeleteUserResult>");
    }

    private ServiceResponse ModifyUser(Dictionary<string, string[]> p)
    {
        var userId = P(p, "UserId");
        if (!_users.TryGetValue(userId, out var user))
            return Error("UserNotFound", $"User {userId} not found", 404);

        if (!string.IsNullOrEmpty(P(p, "AccessString")))
            user["AccessString"] = P(p, "AccessString");
        if (!string.IsNullOrEmpty(P(p, "Passwords.member.1")))
            user["Authentication"] = new Dictionary<string, object?> { ["Type"] = "password", ["PasswordCount"] = 1 };

        return Xml(200, "ModifyUserResponse", $"<ModifyUserResult>{UserXml(user)}</ModifyUserResult>");
    }

    // ── User Groups ───────────────────────────────────────────────────────────

    private ServiceResponse CreateUserGroup(Dictionary<string, string[]> p)
    {
        var groupId = P(p, "UserGroupId");
        if (string.IsNullOrEmpty(groupId))
            return Error("InvalidParameterValue", "UserGroupId is required", 400);
        if (_userGroups.ContainsKey(groupId))
            return Error("UserGroupAlreadyExistsFault", $"User group {groupId} already exists", 400);

        var arn = ArnUserGroup(groupId);
        var userIds = ParseMemberList(p, "UserIds");

        var group = new Dictionary<string, object?>
        {
            ["UserGroupId"] = groupId,
            ["Status"] = "active",
            ["Engine"] = P(p, "Engine", "redis"),
            ["UserIds"] = userIds,
            ["PendingChanges"] = new Dictionary<string, object?>(),
            ["ReplicationGroups"] = new List<string>(),
            ["ARN"] = arn,
        };
        _userGroups[groupId] = group;

        foreach (var uid in userIds)
        {
            if (_users.TryGetValue(uid, out var user))
            {
                var gids = (List<string>)user["UserGroupIds"]!;
                gids.Add(groupId);
            }
        }

        var tags = ExtractTags(p);
        if (tags.Count > 0)
            _tags[arn] = tags;

        return Xml(200, "CreateUserGroupResponse", $"<CreateUserGroupResult>{UserGroupXml(group)}</CreateUserGroupResult>");
    }

    private ServiceResponse DescribeUserGroups(Dictionary<string, string[]> p)
    {
        var groupId = P(p, "UserGroupId");
        List<Dictionary<string, object?>> groups;
        if (!string.IsNullOrEmpty(groupId))
        {
            if (!_userGroups.TryGetValue(groupId, out var group))
                return Error("UserGroupNotFound", $"User group {groupId} not found", 404);
            groups = [group];
        }
        else
        {
            groups = [.. _userGroups.Values];
        }

        var sb = new StringBuilder();
        foreach (var g in groups)
            sb.Append($"<member>{UserGroupXml(g)}</member>");
        return Xml(200, "DescribeUserGroupsResponse",
            $"<DescribeUserGroupsResult><UserGroups>{sb}</UserGroups></DescribeUserGroupsResult>");
    }

    private ServiceResponse DeleteUserGroup(Dictionary<string, string[]> p)
    {
        var groupId = P(p, "UserGroupId");
        if (!_userGroups.TryGetValue(groupId, out var group))
            return Error("UserGroupNotFound", $"User group {groupId} not found", 404);

        _userGroups.TryRemove(groupId, out _);
        _tags.TryRemove(group.GetValueOrDefault("ARN")?.ToString() ?? "", out _);

        var userIds = (List<string>)group["UserIds"]!;
        foreach (var uid in userIds)
        {
            if (_users.TryGetValue(uid, out var user))
            {
                var gids = (List<string>)user["UserGroupIds"]!;
                gids.Remove(groupId);
            }
        }

        group["Status"] = "deleting";
        return Xml(200, "DeleteUserGroupResponse", $"<DeleteUserGroupResult>{UserGroupXml(group)}</DeleteUserGroupResult>");
    }

    private ServiceResponse ModifyUserGroup(Dictionary<string, string[]> p)
    {
        var groupId = P(p, "UserGroupId");
        if (!_userGroups.TryGetValue(groupId, out var group))
            return Error("UserGroupNotFound", $"User group {groupId} not found", 404);

        var toAdd = ParseMemberList(p, "UserIdsToAdd");
        var toRemove = ParseMemberList(p, "UserIdsToRemove");

        var userIds = (List<string>)group["UserIds"]!;

        foreach (var uid in toAdd)
        {
            if (!userIds.Contains(uid))
                userIds.Add(uid);
            if (_users.TryGetValue(uid, out var user))
            {
                var gids = (List<string>)user["UserGroupIds"]!;
                gids.Add(groupId);
            }
        }

        foreach (var uid in toRemove)
        {
            userIds.Remove(uid);
            if (_users.TryGetValue(uid, out var user))
            {
                var gids = (List<string>)user["UserGroupIds"]!;
                gids.Remove(groupId);
            }
        }

        return Xml(200, "ModifyUserGroupResponse", $"<ModifyUserGroupResult>{UserGroupXml(group)}</ModifyUserGroupResult>");
    }

    // ── Tags ──────────────────────────────────────────────────────────────────

    private ServiceResponse ListTagsForResource(Dictionary<string, string[]> p)
    {
        var arn = P(p, "ResourceName");
        _tags.TryGetValue(arn, out var tags);
        tags ??= [];
        var sb = new StringBuilder();
        foreach (var t in tags)
            sb.Append($"<Tag><Key>{Esc(t["Key"])}</Key><Value>{Esc(t["Value"])}</Value></Tag>");
        return Xml(200, "ListTagsForResourceResponse",
            $"<ListTagsForResourceResult><TagList>{sb}</TagList></ListTagsForResourceResult>");
    }

    private ServiceResponse AddTagsToResource(Dictionary<string, string[]> p)
    {
        var arn = P(p, "ResourceName");
        var newTags = ExtractTags(p);
        if (!_tags.TryGetValue(arn, out var existing))
        {
            existing = [];
            _tags[arn] = existing;
        }
        var existingKeys = new HashSet<string>(existing.Select(t => t["Key"]));
        foreach (var t in newTags)
        {
            if (existingKeys.Contains(t["Key"]))
            {
                var found = existing.First(e => e["Key"] == t["Key"]);
                found["Value"] = t["Value"];
            }
            else
            {
                existing.Add(t);
                existingKeys.Add(t["Key"]);
            }
        }

        var sb = new StringBuilder();
        foreach (var t in existing)
            sb.Append($"<Tag><Key>{Esc(t["Key"])}</Key><Value>{Esc(t["Value"])}</Value></Tag>");
        return Xml(200, "AddTagsToResourceResponse",
            $"<AddTagsToResourceResult><TagList>{sb}</TagList></AddTagsToResourceResult>");
    }

    private ServiceResponse RemoveTagsFromResource(Dictionary<string, string[]> p)
    {
        var arn = P(p, "ResourceName");
        var keysToRemove = new HashSet<string>();
        for (var idx = 1; ; idx++)
        {
            var key = P(p, $"TagKeys.member.{idx}");
            if (string.IsNullOrEmpty(key)) break;
            keysToRemove.Add(key);
        }

        if (_tags.TryGetValue(arn, out var tags))
            _tags[arn] = tags.Where(t => !keysToRemove.Contains(t["Key"])).ToList();

        _tags.TryGetValue(arn, out var remaining);
        remaining ??= [];
        var sb = new StringBuilder();
        foreach (var t in remaining)
            sb.Append($"<Tag><Key>{Esc(t["Key"])}</Key><Value>{Esc(t["Value"])}</Value></Tag>");
        return Xml(200, "RemoveTagsFromResourceResponse",
            $"<RemoveTagsFromResourceResult><TagList>{sb}</TagList></RemoveTagsFromResourceResult>");
    }

    // ── Snapshots ─────────────────────────────────────────────────────────────

    private ServiceResponse CreateSnapshot(Dictionary<string, string[]> p)
    {
        var snapshotName = P(p, "SnapshotName");
        var clusterId = P(p, "CacheClusterId");
        var rgId = P(p, "ReplicationGroupId");

        if (_snapshots.ContainsKey(snapshotName))
            return Error("SnapshotAlreadyExistsFault", $"Snapshot {snapshotName} already exists", 400);

        var arn = ArnSnapshot(snapshotName);
        var now = TimeHelpers.NowEpoch();
        var snap = new Dictionary<string, object?>
        {
            ["SnapshotName"] = snapshotName,
            ["SnapshotStatus"] = "available",
            ["SnapshotSource"] = "manual",
            ["CacheClusterId"] = clusterId,
            ["ReplicationGroupId"] = rgId,
            ["CacheNodeType"] = "cache.t3.micro",
            ["Engine"] = "redis",
            ["EngineVersion"] = "7.0.12",
            ["SnapshotRetentionLimit"] = 0,
            ["SnapshotWindow"] = "05:00-06:00",
            ["NodeSnapshots"] = new List<Dictionary<string, object?>>
            {
                new()
                {
                    ["CacheNodeId"] = "0001",
                    ["SnapshotCreateTime"] = now,
                    ["CacheSize"] = "0 MB",
                },
            },
            ["ARN"] = arn,
            ["CreateTime"] = now,
        };

        // Copy info from source cluster/rg
        var sourceId = !string.IsNullOrEmpty(clusterId) ? clusterId : rgId;
        if (!string.IsNullOrEmpty(sourceId))
        {
            _clusters.TryGetValue(sourceId, out var clSrc);
            _replicationGroups.TryGetValue(sourceId, out var rgSrc);
            var src = clSrc ?? rgSrc;
            if (src is not null)
            {
                snap["CacheNodeType"] = src.GetValueOrDefault("CacheNodeType") ?? "cache.t3.micro";
                snap["Engine"] = src.GetValueOrDefault("Engine") ?? "redis";
                snap["EngineVersion"] = src.GetValueOrDefault("EngineVersion") ?? "7.0.12";
            }
        }

        _snapshots[snapshotName] = snap;
        RecordEvent(snapshotName, "snapshot", "Snapshot created");
        return Xml(200, "CreateSnapshotResponse",
            $"<CreateSnapshotResult><Snapshot>{SnapshotXml(snap)}</Snapshot></CreateSnapshotResult>");
    }

    private ServiceResponse DeleteSnapshot(Dictionary<string, string[]> p)
    {
        var snapshotName = P(p, "SnapshotName");
        if (!_snapshots.TryGetValue(snapshotName, out var snap))
            return Error("SnapshotNotFoundFault", $"Snapshot {snapshotName} not found", 404);

        _snapshots.TryRemove(snapshotName, out _);
        _tags.TryRemove(snap.GetValueOrDefault("ARN")?.ToString() ?? "", out _);
        snap["SnapshotStatus"] = "deleting";
        RecordEvent(snapshotName, "snapshot", "Snapshot deleted");
        return Xml(200, "DeleteSnapshotResponse",
            $"<DeleteSnapshotResult><Snapshot>{SnapshotXml(snap)}</Snapshot></DeleteSnapshotResult>");
    }

    private ServiceResponse DescribeSnapshots(Dictionary<string, string[]> p)
    {
        var snapshotName = P(p, "SnapshotName");
        var clusterId = P(p, "CacheClusterId");
        var rgId = P(p, "ReplicationGroupId");

        var snaps = _snapshots.Values.ToList();
        if (!string.IsNullOrEmpty(snapshotName))
            snaps = snaps.Where(s => s.GetValueOrDefault("SnapshotName")?.ToString() == snapshotName).ToList();
        if (!string.IsNullOrEmpty(clusterId))
            snaps = snaps.Where(s => s.GetValueOrDefault("CacheClusterId")?.ToString() == clusterId).ToList();
        if (!string.IsNullOrEmpty(rgId))
            snaps = snaps.Where(s => s.GetValueOrDefault("ReplicationGroupId")?.ToString() == rgId).ToList();

        var sb = new StringBuilder();
        foreach (var s in snaps)
            sb.Append($"<Snapshot>{SnapshotXml(s)}</Snapshot>");
        return Xml(200, "DescribeSnapshotsResponse",
            $"<DescribeSnapshotsResult><Snapshots>{sb}</Snapshots></DescribeSnapshotsResult>");
    }

    // ── Events ────────────────────────────────────────────────────────────────

    private ServiceResponse DescribeEvents(Dictionary<string, string[]> p)
    {
        var sourceId = P(p, "SourceIdentifier");
        var sourceType = P(p, "SourceType");
        var maxRecords = int.Parse(P(p, "MaxRecords", "100"));

        IEnumerable<Dictionary<string, object>> filtered = _events;
        if (!string.IsNullOrEmpty(sourceId))
            filtered = filtered.Where(e => e["SourceIdentifier"].ToString() == sourceId);
        if (!string.IsNullOrEmpty(sourceType))
            filtered = filtered.Where(e => e["SourceType"].ToString() == sourceType);

        var results = filtered.TakeLast(maxRecords);
        var sb = new StringBuilder();
        foreach (var e in results)
        {
            sb.Append("<member>");
            sb.Append($"<SourceIdentifier>{Esc(e["SourceIdentifier"].ToString()!)}</SourceIdentifier>");
            sb.Append($"<SourceType>{Esc(e["SourceType"].ToString()!)}</SourceType>");
            sb.Append($"<Message>{Esc(e["Message"].ToString()!)}</Message>");
            sb.Append($"<Date>{e["Date"]}</Date>");
            sb.Append("</member>");
        }
        return Xml(200, "DescribeEventsResponse",
            $"<DescribeEventsResult><Events>{sb}</Events></DescribeEventsResult>");
    }

    // ── XML helpers ───────────────────────────────────────────────────────────

    private static string ClusterXmlInner(Dictionary<string, object?> c)
    {
        var sb = new StringBuilder();
        var nodes = c.GetValueOrDefault("CacheNodes") as List<Dictionary<string, object?>>;
        if (nodes is not null)
        {
            foreach (var node in nodes)
            {
                var nep = node.GetValueOrDefault("Endpoint") as Dictionary<string, object?>;
                sb.Append("<CacheNode>");
                sb.Append($"<CacheNodeId>{node.GetValueOrDefault("CacheNodeId")}</CacheNodeId>");
                sb.Append($"<CacheNodeStatus>{node.GetValueOrDefault("CacheNodeStatus")}</CacheNodeStatus>");
                sb.Append($"<Endpoint><Address>{nep?.GetValueOrDefault("Address") ?? "localhost"}</Address>");
                sb.Append($"<Port>{nep?.GetValueOrDefault("Port") ?? 6379}</Port></Endpoint>");
                sb.Append("</CacheNode>");
            }
        }
        var nodesXml = sb.ToString();

        return $"<CacheClusterId>{c["CacheClusterId"]}</CacheClusterId>"
            + $"<CacheClusterStatus>{c["CacheClusterStatus"]}</CacheClusterStatus>"
            + $"<Engine>{c["Engine"]}</Engine>"
            + $"<EngineVersion>{c["EngineVersion"]}</EngineVersion>"
            + $"<CacheNodeType>{c["CacheNodeType"]}</CacheNodeType>"
            + $"<NumCacheNodes>{c["NumCacheNodes"]}</NumCacheNodes>"
            + $"<CacheClusterArn>{c.GetValueOrDefault("CacheClusterArn") ?? ""}</CacheClusterArn>"
            + $"<ARN>{c.GetValueOrDefault("CacheClusterArn") ?? ""}</ARN>"
            + $"<PreferredAvailabilityZone>{c.GetValueOrDefault("PreferredAvailabilityZone") ?? ""}</PreferredAvailabilityZone>"
            + $"<CacheSubnetGroupName>{c.GetValueOrDefault("CacheSubnetGroupName") ?? ""}</CacheSubnetGroupName>"
            + $"<ReplicationGroupId>{c.GetValueOrDefault("ReplicationGroupId") ?? ""}</ReplicationGroupId>"
            + $"<SnapshotRetentionLimit>{c.GetValueOrDefault("SnapshotRetentionLimit") ?? 0}</SnapshotRetentionLimit>"
            + $"<SnapshotWindow>{c.GetValueOrDefault("SnapshotWindow") ?? ""}</SnapshotWindow>"
            + $"<CacheNodes>{nodesXml}</CacheNodes>";
    }

    private static string ClusterXml(Dictionary<string, object?> c)
    {
        return $"<CacheCluster>{ClusterXmlInner(c)}</CacheCluster>";
    }

    private static string RgXml(Dictionary<string, object?> rg)
    {
        var nodeGroupsSb = new StringBuilder();
        var nodeGroups = rg.GetValueOrDefault("NodeGroups") as List<Dictionary<string, object?>>;
        if (nodeGroups is not null)
        {
            foreach (var ng in nodeGroups)
            {
                var membersSb = new StringBuilder();
                var members = ng.GetValueOrDefault("NodeGroupMembers") as List<Dictionary<string, object?>>;
                if (members is not null)
                {
                    foreach (var m in members)
                    {
                        var rep = m.GetValueOrDefault("ReadEndpoint") as Dictionary<string, object?>;
                        membersSb.Append("<NodeGroupMember>");
                        membersSb.Append($"<CacheClusterId>{m.GetValueOrDefault("CacheClusterId") ?? ""}</CacheClusterId>");
                        membersSb.Append($"<CacheNodeId>{m.GetValueOrDefault("CacheNodeId") ?? "0001"}</CacheNodeId>");
                        membersSb.Append($"<CurrentRole>{m.GetValueOrDefault("CurrentRole") ?? "primary"}</CurrentRole>");
                        membersSb.Append($"<PreferredAvailabilityZone>{m.GetValueOrDefault("PreferredAvailabilityZone") ?? ""}</PreferredAvailabilityZone>");
                        membersSb.Append($"<ReadEndpoint><Address>{rep?.GetValueOrDefault("Address") ?? "localhost"}</Address>");
                        membersSb.Append($"<Port>{rep?.GetValueOrDefault("Port") ?? 6379}</Port></ReadEndpoint>");
                        membersSb.Append("</NodeGroupMember>");
                    }
                }

                var pep = ng.GetValueOrDefault("PrimaryEndpoint") as Dictionary<string, object?>;
                var rdr = ng.GetValueOrDefault("ReaderEndpoint") as Dictionary<string, object?>;
                nodeGroupsSb.Append("<NodeGroup>");
                nodeGroupsSb.Append($"<NodeGroupId>{ng["NodeGroupId"]}</NodeGroupId>");
                nodeGroupsSb.Append($"<Status>{ng["Status"]}</Status>");
                nodeGroupsSb.Append($"<PrimaryEndpoint><Address>{pep?.GetValueOrDefault("Address") ?? "localhost"}</Address>");
                nodeGroupsSb.Append($"<Port>{pep?.GetValueOrDefault("Port") ?? 6379}</Port></PrimaryEndpoint>");
                nodeGroupsSb.Append($"<ReaderEndpoint><Address>{rdr?.GetValueOrDefault("Address") ?? "localhost"}</Address>");
                nodeGroupsSb.Append($"<Port>{rdr?.GetValueOrDefault("Port") ?? 6379}</Port></ReaderEndpoint>");
                nodeGroupsSb.Append($"<NodeGroupMembers>{membersSb}</NodeGroupMembers>");
                nodeGroupsSb.Append("</NodeGroup>");
            }
        }

        var configEpXml = "";
        var cep = rg.GetValueOrDefault("ConfigurationEndpoint") as Dictionary<string, object?>;
        if (cep is not null)
        {
            configEpXml = $"<ConfigurationEndpoint><Address>{cep["Address"]}</Address>"
                + $"<Port>{cep["Port"]}</Port></ConfigurationEndpoint>";
        }

        return $"<ReplicationGroupId>{rg["ReplicationGroupId"]}</ReplicationGroupId>"
            + $"<Description>{rg.GetValueOrDefault("Description") ?? ""}</Description>"
            + $"<Status>{rg["Status"]}</Status>"
            + $"<CacheNodeType>{rg.GetValueOrDefault("CacheNodeType") ?? "cache.t3.micro"}</CacheNodeType>"
            + $"<AutomaticFailover>{rg.GetValueOrDefault("AutomaticFailover") ?? "disabled"}</AutomaticFailover>"
            + $"<MultiAZ>{rg.GetValueOrDefault("MultiAZ") ?? "disabled"}</MultiAZ>"
            + $"<ClusterEnabled>{BoolStr(rg.GetValueOrDefault("ClusterEnabled"))}</ClusterEnabled>"
            + $"<AuthTokenEnabled>{BoolStr(rg.GetValueOrDefault("AuthTokenEnabled"))}</AuthTokenEnabled>"
            + $"<TransitEncryptionEnabled>{BoolStr(rg.GetValueOrDefault("TransitEncryptionEnabled"))}</TransitEncryptionEnabled>"
            + $"<AtRestEncryptionEnabled>{BoolStr(rg.GetValueOrDefault("AtRestEncryptionEnabled"))}</AtRestEncryptionEnabled>"
            + $"<SnapshotRetentionLimit>{rg.GetValueOrDefault("SnapshotRetentionLimit") ?? 0}</SnapshotRetentionLimit>"
            + $"<SnapshotWindow>{rg.GetValueOrDefault("SnapshotWindow") ?? ""}</SnapshotWindow>"
            + configEpXml
            + $"<NodeGroups>{nodeGroupsSb}</NodeGroups>"
            + $"<ARN>{rg["ARN"]}</ARN>";
    }

    private static string SnapshotXml(Dictionary<string, object?> snap)
    {
        var nodesSb = new StringBuilder();
        var nodeSnapshots = snap.GetValueOrDefault("NodeSnapshots") as List<Dictionary<string, object?>>;
        if (nodeSnapshots is not null)
        {
            foreach (var ns in nodeSnapshots)
            {
                nodesSb.Append("<NodeSnapshot>");
                nodesSb.Append($"<CacheNodeId>{ns.GetValueOrDefault("CacheNodeId") ?? "0001"}</CacheNodeId>");
                nodesSb.Append($"<SnapshotCreateTime>{ns.GetValueOrDefault("SnapshotCreateTime") ?? 0}</SnapshotCreateTime>");
                nodesSb.Append($"<CacheSize>{ns.GetValueOrDefault("CacheSize") ?? "0 MB"}</CacheSize>");
                nodesSb.Append("</NodeSnapshot>");
            }
        }
        return $"<SnapshotName>{snap["SnapshotName"]}</SnapshotName>"
            + $"<SnapshotStatus>{snap["SnapshotStatus"]}</SnapshotStatus>"
            + $"<SnapshotSource>{snap.GetValueOrDefault("SnapshotSource") ?? "manual"}</SnapshotSource>"
            + $"<CacheClusterId>{snap.GetValueOrDefault("CacheClusterId") ?? ""}</CacheClusterId>"
            + $"<ReplicationGroupId>{snap.GetValueOrDefault("ReplicationGroupId") ?? ""}</ReplicationGroupId>"
            + $"<CacheNodeType>{snap.GetValueOrDefault("CacheNodeType") ?? "cache.t3.micro"}</CacheNodeType>"
            + $"<Engine>{snap.GetValueOrDefault("Engine") ?? "redis"}</Engine>"
            + $"<EngineVersion>{snap.GetValueOrDefault("EngineVersion") ?? "7.0.12"}</EngineVersion>"
            + $"<NodeSnapshots>{nodesSb}</NodeSnapshots>"
            + $"<ARN>{snap.GetValueOrDefault("ARN") ?? ""}</ARN>";
    }

    private static string UserXml(Dictionary<string, object?> u)
    {
        var groupIds = u.GetValueOrDefault("UserGroupIds") as List<string>;
        var groupIdsSb = new StringBuilder();
        if (groupIds is not null)
        {
            foreach (var gid in groupIds)
                groupIdsSb.Append($"<member>{Esc(gid)}</member>");
        }

        var auth = u.GetValueOrDefault("Authentication") as Dictionary<string, object?>;
        return $"<UserId>{u["UserId"]}</UserId>"
            + $"<UserName>{u.GetValueOrDefault("UserName") ?? ""}</UserName>"
            + $"<Engine>{u.GetValueOrDefault("Engine") ?? "redis"}</Engine>"
            + $"<Status>{u.GetValueOrDefault("Status") ?? "active"}</Status>"
            + $"<AccessString>{Esc(u.GetValueOrDefault("AccessString")?.ToString() ?? "")}</AccessString>"
            + $"<UserGroupIds>{groupIdsSb}</UserGroupIds>"
            + $"<Authentication><Type>{auth?.GetValueOrDefault("Type") ?? "no-password"}</Type>"
            + $"<PasswordCount>{auth?.GetValueOrDefault("PasswordCount") ?? 0}</PasswordCount></Authentication>"
            + $"<ARN>{u.GetValueOrDefault("ARN") ?? ""}</ARN>";
    }

    private static string UserGroupXml(Dictionary<string, object?> g)
    {
        var userIds = g.GetValueOrDefault("UserIds") as List<string>;
        var userIdsSb = new StringBuilder();
        if (userIds is not null)
        {
            foreach (var uid in userIds)
                userIdsSb.Append($"<member>{Esc(uid)}</member>");
        }

        var rgs = g.GetValueOrDefault("ReplicationGroups") as List<string>;
        var rgsSb = new StringBuilder();
        if (rgs is not null)
        {
            foreach (var rg in rgs)
                rgsSb.Append($"<member>{Esc(rg)}</member>");
        }

        return $"<UserGroupId>{g["UserGroupId"]}</UserGroupId>"
            + $"<Status>{g.GetValueOrDefault("Status") ?? "active"}</Status>"
            + $"<Engine>{g.GetValueOrDefault("Engine") ?? "redis"}</Engine>"
            + $"<UserIds>{userIdsSb}</UserIds>"
            + $"<ReplicationGroups>{rgsSb}</ReplicationGroups>"
            + $"<ARN>{g.GetValueOrDefault("ARN") ?? ""}</ARN>";
    }

    private ServiceResponse SingleClusterResponse(string rootTag, string resultTag, Dictionary<string, object?> cluster)
    {
        return Xml(200, rootTag,
            $"<{resultTag}><CacheCluster>{ClusterXmlInner(cluster)}</CacheCluster></{resultTag}>");
    }

    // ── Parse helpers ─────────────────────────────────────────────────────────

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
        if (items.Count > 0) return items;

        // Fallback to botocore serializer format: Prefix.<AnyMemberName>.N
        var pattern = new Regex($@"^{Regex.Escape(prefix)}\.([^.]+)\.(\d+)$", RegexOptions.Compiled);
        var numbered = new SortedDictionary<int, string>();
        foreach (var key in p.Keys)
        {
            var m = pattern.Match(key);
            if (m.Success)
            {
                var idx = int.Parse(m.Groups[2].Value);
                numbered[idx] = P(p, key);
            }
        }
        return [.. numbered.Values];
    }

    private static List<Dictionary<string, string>> ExtractTags(Dictionary<string, string[]> p)
    {
        var tags = new List<Dictionary<string, string>>();
        foreach (var tagPrefix in new[] { "Tags.member", "Tags.Tag" })
        {
            for (var idx = 1; ; idx++)
            {
                var key = P(p, $"{tagPrefix}.{idx}.Key");
                if (string.IsNullOrEmpty(key)) break;
                var value = P(p, $"{tagPrefix}.{idx}.Value");
                tags.Add(new Dictionary<string, string> { ["Key"] = key, ["Value"] = value });
            }
            if (tags.Count > 0)
                break;
        }
        return tags;
    }

    private static Dictionary<string, Dictionary<string, string>> DefaultParamsForFamily(string family)
    {
        if (family.StartsWith("redis", StringComparison.OrdinalIgnoreCase))
        {
            return new Dictionary<string, Dictionary<string, string>>
            {
                ["maxmemory-policy"] = new() { ["Value"] = "volatile-lru", ["Description"] = "Eviction policy", ["Source"] = "system", ["DataType"] = "string", ["AllowedValues"] = "volatile-lru,allkeys-lru,volatile-random,allkeys-random,volatile-ttl,noeviction", ["IsModifiable"] = "true", ["MinimumEngineVersion"] = "2.8.6" },
                ["maxmemory-samples"] = new() { ["Value"] = "5", ["Description"] = "Number of keys to sample", ["Source"] = "system", ["DataType"] = "integer", ["AllowedValues"] = "1-", ["IsModifiable"] = "true", ["MinimumEngineVersion"] = "2.8.6" },
                ["timeout"] = new() { ["Value"] = "0", ["Description"] = "Close connection after N seconds idle", ["Source"] = "system", ["DataType"] = "integer", ["AllowedValues"] = "0-", ["IsModifiable"] = "true", ["MinimumEngineVersion"] = "2.6.13" },
                ["tcp-keepalive"] = new() { ["Value"] = "300", ["Description"] = "TCP keepalive", ["Source"] = "system", ["DataType"] = "integer", ["AllowedValues"] = "0-", ["IsModifiable"] = "true", ["MinimumEngineVersion"] = "2.6.13" },
                ["databases"] = new() { ["Value"] = "16", ["Description"] = "Number of databases", ["Source"] = "system", ["DataType"] = "integer", ["AllowedValues"] = "1-1200000", ["IsModifiable"] = "true", ["MinimumEngineVersion"] = "2.6.13" },
            };
        }

        return new Dictionary<string, Dictionary<string, string>>
        {
            ["max_simultaneous_connections_per_server"] = new() { ["Value"] = "8", ["Source"] = "system", ["DataType"] = "integer", ["Description"] = "Max connections", ["IsModifiable"] = "true" },
        };
    }

    // ── Static helpers ────────────────────────────────────────────────────────

    private static string BoolStr(object? val)
    {
        if (val is bool b) return b ? "true" : "false";
        return "false";
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

    // ── XML / Error response builders ─────────────────────────────────────────

    private static readonly IReadOnlyDictionary<string, string> XmlHeaders =
        new Dictionary<string, string> { ["Content-Type"] = "application/xml" };

    private static ServiceResponse Xml(int status, string rootTag, string inner)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{rootTag} xmlns=\"{ElastiCacheNs}\">\n    {inner}\n    <ResponseMetadata><RequestId>{requestId}</RequestId></ResponseMetadata>\n</{rootTag}>";
        return new ServiceResponse(status, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }

    private static ServiceResponse Error(string code, string message, int status)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ErrorResponse xmlns=\"{ElastiCacheNs}\">\n    <Error><Code>{code}</Code><Message>{message}</Message></Error>\n    <RequestId>{requestId}</RequestId>\n</ErrorResponse>";
        return new ServiceResponse(status, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }
}
