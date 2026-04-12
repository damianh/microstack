using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using MicroStack.Internal;

namespace MicroStack.Services.Rds;

internal sealed class RdsServiceHandler : IServiceHandler
{
    public string ServiceName => "rds";

    private const string RdsNs = "http://rds.amazonaws.com/doc/2014-10-31/";

    private static string Region =>
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";

    // ── State stores ──────────────────────────────────────────────────────────

    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _instances = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _clusters = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _subnetGroups = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _paramGroups = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _snapshots = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _clusterParamGroups = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _clusterSnapshots = new();
    private readonly AccountScopedDictionary<string, List<Dictionary<string, string>>> _tags = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _eventSubscriptions = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _dbProxies = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _optionGroups = new();
    private readonly AccountScopedDictionary<string, Dictionary<string, object?>> _globalClusters = new();

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
            _instances.Clear();
            _clusters.Clear();
            _subnetGroups.Clear();
            _paramGroups.Clear();
            _snapshots.Clear();
            _clusterParamGroups.Clear();
            _clusterSnapshots.Clear();
            _tags.Clear();
            _eventSubscriptions.Clear();
            _dbProxies.Clear();
            _optionGroups.Clear();
            _globalClusters.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // ── Action dispatch ───────────────────────────────────────────────────────

    private ServiceResponse DispatchAction(string action, Dictionary<string, string[]> p)
    {
        return action switch
        {
            // DB Instances
            "CreateDBInstance" => CreateDBInstance(p),
            "DescribeDBInstances" => DescribeDBInstances(p),
            "ModifyDBInstance" => ModifyDBInstance(p),
            "DeleteDBInstance" => DeleteDBInstance(p),
            "RebootDBInstance" => RebootDBInstance(p),
            "StopDBInstance" => StopDBInstance(p),
            "StartDBInstance" => StartDBInstance(p),
            // DB Clusters
            "CreateDBCluster" => CreateDBCluster(p),
            "DescribeDBClusters" => DescribeDBClusters(p),
            "ModifyDBCluster" => ModifyDBCluster(p),
            "DeleteDBCluster" => DeleteDBCluster(p),
            "StopDBCluster" => StopDBCluster(p),
            "StartDBCluster" => StartDBCluster(p),
            // DB Subnet Groups
            "CreateDBSubnetGroup" => CreateDBSubnetGroup(p),
            "DescribeDBSubnetGroups" => DescribeDBSubnetGroups(p),
            "ModifyDBSubnetGroup" => ModifyDBSubnetGroup(p),
            "DeleteDBSubnetGroup" => DeleteDBSubnetGroup(p),
            // DB Parameter Groups
            "CreateDBParameterGroup" => CreateDBParameterGroup(p),
            "DescribeDBParameterGroups" => DescribeDBParameterGroups(p),
            "DeleteDBParameterGroup" => DeleteDBParameterGroup(p),
            "DescribeDBParameters" => DescribeDBParameters(p),
            "ModifyDBParameterGroup" => ModifyDBParameterGroup(p),
            // DB Cluster Parameter Groups
            "CreateDBClusterParameterGroup" => CreateDBClusterParameterGroup(p),
            "DescribeDBClusterParameterGroups" => DescribeDBClusterParameterGroups(p),
            "DeleteDBClusterParameterGroup" => DeleteDBClusterParameterGroup(p),
            // DB Snapshots
            "CreateDBSnapshot" => CreateDBSnapshot(p),
            "DescribeDBSnapshots" => DescribeDBSnapshots(p),
            "DeleteDBSnapshot" => DeleteDBSnapshot(p),
            "CopyDBSnapshot" => CopyDBSnapshot(p),
            // DB Cluster Snapshots
            "CreateDBClusterSnapshot" => CreateDBClusterSnapshot(p),
            "DescribeDBClusterSnapshots" => DescribeDBClusterSnapshots(p),
            "DeleteDBClusterSnapshot" => DeleteDBClusterSnapshot(p),
            // Tags
            "AddTagsToResource" => AddTagsToResource(p),
            "ListTagsForResource" => ListTagsForResource(p),
            "RemoveTagsFromResource" => RemoveTagsFromResource(p),
            // Event Subscriptions
            "CreateEventSubscription" => CreateEventSubscription(p),
            "DescribeEventSubscriptions" => DescribeEventSubscriptions(p),
            "DeleteEventSubscription" => DeleteEventSubscription(p),
            // DB Proxy
            "CreateDBProxy" => CreateDBProxy(p),
            "DescribeDBProxies" => DescribeDBProxies(p),
            "DeleteDBProxy" => DeleteDBProxy(p),
            // Option Groups
            "CreateOptionGroup" => CreateOptionGroup(p),
            "DescribeOptionGroups" => DescribeOptionGroups(p),
            "DeleteOptionGroup" => DeleteOptionGroup(p),
            // Global Clusters
            "CreateGlobalCluster" => CreateGlobalCluster(p),
            "DescribeGlobalClusters" => DescribeGlobalClusters(p),
            "ModifyGlobalCluster" => ModifyGlobalCluster(p),
            "DeleteGlobalCluster" => DeleteGlobalCluster(p),
            "RemoveFromGlobalCluster" => RemoveFromGlobalCluster(p),
            // Describe helpers
            "DescribeOrderableDBInstanceOptions" => DescribeOrderableDBInstanceOptions(p),
            "DescribeEngineDefaultClusterParameters" => DescribeEngineDefaultClusterParameters(p),
            "DescribeDBEngineVersions" => DescribeDBEngineVersions(p),
            _ => Error("InvalidAction", $"Unknown RDS action: {action}", 400),
        };
    }

    // ── DB Instances ──────────────────────────────────────────────────────────

    private ServiceResponse CreateDBInstance(Dictionary<string, string[]> p)
    {
        var dbId = P(p, "DBInstanceIdentifier");
        if (string.IsNullOrEmpty(dbId))
            return Error("MissingParameter", "DBInstanceIdentifier is required", 400);
        if (_instances.ContainsKey(dbId))
            return Error("DBInstanceAlreadyExistsFault", $"DB instance {dbId} already exists", 400);

        var engine = P(p, "Engine", "postgres");
        var engineVersion = P(p, "EngineVersion");
        if (string.IsNullOrEmpty(engineVersion))
            engineVersion = DefaultEngineVersion(engine);
        var dbClass = P(p, "DBInstanceClass", "db.t3.micro");
        var masterUser = P(p, "MasterUsername", "admin");
        var dbName = P(p, "DBName", "mydb");
        var port = int.Parse(P(p, "Port", DefaultPort(engine)));
        var allocatedStorage = int.Parse(P(p, "AllocatedStorage", "20"));
        var storageType = P(p, "StorageType", "gp2");
        var subnetGroupName = P(p, "DBSubnetGroupName", "default");

        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:db:{dbId}";
        var dbiResourceId = $"db-{Guid.NewGuid():N}"[..23].ToUpperInvariant();
        var now = TimeHelpers.NowIso();

        var vpcSgs = ParseMemberList(p, "VpcSecurityGroupIds");
        var vpcSgList = vpcSgs.Select(sg => new Dictionary<string, object?>
        {
            ["VpcSecurityGroupId"] = sg,
            ["Status"] = "active",
        }).ToList();

        var subnetGroup = _subnetGroups.TryGetValue(subnetGroupName, out var existingSg)
            ? existingSg
            : new Dictionary<string, object?>
            {
                ["DBSubnetGroupName"] = subnetGroupName,
                ["DBSubnetGroupDescription"] = "default",
                ["SubnetGroupStatus"] = "Complete",
                ["Subnets"] = new List<Dictionary<string, object?>>(),
                ["VpcId"] = "vpc-00000000",
                ["DBSubnetGroupArn"] = $"arn:aws:rds:{Region}:{accountId}:subgrp:{subnetGroupName}",
            };

        var clusterId = P(p, "DBClusterIdentifier");
        var paramGroupName = P(p, "DBParameterGroupName");
        if (string.IsNullOrEmpty(paramGroupName))
            paramGroupName = $"default.{engine}{engineVersion.Split('.')[0]}";

        var instance = new Dictionary<string, object?>
        {
            ["DBInstanceIdentifier"] = dbId,
            ["DBInstanceClass"] = dbClass,
            ["Engine"] = engine,
            ["EngineVersion"] = engineVersion,
            ["DBInstanceStatus"] = "available",
            ["MasterUsername"] = masterUser,
            ["DBName"] = dbName,
            ["Endpoint"] = new Dictionary<string, object?>
            {
                ["Address"] = "localhost",
                ["Port"] = port,
                ["HostedZoneId"] = "Z2R2ITUGPM61AM",
            },
            ["AllocatedStorage"] = allocatedStorage,
            ["InstanceCreateTime"] = now,
            ["PreferredBackupWindow"] = "03:00-04:00",
            ["BackupRetentionPeriod"] = int.Parse(P(p, "BackupRetentionPeriod", "1")),
            ["DBSecurityGroups"] = new List<object>(),
            ["VpcSecurityGroups"] = vpcSgList,
            ["DBParameterGroups"] = new List<Dictionary<string, object?>>
            {
                new()
                {
                    ["DBParameterGroupName"] = paramGroupName,
                    ["ParameterApplyStatus"] = "in-sync",
                },
            },
            ["AvailabilityZone"] = P(p, "AvailabilityZone", $"{Region}a"),
            ["DBSubnetGroup"] = subnetGroup,
            ["PreferredMaintenanceWindow"] = "sun:05:00-sun:06:00",
            ["PendingModifiedValues"] = new Dictionary<string, object?>(),
            ["LatestRestorableTime"] = now,
            ["MultiAZ"] = P(p, "MultiAZ") == "true",
            ["AutoMinorVersionUpgrade"] = P(p, "AutoMinorVersionUpgrade") != "false",
            ["ReadReplicaDBInstanceIdentifiers"] = new List<string>(),
            ["ReadReplicaSourceDBInstanceIdentifier"] = "",
            ["LicenseModel"] = LicenseModel(engine),
            ["Iops"] = string.IsNullOrEmpty(P(p, "Iops")) ? null : (object)int.Parse(P(p, "Iops")),
            ["OptionGroupMemberships"] = new List<Dictionary<string, object?>>
            {
                new()
                {
                    ["OptionGroupName"] = $"default:{engine}-{engineVersion.Split('.')[0]}",
                    ["Status"] = "in-sync",
                },
            },
            ["PubliclyAccessible"] = P(p, "PubliclyAccessible") == "true",
            ["StorageType"] = storageType,
            ["DbInstancePort"] = 0,
            ["DBClusterIdentifier"] = clusterId,
            ["StorageEncrypted"] = P(p, "StorageEncrypted") == "true",
            ["KmsKeyId"] = P(p, "KmsKeyId"),
            ["DbiResourceId"] = dbiResourceId,
            ["CACertificateIdentifier"] = "rds-ca-rsa2048-g1",
            ["CopyTagsToSnapshot"] = P(p, "CopyTagsToSnapshot") == "true",
            ["MonitoringInterval"] = int.Parse(P(p, "MonitoringInterval", "0")),
            ["MonitoringRoleArn"] = P(p, "MonitoringRoleArn"),
            ["PromotionTier"] = int.Parse(P(p, "PromotionTier", "1")),
            ["DBInstanceArn"] = arn,
            ["IAMDatabaseAuthenticationEnabled"] = P(p, "EnableIAMDatabaseAuthentication") == "true",
            ["PerformanceInsightsEnabled"] = P(p, "EnablePerformanceInsights") == "true",
            ["DeletionProtection"] = P(p, "DeletionProtection") == "true",
            ["MaxAllocatedStorage"] = int.Parse(P(p, "MaxAllocatedStorage", allocatedStorage.ToString())),
            ["TagList"] = new List<Dictionary<string, string>>(),
            ["CertificateDetails"] = new Dictionary<string, object?>
            {
                ["CAIdentifier"] = "rds-ca-rsa2048-g1",
                ["ValidTill"] = "2061-01-01T00:00:00Z",
            },
            ["BackupTarget"] = "region",
            ["NetworkType"] = "IPV4",
            ["StorageThroughput"] = 0,
        };

        _instances[dbId] = instance;

        var reqTags = ParseTags(p);
        if (reqTags.Count > 0)
        {
            _tags[arn] = reqTags;
            instance["TagList"] = reqTags;
        }

        return SingleInstanceResponse("CreateDBInstanceResponse", "CreateDBInstanceResult", instance);
    }

    private ServiceResponse DescribeDBInstances(Dictionary<string, string[]> p)
    {
        var dbId = P(p, "DBInstanceIdentifier");
        List<Dictionary<string, object?>> instances;

        if (!string.IsNullOrEmpty(dbId))
        {
            if (!_instances.TryGetValue(dbId, out var instance))
                return Error("DBInstanceNotFound", $"DBInstance {dbId} not found.", 404);
            instances = [instance];
        }
        else
        {
            instances = _instances.Values.ToList();
            var filters = ParseFilters(p);
            if (filters.Count > 0)
                instances = ApplyInstanceFilters(instances, filters);
        }

        var members = new StringBuilder();
        foreach (var i in instances)
            members.Append($"<DBInstance>{InstanceXml(i)}</DBInstance>");

        return RdsXml(200, "DescribeDBInstancesResponse",
            $"<DescribeDBInstancesResult><DBInstances>{members}</DBInstances></DescribeDBInstancesResult>");
    }

    private ServiceResponse ModifyDBInstance(Dictionary<string, string[]> p)
    {
        var dbId = P(p, "DBInstanceIdentifier");
        if (!_instances.TryGetValue(dbId, out var instance))
            return Error("DBInstanceNotFound", $"DBInstance {dbId} not found.", 404);

        var applyImmediately = P(p, "ApplyImmediately") == "true";
        var pending = new Dictionary<string, object?>();

        ApplyModifyField(p, instance, pending, "DBInstanceClass", "DBInstanceClass", applyImmediately, false);
        ApplyModifyField(p, instance, pending, "AllocatedStorage", "AllocatedStorage", applyImmediately, true);
        ApplyModifyField(p, instance, pending, "EngineVersion", "EngineVersion", applyImmediately, false);
        ApplyModifyField(p, instance, pending, "StorageType", "StorageType", applyImmediately, false);
        ApplyModifyField(p, instance, pending, "BackupRetentionPeriod", "BackupRetentionPeriod", applyImmediately, true);
        ApplyModifyField(p, instance, pending, "PreferredBackupWindow", "PreferredBackupWindow", applyImmediately, false);
        ApplyModifyField(p, instance, pending, "PreferredMaintenanceWindow", "PreferredMaintenanceWindow", applyImmediately, false);
        ApplyModifyField(p, instance, pending, "CACertificateIdentifier", "CACertificateIdentifier", applyImmediately, false);
        ApplyModifyField(p, instance, pending, "MonitoringInterval", "MonitoringInterval", applyImmediately, true);
        ApplyModifyField(p, instance, pending, "MonitoringRoleArn", "MonitoringRoleArn", applyImmediately, false);
        ApplyModifyField(p, instance, pending, "MaxAllocatedStorage", "MaxAllocatedStorage", applyImmediately, true);
        ApplyModifyField(p, instance, pending, "Iops", "Iops", applyImmediately, true);

        ApplyModifyBoolField(p, instance, pending, "MultiAZ", "MultiAZ", applyImmediately);
        ApplyModifyBoolField(p, instance, pending, "PubliclyAccessible", "PubliclyAccessible", applyImmediately);
        ApplyModifyBoolField(p, instance, pending, "DeletionProtection", "DeletionProtection", applyImmediately);
        ApplyModifyBoolField(p, instance, pending, "CopyTagsToSnapshot", "CopyTagsToSnapshot", applyImmediately);

        var newParamGroupName = P(p, "DBParameterGroupName");
        if (!string.IsNullOrEmpty(newParamGroupName))
        {
            instance["DBParameterGroups"] = new List<Dictionary<string, object?>>
            {
                new()
                {
                    ["DBParameterGroupName"] = newParamGroupName,
                    ["ParameterApplyStatus"] = applyImmediately ? "applying" : "pending-reboot",
                },
            };
        }

        var vpcSgs = ParseMemberList(p, "VpcSecurityGroupIds");
        if (vpcSgs.Count > 0)
        {
            instance["VpcSecurityGroups"] = vpcSgs.Select(sg => new Dictionary<string, object?>
            {
                ["VpcSecurityGroupId"] = sg,
                ["Status"] = "active",
            }).ToList();
        }

        if (pending.Count > 0)
            instance["PendingModifiedValues"] = pending;

        return SingleInstanceResponse("ModifyDBInstanceResponse", "ModifyDBInstanceResult", instance);
    }

    private ServiceResponse DeleteDBInstance(Dictionary<string, string[]> p)
    {
        var dbId = P(p, "DBInstanceIdentifier");
        if (!_instances.TryGetValue(dbId, out var instance))
            return Error("DBInstanceNotFound", $"DBInstance {dbId} not found.", 404);

        if (instance.TryGetValue("DeletionProtection", out var dp) && dp is true)
            return Error("InvalidParameterCombination",
                "Cannot delete a DB instance when DeletionProtection is enabled.", 400);

        var skipSnapshot = P(p, "SkipFinalSnapshot") == "true";
        var finalSnapId = P(p, "FinalDBSnapshotIdentifier");
        if (!skipSnapshot && !string.IsNullOrEmpty(finalSnapId))
            CreateSnapshotInternal(finalSnapId, instance);

        instance["DBInstanceStatus"] = "deleting";
        var arn = (string)(instance["DBInstanceArn"] ?? "");
        _tags.TryRemove(arn, out _);
        _instances.TryRemove(dbId, out _);

        return SingleInstanceResponse("DeleteDBInstanceResponse", "DeleteDBInstanceResult", instance);
    }

    private ServiceResponse RebootDBInstance(Dictionary<string, string[]> p)
    {
        var dbId = P(p, "DBInstanceIdentifier");
        if (!_instances.TryGetValue(dbId, out var instance))
            return Error("DBInstanceNotFound", $"DBInstance {dbId} not found.", 404);
        instance["DBInstanceStatus"] = "available";
        return SingleInstanceResponse("RebootDBInstanceResponse", "RebootDBInstanceResult", instance);
    }

    private ServiceResponse StopDBInstance(Dictionary<string, string[]> p)
    {
        var dbId = P(p, "DBInstanceIdentifier");
        if (!_instances.TryGetValue(dbId, out var instance))
            return Error("DBInstanceNotFound", $"DBInstance {dbId} not found.", 404);
        instance["DBInstanceStatus"] = "stopped";
        return SingleInstanceResponse("StopDBInstanceResponse", "StopDBInstanceResult", instance);
    }

    private ServiceResponse StartDBInstance(Dictionary<string, string[]> p)
    {
        var dbId = P(p, "DBInstanceIdentifier");
        if (!_instances.TryGetValue(dbId, out var instance))
            return Error("DBInstanceNotFound", $"DBInstance {dbId} not found.", 404);
        instance["DBInstanceStatus"] = "available";
        return SingleInstanceResponse("StartDBInstanceResponse", "StartDBInstanceResult", instance);
    }

    // ── DB Clusters ───────────────────────────────────────────────────────────

    private ServiceResponse CreateDBCluster(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "DBClusterIdentifier");
        if (string.IsNullOrEmpty(clusterId))
            return Error("MissingParameter", "DBClusterIdentifier is required", 400);
        if (_clusters.ContainsKey(clusterId))
            return Error("DBClusterAlreadyExistsFault", $"DB cluster {clusterId} already exists.", 400);

        var engine = P(p, "Engine", "aurora-postgresql");
        var engineVersion = P(p, "EngineVersion");
        if (string.IsNullOrEmpty(engineVersion))
            engineVersion = DefaultEngineVersion(engine);
        var port = int.Parse(P(p, "Port", DefaultPort(engine)));
        var masterUser = P(p, "MasterUsername", "admin");
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:cluster:{clusterId}";
        var uniqueSuffix = Guid.NewGuid().ToString("N")[..8];
        var now = TimeHelpers.NowIso();

        var vpcSgs = ParseMemberList(p, "VpcSecurityGroupIds");
        var vpcSgList = vpcSgs.Select(sg => new Dictionary<string, object?>
        {
            ["VpcSecurityGroupId"] = sg,
            ["Status"] = "active",
        }).ToList();

        var azList = ParseMemberList(p, "AvailabilityZones");
        if (azList.Count == 0)
            azList = [$"{Region}a", $"{Region}b", $"{Region}c"];

        var cluster = new Dictionary<string, object?>
        {
            ["DBClusterIdentifier"] = clusterId,
            ["DBClusterArn"] = arn,
            ["Engine"] = engine,
            ["EngineVersion"] = engineVersion,
            ["EngineMode"] = P(p, "EngineMode", "provisioned"),
            ["Status"] = "available",
            ["MasterUsername"] = masterUser,
            ["DatabaseName"] = P(p, "DatabaseName"),
            ["Endpoint"] = $"{clusterId}.cluster-{uniqueSuffix}.{Region}.rds.amazonaws.com",
            ["ReaderEndpoint"] = $"{clusterId}.cluster-ro-{uniqueSuffix}.{Region}.rds.amazonaws.com",
            ["Port"] = port,
            ["MultiAZ"] = P(p, "MultiAZ") == "true",
            ["AvailabilityZones"] = azList,
            ["DBClusterMembers"] = new List<Dictionary<string, object?>>(),
            ["VpcSecurityGroups"] = vpcSgList,
            ["DBSubnetGroup"] = P(p, "DBSubnetGroupName", "default"),
            ["DBClusterParameterGroup"] = P(p, "DBClusterParameterGroupName", $"default.{engine}"),
            ["BackupRetentionPeriod"] = int.Parse(P(p, "BackupRetentionPeriod", "1")),
            ["PreferredBackupWindow"] = P(p, "PreferredBackupWindow", "03:00-04:00"),
            ["PreferredMaintenanceWindow"] = P(p, "PreferredMaintenanceWindow", "sun:05:00-sun:06:00"),
            ["ClusterCreateTime"] = now,
            ["EarliestRestorableTime"] = now,
            ["LatestRestorableTime"] = now,
            ["StorageEncrypted"] = P(p, "StorageEncrypted") == "true",
            ["KmsKeyId"] = P(p, "KmsKeyId"),
            ["DeletionProtection"] = P(p, "DeletionProtection") == "true",
            ["IAMDatabaseAuthenticationEnabled"] = P(p, "EnableIAMDatabaseAuthentication") == "true",
            ["HttpEndpointEnabled"] = P(p, "EnableHttpEndpoint") == "true",
            ["CopyTagsToSnapshot"] = P(p, "CopyTagsToSnapshot") == "true",
            ["CrossAccountClone"] = false,
            ["DbClusterResourceId"] = $"cluster-{Guid.NewGuid():N}"[..28].ToUpperInvariant(),
            ["TagList"] = new List<Dictionary<string, string>>(),
            ["HostedZoneId"] = "Z2R2ITUGPM61AM",
            ["AllocatedStorage"] = 1,
            ["ActivityStreamStatus"] = "stopped",
        };

        _clusters[clusterId] = cluster;

        var reqTags = ParseTags(p);
        if (reqTags.Count > 0)
        {
            _tags[arn] = reqTags;
            cluster["TagList"] = reqTags;
        }

        return RdsXml(200, "CreateDBClusterResponse",
            $"<CreateDBClusterResult><DBCluster>{ClusterXml(cluster)}</DBCluster></CreateDBClusterResult>");
    }

    private ServiceResponse DescribeDBClusters(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "DBClusterIdentifier");
        List<Dictionary<string, object?>> clusters;

        if (!string.IsNullOrEmpty(clusterId))
        {
            if (!_clusters.TryGetValue(clusterId, out var cluster))
                return Error("DBClusterNotFoundFault", $"DBCluster {clusterId} not found.", 404);
            clusters = [cluster];
        }
        else
        {
            clusters = _clusters.Values.ToList();
            var filters = ParseFilters(p);
            if (filters.Count > 0)
                clusters = ApplyClusterFilters(clusters, filters);
        }

        var members = new StringBuilder();
        foreach (var c in clusters)
            members.Append($"<DBCluster>{ClusterXml(c)}</DBCluster>");

        return RdsXml(200, "DescribeDBClustersResponse",
            $"<DescribeDBClustersResult><DBClusters>{members}</DBClusters></DescribeDBClustersResult>");
    }

    private ServiceResponse ModifyDBCluster(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "DBClusterIdentifier");
        if (!_clusters.TryGetValue(clusterId, out var cluster))
            return Error("DBClusterNotFoundFault", $"DBCluster {clusterId} not found.", 404);

        var val = P(p, "EngineVersion");
        if (!string.IsNullOrEmpty(val))
            cluster["EngineVersion"] = val;

        val = P(p, "Port");
        if (!string.IsNullOrEmpty(val))
            cluster["Port"] = int.Parse(val);

        val = P(p, "BackupRetentionPeriod");
        if (!string.IsNullOrEmpty(val))
            cluster["BackupRetentionPeriod"] = int.Parse(val);

        val = P(p, "PreferredBackupWindow");
        if (!string.IsNullOrEmpty(val))
            cluster["PreferredBackupWindow"] = val;

        val = P(p, "PreferredMaintenanceWindow");
        if (!string.IsNullOrEmpty(val))
            cluster["PreferredMaintenanceWindow"] = val;

        val = P(p, "DeletionProtection");
        if (!string.IsNullOrEmpty(val))
            cluster["DeletionProtection"] = val == "true";

        val = P(p, "EnableIAMDatabaseAuthentication");
        if (!string.IsNullOrEmpty(val))
            cluster["IAMDatabaseAuthenticationEnabled"] = val == "true";

        val = P(p, "EnableHttpEndpoint");
        if (!string.IsNullOrEmpty(val))
            cluster["HttpEndpointEnabled"] = val == "true";

        val = P(p, "CopyTagsToSnapshot");
        if (!string.IsNullOrEmpty(val))
            cluster["CopyTagsToSnapshot"] = val == "true";

        val = P(p, "DBClusterParameterGroupName");
        if (!string.IsNullOrEmpty(val))
            cluster["DBClusterParameterGroup"] = val;

        var vpcSgs = ParseMemberList(p, "VpcSecurityGroupIds");
        if (vpcSgs.Count > 0)
        {
            cluster["VpcSecurityGroups"] = vpcSgs.Select(sg => new Dictionary<string, object?>
            {
                ["VpcSecurityGroupId"] = sg,
                ["Status"] = "active",
            }).ToList();
        }

        return RdsXml(200, "ModifyDBClusterResponse",
            $"<ModifyDBClusterResult><DBCluster>{ClusterXml(cluster)}</DBCluster></ModifyDBClusterResult>");
    }

    private ServiceResponse DeleteDBCluster(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "DBClusterIdentifier");
        if (!_clusters.TryGetValue(clusterId, out var cluster))
            return Error("DBClusterNotFoundFault", $"DBCluster {clusterId} not found.", 404);

        if (cluster.TryGetValue("DeletionProtection", out var dp) && dp is true)
            return Error("InvalidParameterCombination",
                "Cannot delete a DB cluster when DeletionProtection is enabled.", 400);

        cluster["Status"] = "deleting";
        var arn = (string)(cluster["DBClusterArn"] ?? "");
        _tags.TryRemove(arn, out _);
        _clusters.TryRemove(clusterId, out _);

        return RdsXml(200, "DeleteDBClusterResponse",
            $"<DeleteDBClusterResult><DBCluster>{ClusterXml(cluster)}</DBCluster></DeleteDBClusterResult>");
    }

    private ServiceResponse StopDBCluster(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "DBClusterIdentifier");
        if (!_clusters.TryGetValue(clusterId, out var cluster))
            return Error("DBClusterNotFoundFault", $"DBCluster {clusterId} not found.", 404);
        cluster["Status"] = "stopped";
        return RdsXml(200, "StopDBClusterResponse",
            $"<StopDBClusterResult><DBCluster>{ClusterXml(cluster)}</DBCluster></StopDBClusterResult>");
    }

    private ServiceResponse StartDBCluster(Dictionary<string, string[]> p)
    {
        var clusterId = P(p, "DBClusterIdentifier");
        if (!_clusters.TryGetValue(clusterId, out var cluster))
            return Error("DBClusterNotFoundFault", $"DBCluster {clusterId} not found.", 404);
        cluster["Status"] = "available";
        return RdsXml(200, "StartDBClusterResponse",
            $"<StartDBClusterResult><DBCluster>{ClusterXml(cluster)}</DBCluster></StartDBClusterResult>");
    }

    // ── DB Subnet Groups ──────────────────────────────────────────────────────

    private ServiceResponse CreateDBSubnetGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBSubnetGroupName");
        if (string.IsNullOrEmpty(name))
            return Error("MissingParameter", "DBSubnetGroupName is required", 400);

        var desc = P(p, "DBSubnetGroupDescription", name);
        var subnetIds = ParseMemberList(p, "SubnetIds");
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:subgrp:{name}";

        var subnets = subnetIds.Select(sid => new Dictionary<string, object?>
        {
            ["SubnetIdentifier"] = sid,
            ["SubnetAvailabilityZone"] = new Dictionary<string, object?> { ["Name"] = $"{Region}a" },
            ["SubnetOutpost"] = new Dictionary<string, object?>(),
            ["SubnetStatus"] = "Active",
        }).ToList();

        _subnetGroups[name] = new Dictionary<string, object?>
        {
            ["DBSubnetGroupName"] = name,
            ["DBSubnetGroupDescription"] = desc,
            ["VpcId"] = "vpc-00000000",
            ["SubnetGroupStatus"] = "Complete",
            ["Subnets"] = subnets,
            ["DBSubnetGroupArn"] = arn,
            ["SupportedNetworkTypes"] = new List<string> { "IPV4" },
        };

        var reqTags = ParseTags(p);
        if (reqTags.Count > 0)
            _tags[arn] = reqTags;

        return RdsXml(200, "CreateDBSubnetGroupResponse",
            $"<CreateDBSubnetGroupResult><DBSubnetGroup>{SubnetGroupXml(_subnetGroups[name])}</DBSubnetGroup></CreateDBSubnetGroupResult>");
    }

    private ServiceResponse DescribeDBSubnetGroups(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBSubnetGroupName");
        List<Dictionary<string, object?>> groups;

        if (!string.IsNullOrEmpty(name))
        {
            if (!_subnetGroups.TryGetValue(name, out var sg))
                return Error("DBSubnetGroupNotFoundFault", $"Subnet group {name} not found.", 404);
            groups = [sg];
        }
        else
        {
            groups = _subnetGroups.Values.ToList();
        }

        var members = new StringBuilder();
        foreach (var g in groups)
            members.Append($"<DBSubnetGroup>{SubnetGroupXml(g)}</DBSubnetGroup>");

        return RdsXml(200, "DescribeDBSubnetGroupsResponse",
            $"<DescribeDBSubnetGroupsResult><DBSubnetGroups>{members}</DBSubnetGroups></DescribeDBSubnetGroupsResult>");
    }

    private ServiceResponse ModifyDBSubnetGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBSubnetGroupName");
        if (!_subnetGroups.TryGetValue(name, out var sg))
            return Error("DBSubnetGroupNotFoundFault", $"Subnet group {name} not found.", 404);

        var desc = P(p, "DBSubnetGroupDescription");
        if (!string.IsNullOrEmpty(desc))
            sg["DBSubnetGroupDescription"] = desc;

        var subnetIds = ParseMemberList(p, "SubnetIds");
        if (subnetIds.Count > 0)
        {
            sg["Subnets"] = subnetIds.Select(sid => new Dictionary<string, object?>
            {
                ["SubnetIdentifier"] = sid,
                ["SubnetAvailabilityZone"] = new Dictionary<string, object?> { ["Name"] = $"{Region}a" },
                ["SubnetOutpost"] = new Dictionary<string, object?>(),
                ["SubnetStatus"] = "Active",
            }).ToList();
        }

        return RdsXml(200, "ModifyDBSubnetGroupResponse",
            $"<ModifyDBSubnetGroupResult><DBSubnetGroup>{SubnetGroupXml(sg)}</DBSubnetGroup></ModifyDBSubnetGroupResult>");
    }

    private ServiceResponse DeleteDBSubnetGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBSubnetGroupName");
        if (!_subnetGroups.TryRemove(name, out var sg))
            return Error("DBSubnetGroupNotFoundFault", $"Subnet group {name} not found.", 404);

        var arn = (string)(sg["DBSubnetGroupArn"] ?? "");
        _tags.TryRemove(arn, out _);

        return RdsXml(200, "DeleteDBSubnetGroupResponse", "");
    }

    // ── DB Parameter Groups ───────────────────────────────────────────────────

    private ServiceResponse CreateDBParameterGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBParameterGroupName");
        if (string.IsNullOrEmpty(name))
            return Error("MissingParameter", "DBParameterGroupName is required", 400);

        var family = P(p, "DBParameterGroupFamily", "postgres15");
        var desc = P(p, "Description", name);
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:pg:{name}";

        _paramGroups[name] = new Dictionary<string, object?>
        {
            ["DBParameterGroupName"] = name,
            ["DBParameterGroupFamily"] = family,
            ["Description"] = desc,
            ["DBParameterGroupArn"] = arn,
            ["Parameters"] = new Dictionary<string, string>(),
        };

        var reqTags = ParseTags(p);
        if (reqTags.Count > 0)
            _tags[arn] = reqTags;

        return RdsXml(200, "CreateDBParameterGroupResponse",
            $"<CreateDBParameterGroupResult><DBParameterGroup>"
            + $"<DBParameterGroupName>{name}</DBParameterGroupName>"
            + $"<DBParameterGroupFamily>{family}</DBParameterGroupFamily>"
            + $"<Description>{Esc(desc)}</Description>"
            + $"<DBParameterGroupArn>{arn}</DBParameterGroupArn>"
            + "</DBParameterGroup></CreateDBParameterGroupResult>");
    }

    private ServiceResponse DescribeDBParameterGroups(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBParameterGroupName");
        List<Dictionary<string, object?>> groups;

        if (!string.IsNullOrEmpty(name))
        {
            if (!_paramGroups.TryGetValue(name, out var pg))
                return Error("DBParameterGroupNotFoundFault", $"Parameter group {name} not found.", 404);
            groups = [pg];
        }
        else
        {
            groups = _paramGroups.Values.ToList();
        }

        var members = new StringBuilder();
        foreach (var g in groups)
        {
            members.Append("<DBParameterGroup>"
                + $"<DBParameterGroupName>{g["DBParameterGroupName"]}</DBParameterGroupName>"
                + $"<DBParameterGroupFamily>{g["DBParameterGroupFamily"]}</DBParameterGroupFamily>"
                + $"<Description>{Esc((string)(g["Description"] ?? ""))}</Description>"
                + $"<DBParameterGroupArn>{g["DBParameterGroupArn"]}</DBParameterGroupArn>"
                + "</DBParameterGroup>");
        }

        return RdsXml(200, "DescribeDBParameterGroupsResponse",
            $"<DescribeDBParameterGroupsResult><DBParameterGroups>{members}</DBParameterGroups></DescribeDBParameterGroupsResult>");
    }

    private ServiceResponse DeleteDBParameterGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBParameterGroupName");
        if (!_paramGroups.TryRemove(name, out var pg))
            return Error("DBParameterGroupNotFoundFault", $"Parameter group {name} not found.", 404);

        var arn = (string)(pg["DBParameterGroupArn"] ?? "");
        _tags.TryRemove(arn, out _);

        return RdsXml(200, "DeleteDBParameterGroupResponse", "");
    }

    private ServiceResponse DescribeDBParameters(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBParameterGroupName");
        if (!_paramGroups.TryGetValue(name, out var pg))
            return Error("DBParameterGroupNotFoundFault", $"Parameter group {name} not found.", 404);

        var family = (string)(pg["DBParameterGroupFamily"] ?? "");
        var defaultParams = DefaultParametersForFamily(family);
        var custom = pg["Parameters"] as Dictionary<string, string> ?? new Dictionary<string, string>();

        var paramsXml = new StringBuilder();
        foreach (var param in defaultParams)
        {
            var pName = param["name"];
            var value = custom.TryGetValue(pName, out var cv) ? cv : param.GetValueOrDefault("default", "");
            var source = custom.ContainsKey(pName) ? "user" : "engine-default";
            paramsXml.Append("<Parameter>"
                + $"<ParameterName>{pName}</ParameterName>"
                + $"<ParameterValue>{value}</ParameterValue>"
                + $"<Description>{Esc(param.GetValueOrDefault("description", ""))}</Description>"
                + $"<Source>{source}</Source>"
                + $"<ApplyType>{param.GetValueOrDefault("apply_type", "dynamic")}</ApplyType>"
                + $"<DataType>{param.GetValueOrDefault("data_type", "string")}</DataType>"
                + $"<IsModifiable>{param.GetValueOrDefault("modifiable", "true")}</IsModifiable>"
                + "<ApplyMethod>pending-reboot</ApplyMethod>"
                + "</Parameter>");
        }

        return RdsXml(200, "DescribeDBParametersResponse",
            $"<DescribeDBParametersResult><Parameters>{paramsXml}</Parameters></DescribeDBParametersResult>");
    }

    private ServiceResponse ModifyDBParameterGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBParameterGroupName");
        if (!_paramGroups.TryGetValue(name, out var pg))
            return Error("DBParameterGroupNotFoundFault", $"Parameter group {name} not found.", 404);

        var parameters = pg["Parameters"] as Dictionary<string, string> ?? new Dictionary<string, string>();
        pg["Parameters"] = parameters;

        for (var idx = 1; ; idx++)
        {
            var pName = P(p, $"Parameters.member.{idx}.ParameterName");
            if (string.IsNullOrEmpty(pName)) break;
            var pValue = P(p, $"Parameters.member.{idx}.ParameterValue");
            parameters[pName] = pValue;
        }

        return RdsXml(200, "ModifyDBParameterGroupResponse",
            $"<ModifyDBParameterGroupResult><DBParameterGroupName>{name}</DBParameterGroupName></ModifyDBParameterGroupResult>");
    }

    // ── DB Cluster Parameter Groups ───────────────────────────────────────────

    private ServiceResponse CreateDBClusterParameterGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBClusterParameterGroupName");
        if (string.IsNullOrEmpty(name))
            return Error("MissingParameter", "DBClusterParameterGroupName is required", 400);

        var family = P(p, "DBParameterGroupFamily", "aurora-postgresql15");
        var desc = P(p, "Description", name);
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:cluster-pg:{name}";

        _clusterParamGroups[name] = new Dictionary<string, object?>
        {
            ["DBClusterParameterGroupName"] = name,
            ["DBParameterGroupFamily"] = family,
            ["Description"] = desc,
            ["DBClusterParameterGroupArn"] = arn,
            ["Parameters"] = new Dictionary<string, string>(),
        };

        var reqTags = ParseTags(p);
        if (reqTags.Count > 0)
            _tags[arn] = reqTags;

        return RdsXml(200, "CreateDBClusterParameterGroupResponse",
            $"<CreateDBClusterParameterGroupResult><DBClusterParameterGroup>"
            + $"<DBClusterParameterGroupName>{name}</DBClusterParameterGroupName>"
            + $"<DBParameterGroupFamily>{family}</DBParameterGroupFamily>"
            + $"<Description>{Esc(desc)}</Description>"
            + $"<DBClusterParameterGroupArn>{arn}</DBClusterParameterGroupArn>"
            + "</DBClusterParameterGroup></CreateDBClusterParameterGroupResult>");
    }

    private ServiceResponse DescribeDBClusterParameterGroups(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBClusterParameterGroupName");
        List<Dictionary<string, object?>> groups;

        if (!string.IsNullOrEmpty(name))
        {
            if (!_clusterParamGroups.TryGetValue(name, out var pg))
                return Error("DBParameterGroupNotFoundFault", $"DB cluster parameter group {name} not found.", 404);
            groups = [pg];
        }
        else
        {
            groups = _clusterParamGroups.Values.ToList();
        }

        var members = new StringBuilder();
        foreach (var g in groups)
        {
            members.Append("<DBClusterParameterGroup>"
                + $"<DBClusterParameterGroupName>{g["DBClusterParameterGroupName"]}</DBClusterParameterGroupName>"
                + $"<DBParameterGroupFamily>{g["DBParameterGroupFamily"]}</DBParameterGroupFamily>"
                + $"<Description>{Esc((string)(g["Description"] ?? ""))}</Description>"
                + $"<DBClusterParameterGroupArn>{g["DBClusterParameterGroupArn"]}</DBClusterParameterGroupArn>"
                + "</DBClusterParameterGroup>");
        }

        return RdsXml(200, "DescribeDBClusterParameterGroupsResponse",
            $"<DescribeDBClusterParameterGroupsResult><DBClusterParameterGroups>{members}</DBClusterParameterGroups></DescribeDBClusterParameterGroupsResult>");
    }

    private ServiceResponse DeleteDBClusterParameterGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "DBClusterParameterGroupName");
        if (!_clusterParamGroups.TryRemove(name, out var pg))
            return Error("DBParameterGroupNotFoundFault", $"DB cluster parameter group {name} not found.", 404);

        var arn = (string)(pg["DBClusterParameterGroupArn"] ?? "");
        _tags.TryRemove(arn, out _);

        return RdsXml(200, "DeleteDBClusterParameterGroupResponse", "");
    }

    // ── DB Snapshots ──────────────────────────────────────────────────────────

    private ServiceResponse CreateDBSnapshot(Dictionary<string, string[]> p)
    {
        var snapId = P(p, "DBSnapshotIdentifier");
        var dbId = P(p, "DBInstanceIdentifier");
        if (string.IsNullOrEmpty(snapId))
            return Error("MissingParameter", "DBSnapshotIdentifier is required", 400);
        if (_snapshots.ContainsKey(snapId))
            return Error("DBSnapshotAlreadyExists", $"Snapshot {snapId} already exists.", 400);
        if (!_instances.TryGetValue(dbId, out var instance))
            return Error("DBInstanceNotFound", $"DBInstance {dbId} not found.", 404);

        var snap = CreateSnapshotInternal(snapId, instance);

        var reqTags = ParseTags(p);
        if (reqTags.Count > 0)
        {
            var arn = (string)(snap["DBSnapshotArn"] ?? "");
            _tags[arn] = reqTags;
            snap["TagList"] = reqTags;
        }

        return RdsXml(200, "CreateDBSnapshotResponse",
            $"<CreateDBSnapshotResult><DBSnapshot>{SnapshotXml(snap)}</DBSnapshot></CreateDBSnapshotResult>");
    }

    private ServiceResponse DescribeDBSnapshots(Dictionary<string, string[]> p)
    {
        var snapId = P(p, "DBSnapshotIdentifier");
        var dbId = P(p, "DBInstanceIdentifier");
        var snapType = P(p, "SnapshotType");
        List<Dictionary<string, object?>> snaps;

        if (!string.IsNullOrEmpty(snapId))
        {
            if (!_snapshots.TryGetValue(snapId, out var snap))
                return Error("DBSnapshotNotFound", $"Snapshot {snapId} not found.", 404);
            snaps = [snap];
        }
        else
        {
            snaps = _snapshots.Values.ToList();
            if (!string.IsNullOrEmpty(dbId))
                snaps = snaps.Where(s => (string)(s["DBInstanceIdentifier"] ?? "") == dbId).ToList();
            if (!string.IsNullOrEmpty(snapType))
                snaps = snaps.Where(s => (string)(s["SnapshotType"] ?? "") == snapType).ToList();
        }

        var members = new StringBuilder();
        foreach (var s in snaps)
            members.Append($"<DBSnapshot>{SnapshotXml(s)}</DBSnapshot>");

        return RdsXml(200, "DescribeDBSnapshotsResponse",
            $"<DescribeDBSnapshotsResult><DBSnapshots>{members}</DBSnapshots></DescribeDBSnapshotsResult>");
    }

    private ServiceResponse DeleteDBSnapshot(Dictionary<string, string[]> p)
    {
        var snapId = P(p, "DBSnapshotIdentifier");
        if (!_snapshots.TryRemove(snapId, out var snap))
            return Error("DBSnapshotNotFound", $"Snapshot {snapId} not found.", 404);

        var arn = (string)(snap["DBSnapshotArn"] ?? "");
        _tags.TryRemove(arn, out _);
        snap["Status"] = "deleted";

        return RdsXml(200, "DeleteDBSnapshotResponse",
            $"<DeleteDBSnapshotResult><DBSnapshot>{SnapshotXml(snap)}</DBSnapshot></DeleteDBSnapshotResult>");
    }

    private ServiceResponse CopyDBSnapshot(Dictionary<string, string[]> p)
    {
        var sourceSnapId = P(p, "SourceDBSnapshotIdentifier");
        var targetSnapId = P(p, "TargetDBSnapshotIdentifier");

        if (string.IsNullOrEmpty(targetSnapId))
            return Error("MissingParameter", "TargetDBSnapshotIdentifier is required", 400);
        if (_snapshots.ContainsKey(targetSnapId))
            return Error("DBSnapshotAlreadyExists", $"Snapshot {targetSnapId} already exists.", 400);

        if (!_snapshots.TryGetValue(sourceSnapId, out var sourceSnap))
            return Error("DBSnapshotNotFound", $"Snapshot {sourceSnapId} not found.", 404);

        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:snapshot:{targetSnapId}";
        var now = TimeHelpers.NowIso();

        var copy = new Dictionary<string, object?>(sourceSnap)
        {
            ["DBSnapshotIdentifier"] = targetSnapId,
            ["DBSnapshotArn"] = arn,
            ["SnapshotCreateTime"] = now,
            ["OriginalSnapshotCreateTime"] = now,
            ["SnapshotDatabaseTime"] = now,
            ["Status"] = "available",
        };
        _snapshots[targetSnapId] = copy;

        var reqTags = ParseTags(p);
        if (reqTags.Count > 0)
        {
            _tags[arn] = reqTags;
            copy["TagList"] = reqTags;
        }

        return RdsXml(200, "CopyDBSnapshotResponse",
            $"<CopyDBSnapshotResult><DBSnapshot>{SnapshotXml(copy)}</DBSnapshot></CopyDBSnapshotResult>");
    }

    // ── DB Cluster Snapshots ──────────────────────────────────────────────────

    private ServiceResponse CreateDBClusterSnapshot(Dictionary<string, string[]> p)
    {
        var snapId = P(p, "DBClusterSnapshotIdentifier");
        var clusterId = P(p, "DBClusterIdentifier");
        if (string.IsNullOrEmpty(snapId))
            return Error("MissingParameter", "DBClusterSnapshotIdentifier is required", 400);
        if (_clusterSnapshots.ContainsKey(snapId))
            return Error("DBClusterSnapshotAlreadyExistsFault", $"DB cluster snapshot {snapId} already exists.", 400);
        if (!_clusters.TryGetValue(clusterId, out var cluster))
            return Error("DBClusterNotFoundFault", $"DBCluster {clusterId} not found.", 404);

        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:cluster-snapshot:{snapId}";
        var now = TimeHelpers.NowIso();

        var clusterArn = (string)(cluster["DBClusterArn"] ?? "");
        var clusterTags = _tags.TryGetValue(clusterArn, out var ct)
            ? new List<Dictionary<string, string>>(ct) : new List<Dictionary<string, string>>();

        var snap = new Dictionary<string, object?>
        {
            ["DBClusterSnapshotIdentifier"] = snapId,
            ["DBClusterIdentifier"] = clusterId,
            ["DBClusterSnapshotArn"] = arn,
            ["Engine"] = cluster["Engine"],
            ["EngineVersion"] = cluster["EngineVersion"],
            ["SnapshotCreateTime"] = now,
            ["ClusterCreateTime"] = cluster.GetValueOrDefault("ClusterCreateTime", now),
            ["Status"] = "available",
            ["Port"] = cluster.GetValueOrDefault("Port", 5432),
            ["VpcId"] = "vpc-00000000",
            ["MasterUsername"] = cluster.GetValueOrDefault("MasterUsername", "admin"),
            ["SnapshotType"] = "manual",
            ["PercentProgress"] = 100,
            ["StorageEncrypted"] = cluster.GetValueOrDefault("StorageEncrypted", false),
            ["KmsKeyId"] = cluster.GetValueOrDefault("KmsKeyId", ""),
            ["AvailabilityZones"] = cluster.GetValueOrDefault("AvailabilityZones", new List<string>()),
            ["LicenseModel"] = LicenseModel((string)(cluster["Engine"] ?? "aurora-postgresql")),
            ["DbClusterResourceId"] = cluster.GetValueOrDefault("DbClusterResourceId", ""),
            ["IAMDatabaseAuthenticationEnabled"] = cluster.GetValueOrDefault("IAMDatabaseAuthenticationEnabled", false),
            ["AllocatedStorage"] = cluster.GetValueOrDefault("AllocatedStorage", 1),
            ["TagList"] = clusterTags,
        };
        _clusterSnapshots[snapId] = snap;

        var reqTags = ParseTags(p);
        if (reqTags.Count > 0)
        {
            _tags[arn] = reqTags;
            snap["TagList"] = reqTags;
        }

        return RdsXml(200, "CreateDBClusterSnapshotResponse",
            $"<CreateDBClusterSnapshotResult><DBClusterSnapshot>{ClusterSnapshotXml(snap)}</DBClusterSnapshot></CreateDBClusterSnapshotResult>");
    }

    private ServiceResponse DescribeDBClusterSnapshots(Dictionary<string, string[]> p)
    {
        var snapId = P(p, "DBClusterSnapshotIdentifier");
        var clusterId = P(p, "DBClusterIdentifier");
        var snapType = P(p, "SnapshotType");
        List<Dictionary<string, object?>> snaps;

        if (!string.IsNullOrEmpty(snapId))
        {
            if (!_clusterSnapshots.TryGetValue(snapId, out var snap))
                return Error("DBClusterSnapshotNotFoundFault", $"DB cluster snapshot {snapId} not found.", 404);
            snaps = [snap];
        }
        else
        {
            snaps = _clusterSnapshots.Values.ToList();
            if (!string.IsNullOrEmpty(clusterId))
                snaps = snaps.Where(s => (string)(s["DBClusterIdentifier"] ?? "") == clusterId).ToList();
            if (!string.IsNullOrEmpty(snapType))
                snaps = snaps.Where(s => (string)(s["SnapshotType"] ?? "") == snapType).ToList();
        }

        var members = new StringBuilder();
        foreach (var s in snaps)
            members.Append($"<DBClusterSnapshot>{ClusterSnapshotXml(s)}</DBClusterSnapshot>");

        return RdsXml(200, "DescribeDBClusterSnapshotsResponse",
            $"<DescribeDBClusterSnapshotsResult><DBClusterSnapshots>{members}</DBClusterSnapshots></DescribeDBClusterSnapshotsResult>");
    }

    private ServiceResponse DeleteDBClusterSnapshot(Dictionary<string, string[]> p)
    {
        var snapId = P(p, "DBClusterSnapshotIdentifier");
        if (!_clusterSnapshots.TryRemove(snapId, out var snap))
            return Error("DBClusterSnapshotNotFoundFault", $"DB cluster snapshot {snapId} not found.", 404);

        var arn = (string)(snap["DBClusterSnapshotArn"] ?? "");
        _tags.TryRemove(arn, out _);
        snap["Status"] = "deleted";

        return RdsXml(200, "DeleteDBClusterSnapshotResponse",
            $"<DeleteDBClusterSnapshotResult><DBClusterSnapshot>{ClusterSnapshotXml(snap)}</DBClusterSnapshot></DeleteDBClusterSnapshotResult>");
    }

    // ── Tags ──────────────────────────────────────────────────────────────────

    private ServiceResponse AddTagsToResource(Dictionary<string, string[]> p)
    {
        var arn = P(p, "ResourceName");
        if (string.IsNullOrEmpty(arn))
            return Error("MissingParameter", "ResourceName is required", 400);

        var newTags = ParseTags(p);
        if (!_tags.TryGetValue(arn, out var existing))
        {
            existing = new List<Dictionary<string, string>>();
            _tags[arn] = existing;
        }

        var existingKeys = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < existing.Count; i++)
            existingKeys[existing[i]["Key"]] = i;

        foreach (var tag in newTags)
        {
            if (existingKeys.TryGetValue(tag["Key"], out var idx))
                existing[idx] = tag;
            else
            {
                existingKeys[tag["Key"]] = existing.Count;
                existing.Add(tag);
            }
        }

        SyncTagListToResource(arn);
        return RdsXml(200, "AddTagsToResourceResponse", "");
    }

    private ServiceResponse ListTagsForResource(Dictionary<string, string[]> p)
    {
        var arn = P(p, "ResourceName");
        if (string.IsNullOrEmpty(arn))
            return RdsXml(200, "ListTagsForResourceResponse",
                "<ListTagsForResourceResult><TagList/></ListTagsForResourceResult>");

        var tagList = _tags.TryGetValue(arn, out var tags) ? tags : new List<Dictionary<string, string>>();
        var members = new StringBuilder();
        foreach (var t in tagList)
            members.Append($"<Tag><Key>{Esc(t["Key"])}</Key><Value>{Esc(t["Value"])}</Value></Tag>");

        return RdsXml(200, "ListTagsForResourceResponse",
            $"<ListTagsForResourceResult><TagList>{members}</TagList></ListTagsForResourceResult>");
    }

    private ServiceResponse RemoveTagsFromResource(Dictionary<string, string[]> p)
    {
        var arn = P(p, "ResourceName");
        if (string.IsNullOrEmpty(arn))
            return Error("MissingParameter", "ResourceName is required", 400);

        var keysToRemove = ParseMemberList(p, "TagKeys").ToHashSet(StringComparer.Ordinal);

        if (_tags.TryGetValue(arn, out var existing))
            _tags[arn] = existing.Where(t => !keysToRemove.Contains(t["Key"])).ToList();

        SyncTagListToResource(arn);
        return RdsXml(200, "RemoveTagsFromResourceResponse", "");
    }

    // ── Event Subscriptions ───────────────────────────────────────────────────

    private ServiceResponse CreateEventSubscription(Dictionary<string, string[]> p)
    {
        var subName = P(p, "SubscriptionName");
        if (string.IsNullOrEmpty(subName))
            return Error("MissingParameter", "SubscriptionName is required", 400);
        if (_eventSubscriptions.ContainsKey(subName))
            return Error("SubscriptionAlreadyExistFault", $"Subscription {subName} already exists.", 400);

        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:es:{subName}";
        var snsTopicArn = P(p, "SnsTopicArn");

        var sub = new Dictionary<string, object?>
        {
            ["SubscriptionName"] = subName,
            ["EventSubscriptionArn"] = arn,
            ["SnsTopicArn"] = snsTopicArn,
            ["Status"] = "active",
            ["SourceType"] = P(p, "SourceType"),
            ["Enabled"] = P(p, "Enabled") != "false",
            ["CustomerAwsId"] = accountId,
            ["CustSubscriptionId"] = subName,
        };
        _eventSubscriptions[subName] = sub;

        return RdsXml(200, "CreateEventSubscriptionResponse",
            $"<CreateEventSubscriptionResult><EventSubscription>{EventSubscriptionXml(sub)}</EventSubscription></CreateEventSubscriptionResult>");
    }

    private ServiceResponse DescribeEventSubscriptions(Dictionary<string, string[]> p)
    {
        var subName = P(p, "SubscriptionName");
        List<Dictionary<string, object?>> subs;

        if (!string.IsNullOrEmpty(subName))
        {
            if (!_eventSubscriptions.TryGetValue(subName, out var sub))
                return Error("SubscriptionNotFoundFault", $"Subscription {subName} not found.", 404);
            subs = [sub];
        }
        else
        {
            subs = _eventSubscriptions.Values.ToList();
        }

        var members = new StringBuilder();
        foreach (var s in subs)
            members.Append($"<EventSubscription>{EventSubscriptionXml(s)}</EventSubscription>");

        return RdsXml(200, "DescribeEventSubscriptionsResponse",
            $"<DescribeEventSubscriptionsResult><EventSubscriptionsList>{members}</EventSubscriptionsList></DescribeEventSubscriptionsResult>");
    }

    private ServiceResponse DeleteEventSubscription(Dictionary<string, string[]> p)
    {
        var subName = P(p, "SubscriptionName");
        if (!_eventSubscriptions.TryRemove(subName, out var sub))
            return Error("SubscriptionNotFoundFault", $"Subscription {subName} not found.", 404);

        return RdsXml(200, "DeleteEventSubscriptionResponse",
            $"<DeleteEventSubscriptionResult><EventSubscription>{EventSubscriptionXml(sub)}</EventSubscription></DeleteEventSubscriptionResult>");
    }

    // ── DB Proxy ──────────────────────────────────────────────────────────────

    private ServiceResponse CreateDBProxy(Dictionary<string, string[]> p)
    {
        var proxyName = P(p, "DBProxyName");
        if (string.IsNullOrEmpty(proxyName))
            return Error("MissingParameter", "DBProxyName is required", 400);
        if (_dbProxies.ContainsKey(proxyName))
            return Error("DBProxyAlreadyExistsFault", $"DB proxy {proxyName} already exists.", 400);

        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:db-proxy:prx-{Guid.NewGuid():N}"[..60];
        var engineFamily = P(p, "EngineFamily", "POSTGRESQL");
        var roleArn = P(p, "RoleArn");

        var proxy = new Dictionary<string, object?>
        {
            ["DBProxyName"] = proxyName,
            ["DBProxyArn"] = arn,
            ["EngineFamily"] = engineFamily,
            ["Status"] = "available",
            ["RoleArn"] = roleArn,
            ["VpcId"] = "vpc-00000000",
            ["Endpoint"] = $"{proxyName}.proxy-{Guid.NewGuid():N}"[..40] + $".{Region}.rds.amazonaws.com",
            ["RequireTLS"] = P(p, "RequireTLS") == "true",
            ["IdleClientTimeout"] = int.Parse(P(p, "IdleClientTimeout", "1800")),
            ["DebugLogging"] = P(p, "DebugLogging") == "true",
        };
        _dbProxies[proxyName] = proxy;

        return RdsXml(200, "CreateDBProxyResponse",
            $"<CreateDBProxyResult><DBProxy>{DbProxyXml(proxy)}</DBProxy></CreateDBProxyResult>");
    }

    private ServiceResponse DescribeDBProxies(Dictionary<string, string[]> p)
    {
        var proxyName = P(p, "DBProxyName");
        List<Dictionary<string, object?>> proxies;

        if (!string.IsNullOrEmpty(proxyName))
        {
            if (!_dbProxies.TryGetValue(proxyName, out var proxy))
                return Error("DBProxyNotFoundFault", $"DB proxy {proxyName} not found.", 404);
            proxies = [proxy];
        }
        else
        {
            proxies = _dbProxies.Values.ToList();
        }

        var members = new StringBuilder();
        foreach (var px in proxies)
            members.Append($"<member>{DbProxyXml(px)}</member>");

        return RdsXml(200, "DescribeDBProxiesResponse",
            $"<DescribeDBProxiesResult><DBProxies>{members}</DBProxies></DescribeDBProxiesResult>");
    }

    private ServiceResponse DeleteDBProxy(Dictionary<string, string[]> p)
    {
        var proxyName = P(p, "DBProxyName");
        if (!_dbProxies.TryRemove(proxyName, out var proxy))
            return Error("DBProxyNotFoundFault", $"DB proxy {proxyName} not found.", 404);

        return RdsXml(200, "DeleteDBProxyResponse",
            $"<DeleteDBProxyResult><DBProxy>{DbProxyXml(proxy)}</DBProxy></DeleteDBProxyResult>");
    }

    // ── Describe helpers ──────────────────────────────────────────────────────

    private ServiceResponse DescribeOrderableDBInstanceOptions(Dictionary<string, string[]> p)
    {
        var engine = P(p, "Engine", "postgres");
        var engineVersion = P(p, "EngineVersion");
        var dbClass = P(p, "DBInstanceClass");

        var instanceClasses = new[]
        {
            "db.t3.micro", "db.t3.small", "db.t3.medium", "db.t3.large",
            "db.r5.large", "db.r5.xlarge", "db.r5.2xlarge",
            "db.m5.large", "db.m5.xlarge", "db.m5.2xlarge",
        };
        var version = string.IsNullOrEmpty(engineVersion) ? DefaultEngineVersion(engine) : engineVersion;

        var members = new StringBuilder();
        foreach (var cls in instanceClasses)
        {
            if (!string.IsNullOrEmpty(dbClass) && cls != dbClass)
                continue;

            members.Append("<OrderableDBInstanceOption>"
                + $"<Engine>{engine}</Engine>"
                + $"<EngineVersion>{version}</EngineVersion>"
                + $"<DBInstanceClass>{cls}</DBInstanceClass>"
                + $"<LicenseModel>{LicenseModel(engine)}</LicenseModel>"
                + "<AvailabilityZones>"
                + $"<AvailabilityZone><Name>{Region}a</Name></AvailabilityZone>"
                + $"<AvailabilityZone><Name>{Region}b</Name></AvailabilityZone>"
                + "</AvailabilityZones>"
                + "<MultiAZCapable>true</MultiAZCapable>"
                + "<ReadReplicaCapable>true</ReadReplicaCapable>"
                + "<Vpc>true</Vpc>"
                + "<SupportsStorageEncryption>true</SupportsStorageEncryption>"
                + "<StorageType>gp2</StorageType>"
                + "<SupportsIops>false</SupportsIops>"
                + "<SupportsEnhancedMonitoring>true</SupportsEnhancedMonitoring>"
                + "<SupportsIAMDatabaseAuthentication>true</SupportsIAMDatabaseAuthentication>"
                + "<SupportsPerformanceInsights>true</SupportsPerformanceInsights>"
                + "<AvailableProcessorFeatures/>"
                + "<SupportedEngineModes><member>provisioned</member></SupportedEngineModes>"
                + "<SupportsStorageAutoscaling>true</SupportsStorageAutoscaling>"
                + "<SupportsKerberosAuthentication>false</SupportsKerberosAuthentication>"
                + "<OutpostCapable>false</OutpostCapable>"
                + "<SupportedNetworkTypes><member>IPV4</member></SupportedNetworkTypes>"
                + "<SupportsGlobalDatabases>false</SupportsGlobalDatabases>"
                + "<SupportsClusters>false</SupportsClusters>"
                + "<SupportedActivityStreamModes/>"
                + "</OrderableDBInstanceOption>");
        }

        return RdsXml(200, "DescribeOrderableDBInstanceOptionsResponse",
            $"<DescribeOrderableDBInstanceOptionsResult><OrderableDBInstanceOptions>{members}</OrderableDBInstanceOptions></DescribeOrderableDBInstanceOptionsResult>");
    }

    private ServiceResponse DescribeEngineDefaultClusterParameters(Dictionary<string, string[]> p)
    {
        var family = P(p, "DBParameterGroupFamily", "aurora-postgresql15");
        return RdsXml(200, "DescribeEngineDefaultClusterParametersResponse",
            $"<DescribeEngineDefaultClusterParametersResult><EngineDefaults>"
            + $"<DBParameterGroupFamily>{family}</DBParameterGroupFamily>"
            + "<Parameters/>"
            + "</EngineDefaults></DescribeEngineDefaultClusterParametersResult>");
    }

    private ServiceResponse DescribeDBEngineVersions(Dictionary<string, string[]> p)
    {
        var engine = P(p, "Engine", "postgres");
        var versionFilter = P(p, "EngineVersion");

        var versionsMap = new Dictionary<string, List<(string Version, string Family)>>(StringComparer.Ordinal)
        {
            ["postgres"] = [("15.3", "15"), ("14.8", "14"), ("13.11", "13"), ("12.15", "12")],
            ["mysql"] = [("8.0.33", "8.0"), ("8.0.28", "8.0"), ("5.7.43", "5.7")],
            ["mariadb"] = [("10.6.14", "10.6"), ("10.5.21", "10.5")],
            ["aurora-postgresql"] = [("15.3", "aurora-postgresql15"), ("14.8", "aurora-postgresql14")],
            ["aurora-mysql"] = [("8.0.mysql_aurora.3.03.0", "aurora-mysql8.0")],
        };

        var versions = versionsMap.GetValueOrDefault(engine) ?? [("15.3", "15")];
        var members = new StringBuilder();

        foreach (var (ver, family) in versions)
        {
            if (!string.IsNullOrEmpty(versionFilter) && ver != versionFilter)
                continue;

            var engineDesc = engine.Replace("-", " ", StringComparison.Ordinal);
            // Title-case the description
            engineDesc = System.Globalization.CultureInfo.InvariantCulture.TextInfo.ToTitleCase(engineDesc);

            members.Append("<DBEngineVersion>"
                + $"<Engine>{engine}</Engine>"
                + $"<EngineVersion>{ver}</EngineVersion>"
                + $"<DBParameterGroupFamily>{engine}{family}</DBParameterGroupFamily>"
                + $"<DBEngineDescription>{engineDesc}</DBEngineDescription>"
                + $"<DBEngineVersionDescription>{engine} {ver}</DBEngineVersionDescription>"
                + "<ValidUpgradeTarget/>"
                + "<ExportableLogTypes/>"
                + "<SupportsLogExportsToCloudwatchLogs>false</SupportsLogExportsToCloudwatchLogs>"
                + "<SupportsReadReplica>true</SupportsReadReplica>"
                + "<SupportedFeatureNames/>"
                + "<Status>available</Status>"
                + "<SupportsParallelQuery>false</SupportsParallelQuery>"
                + "<SupportsGlobalDatabases>false</SupportsGlobalDatabases>"
                + "<SupportsBabelfish>false</SupportsBabelfish>"
                + "<SupportsCertificateRotationWithoutRestart>true</SupportsCertificateRotationWithoutRestart>"
                + "</DBEngineVersion>");
        }

        return RdsXml(200, "DescribeDBEngineVersionsResponse",
            $"<DescribeDBEngineVersionsResult><DBEngineVersions>{members}</DBEngineVersions></DescribeDBEngineVersionsResult>");
    }

    // ── Option Groups ─────────────────────────────────────────────────────────

    private ServiceResponse CreateOptionGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "OptionGroupName");
        if (string.IsNullOrEmpty(name))
            return Error("MissingParameter", "OptionGroupName is required", 400);
        if (_optionGroups.ContainsKey(name))
            return Error("OptionGroupAlreadyExistsFault", $"Option group {name} already exists.", 400);

        var engineName = P(p, "EngineName", "mysql");
        var majorVersion = P(p, "MajorEngineVersion", "8.0");
        var desc = P(p, "OptionGroupDescription", name);
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:og:{name}";

        _optionGroups[name] = new Dictionary<string, object?>
        {
            ["OptionGroupName"] = name,
            ["OptionGroupDescription"] = desc,
            ["EngineName"] = engineName,
            ["MajorEngineVersion"] = majorVersion,
            ["Options"] = new List<Dictionary<string, object?>>(),
            ["AllowsVpcAndNonVpcInstanceMemberships"] = true,
            ["OptionGroupArn"] = arn,
        };

        return RdsXml(200, "CreateOptionGroupResponse",
            $"<CreateOptionGroupResult><OptionGroup>{OptionGroupXml(_optionGroups[name])}</OptionGroup></CreateOptionGroupResult>");
    }

    private ServiceResponse DescribeOptionGroups(Dictionary<string, string[]> p)
    {
        var name = P(p, "OptionGroupName");
        List<Dictionary<string, object?>> groups;

        if (!string.IsNullOrEmpty(name))
        {
            if (!_optionGroups.TryGetValue(name, out var og))
                return Error("OptionGroupNotFoundFault", $"Option group {name} not found.", 404);
            groups = [og];
        }
        else
        {
            groups = _optionGroups.Values.ToList();
        }

        var members = new StringBuilder();
        foreach (var g in groups)
            members.Append($"<OptionGroup>{OptionGroupXml(g)}</OptionGroup>");

        return RdsXml(200, "DescribeOptionGroupsResponse",
            $"<DescribeOptionGroupsResult><OptionGroupsList>{members}</OptionGroupsList></DescribeOptionGroupsResult>");
    }

    private ServiceResponse DeleteOptionGroup(Dictionary<string, string[]> p)
    {
        var name = P(p, "OptionGroupName");
        if (!_optionGroups.TryRemove(name, out _))
            return Error("OptionGroupNotFoundFault", $"Option group {name} not found.", 404);

        return RdsXml(200, "DeleteOptionGroupResponse", "");
    }

    // ── Global Clusters ───────────────────────────────────────────────────────

    private ServiceResponse CreateGlobalCluster(Dictionary<string, string[]> p)
    {
        var gcId = P(p, "GlobalClusterIdentifier");
        if (string.IsNullOrEmpty(gcId))
            return Error("MissingParameter", "GlobalClusterIdentifier is required", 400);
        if (_globalClusters.ContainsKey(gcId))
            return Error("GlobalClusterAlreadyExistsFault", $"Global cluster {gcId} already exists.", 400);

        var engine = P(p, "Engine");
        var engineVersion = P(p, "EngineVersion");
        var sourceClusterId = P(p, "SourceDBClusterIdentifier");
        var gcMembers = new List<Dictionary<string, object?>>();

        if (!string.IsNullOrEmpty(sourceClusterId))
        {
            if (!_clusters.TryGetValue(sourceClusterId, out var sourceCluster))
                return Error("DBClusterNotFoundFault", $"DBCluster {sourceClusterId} not found.", 404);
            if (string.IsNullOrEmpty(engine))
                engine = (string)(sourceCluster["Engine"] ?? "aurora-postgresql");
            if (string.IsNullOrEmpty(engineVersion))
                engineVersion = (string)(sourceCluster["EngineVersion"] ?? "");
            var memberArn = (string)(sourceCluster["DBClusterArn"] ?? "");
            gcMembers.Add(new Dictionary<string, object?>
            {
                ["DBClusterArn"] = memberArn,
                ["IsWriter"] = true,
                ["Readers"] = new List<string>(),
                ["GlobalWriteForwardingStatus"] = "disabled",
            });
        }

        if (string.IsNullOrEmpty(engine))
            engine = "aurora-postgresql";
        if (string.IsNullOrEmpty(engineVersion))
            engineVersion = DefaultEngineVersion(engine);

        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds::{accountId}:global-cluster:{gcId}";
        var resourceId = $"cluster-{Guid.NewGuid():N}"[..28].ToUpperInvariant();

        var gc = new Dictionary<string, object?>
        {
            ["GlobalClusterIdentifier"] = gcId,
            ["GlobalClusterArn"] = arn,
            ["GlobalClusterResourceId"] = resourceId,
            ["Engine"] = engine,
            ["EngineVersion"] = engineVersion,
            ["Status"] = "available",
            ["DeletionProtection"] = P(p, "DeletionProtection") == "true",
            ["StorageEncrypted"] = P(p, "StorageEncrypted") == "true",
            ["GlobalClusterMembers"] = gcMembers,
        };
        _globalClusters[gcId] = gc;

        return RdsXml(200, "CreateGlobalClusterResponse",
            $"<CreateGlobalClusterResult><GlobalCluster>{GlobalClusterXml(gc)}</GlobalCluster></CreateGlobalClusterResult>");
    }

    private ServiceResponse DescribeGlobalClusters(Dictionary<string, string[]> p)
    {
        var gcId = P(p, "GlobalClusterIdentifier");
        List<Dictionary<string, object?>> clusters;

        if (!string.IsNullOrEmpty(gcId))
        {
            if (!_globalClusters.TryGetValue(gcId, out var gc))
                return Error("GlobalClusterNotFoundFault", $"Global cluster {gcId} not found.", 404);
            clusters = [gc];
        }
        else
        {
            clusters = _globalClusters.Values.ToList();
        }

        var members = new StringBuilder();
        foreach (var c in clusters)
            members.Append($"<GlobalClusterMember>{GlobalClusterXml(c)}</GlobalClusterMember>");

        return RdsXml(200, "DescribeGlobalClustersResponse",
            $"<DescribeGlobalClustersResult><GlobalClusters>{members}</GlobalClusters></DescribeGlobalClustersResult>");
    }

    private ServiceResponse ModifyGlobalCluster(Dictionary<string, string[]> p)
    {
        var gcId = P(p, "GlobalClusterIdentifier");
        if (!_globalClusters.TryGetValue(gcId, out var gc))
            return Error("GlobalClusterNotFoundFault", $"Global cluster {gcId} not found.", 404);

        var val = P(p, "DeletionProtection");
        if (!string.IsNullOrEmpty(val))
            gc["DeletionProtection"] = val == "true";

        var newId = P(p, "NewGlobalClusterIdentifier");
        if (!string.IsNullOrEmpty(newId) && newId != gcId)
        {
            _globalClusters.TryRemove(gcId, out _);
            gc["GlobalClusterIdentifier"] = newId;
            _globalClusters[newId] = gc;
        }

        return RdsXml(200, "ModifyGlobalClusterResponse",
            $"<ModifyGlobalClusterResult><GlobalCluster>{GlobalClusterXml(gc)}</GlobalCluster></ModifyGlobalClusterResult>");
    }

    private ServiceResponse DeleteGlobalCluster(Dictionary<string, string[]> p)
    {
        var gcId = P(p, "GlobalClusterIdentifier");
        if (!_globalClusters.TryGetValue(gcId, out var gc))
            return Error("GlobalClusterNotFoundFault", $"Global cluster {gcId} not found.", 404);

        if (gc.TryGetValue("DeletionProtection", out var dp) && dp is true)
            return Error("InvalidParameterCombination",
                "Cannot delete a global cluster when DeletionProtection is enabled.", 400);

        if (gc.GetValueOrDefault("GlobalClusterMembers") is List<Dictionary<string, object?>> members && members.Count > 0)
        {
            var hasWriter = members.Any(m => m.GetValueOrDefault("IsWriter") is true);
            if (hasWriter)
                return Error("InvalidGlobalClusterStateFault",
                    "Global cluster has active members. Remove them before deleting.", 400);
        }

        _globalClusters.TryRemove(gcId, out _);

        return RdsXml(200, "DeleteGlobalClusterResponse",
            $"<DeleteGlobalClusterResult><GlobalCluster>{GlobalClusterXml(gc)}</GlobalCluster></DeleteGlobalClusterResult>");
    }

    private ServiceResponse RemoveFromGlobalCluster(Dictionary<string, string[]> p)
    {
        var gcId = P(p, "GlobalClusterIdentifier");
        if (!_globalClusters.TryGetValue(gcId, out var gc))
            return Error("GlobalClusterNotFoundFault", $"Global cluster {gcId} not found.", 404);

        var dbClusterId = P(p, "DbClusterIdentifier");
        if (gc.GetValueOrDefault("GlobalClusterMembers") is List<Dictionary<string, object?>> members)
        {
            // Match by ARN or identifier
            members.RemoveAll(m =>
            {
                var memberArn = (string)(m.GetValueOrDefault("DBClusterArn") ?? "");
                return memberArn == dbClusterId || memberArn.EndsWith($":{dbClusterId}", StringComparison.Ordinal);
            });
        }

        return RdsXml(200, "RemoveFromGlobalClusterResponse",
            $"<RemoveFromGlobalClusterResult><GlobalCluster>{GlobalClusterXml(gc)}</GlobalCluster></RemoveFromGlobalClusterResult>");
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private Dictionary<string, object?> CreateSnapshotInternal(string snapId, Dictionary<string, object?> instance)
    {
        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:rds:{Region}:{accountId}:snapshot:{snapId}";
        var now = TimeHelpers.NowIso();

        var ep = instance.GetValueOrDefault("Endpoint") as Dictionary<string, object?>;
        var instArn = (string)(instance.GetValueOrDefault("DBInstanceArn") ?? "");
        var tagList = _tags.TryGetValue(instArn, out var instTags)
            ? new List<Dictionary<string, string>>(instTags) : new List<Dictionary<string, string>>();

        var snap = new Dictionary<string, object?>
        {
            ["DBSnapshotIdentifier"] = snapId,
            ["DBInstanceIdentifier"] = instance["DBInstanceIdentifier"],
            ["DBSnapshotArn"] = arn,
            ["Engine"] = instance["Engine"],
            ["EngineVersion"] = instance["EngineVersion"],
            ["SnapshotCreateTime"] = now,
            ["InstanceCreateTime"] = instance.GetValueOrDefault("InstanceCreateTime", now),
            ["Status"] = "available",
            ["AllocatedStorage"] = instance.GetValueOrDefault("AllocatedStorage", 20),
            ["AvailabilityZone"] = instance.GetValueOrDefault("AvailabilityZone", $"{Region}a"),
            ["VpcId"] = "vpc-00000000",
            ["Port"] = ep?.GetValueOrDefault("Port", 5432) ?? 5432,
            ["MasterUsername"] = instance.GetValueOrDefault("MasterUsername", "admin"),
            ["DBName"] = instance.GetValueOrDefault("DBName", ""),
            ["SnapshotType"] = "manual",
            ["LicenseModel"] = instance.GetValueOrDefault("LicenseModel", "general-public-license"),
            ["StorageType"] = instance.GetValueOrDefault("StorageType", "gp2"),
            ["DBInstanceClass"] = instance.GetValueOrDefault("DBInstanceClass", "db.t3.micro"),
            ["StorageEncrypted"] = instance.GetValueOrDefault("StorageEncrypted", false),
            ["KmsKeyId"] = instance.GetValueOrDefault("KmsKeyId", ""),
            ["Encrypted"] = instance.GetValueOrDefault("StorageEncrypted", false),
            ["IAMDatabaseAuthenticationEnabled"] = instance.GetValueOrDefault("IAMDatabaseAuthenticationEnabled", false),
            ["PercentProgress"] = 100,
            ["DbiResourceId"] = instance.GetValueOrDefault("DbiResourceId", ""),
            ["TagList"] = tagList,
            ["OriginalSnapshotCreateTime"] = now,
            ["SnapshotDatabaseTime"] = now,
            ["SnapshotTarget"] = "region",
        };
        _snapshots[snapId] = snap;
        return snap;
    }

    private void SyncTagListToResource(string arn)
    {
        var tagList = _tags.TryGetValue(arn, out var tags)
            ? new List<Dictionary<string, string>>(tags) : new List<Dictionary<string, string>>();

        foreach (var inst in _instances.Values)
        {
            if ((string)(inst["DBInstanceArn"] ?? "") == arn)
            {
                inst["TagList"] = tagList;
                return;
            }
        }

        foreach (var cl in _clusters.Values)
        {
            if ((string)(cl["DBClusterArn"] ?? "") == arn)
            {
                cl["TagList"] = tagList;
                return;
            }
        }

        foreach (var snap in _snapshots.Values)
        {
            if ((string)(snap["DBSnapshotArn"] ?? "") == arn)
            {
                snap["TagList"] = tagList;
                return;
            }
        }
    }

    private static void ApplyModifyField(
        Dictionary<string, string[]> p,
        Dictionary<string, object?> instance,
        Dictionary<string, object?> pending,
        string paramKey,
        string instanceKey,
        bool applyImmediately,
        bool isInt)
    {
        var val = P(p, paramKey);
        if (string.IsNullOrEmpty(val)) return;

        object parsed = isInt ? int.Parse(val) : val;
        if (applyImmediately)
            instance[instanceKey] = parsed;
        else
            pending[instanceKey] = parsed;
    }

    private static void ApplyModifyBoolField(
        Dictionary<string, string[]> p,
        Dictionary<string, object?> instance,
        Dictionary<string, object?> pending,
        string paramKey,
        string instanceKey,
        bool applyImmediately)
    {
        var val = P(p, paramKey);
        if (string.IsNullOrEmpty(val)) return;

        var boolVal = val == "true";
        if (applyImmediately)
            instance[instanceKey] = boolVal;
        else
            pending[instanceKey] = boolVal;
    }

    private static List<Dictionary<string, object?>> ApplyInstanceFilters(
        List<Dictionary<string, object?>> instances,
        Dictionary<string, List<string>> filters)
    {
        var result = new List<Dictionary<string, object?>>();
        foreach (var inst in instances)
        {
            var match = true;
            foreach (var (fname, fvals) in filters)
            {
                if (fname == "db-instance-id")
                {
                    if (!fvals.Contains((string)(inst["DBInstanceIdentifier"] ?? "")))
                        match = false;
                }
                else if (fname == "engine")
                {
                    if (!fvals.Contains((string)(inst["Engine"] ?? "")))
                        match = false;
                }
                else if (fname == "db-cluster-id")
                {
                    if (!fvals.Contains((string)(inst["DBClusterIdentifier"] ?? "")))
                        match = false;
                }
            }
            if (match) result.Add(inst);
        }
        return result;
    }

    private static List<Dictionary<string, object?>> ApplyClusterFilters(
        List<Dictionary<string, object?>> clusters,
        Dictionary<string, List<string>> filters)
    {
        var result = new List<Dictionary<string, object?>>();
        foreach (var cl in clusters)
        {
            var match = true;
            foreach (var (fname, fvals) in filters)
            {
                if (fname == "db-cluster-id")
                {
                    if (!fvals.Contains((string)(cl["DBClusterIdentifier"] ?? "")))
                        match = false;
                }
                else if (fname == "engine")
                {
                    if (!fvals.Contains((string)(cl["Engine"] ?? "")))
                        match = false;
                }
            }
            if (match) result.Add(cl);
        }
        return result;
    }

    // ── XML serializers ───────────────────────────────────────────────────────

    private static string InstanceXml(Dictionary<string, object?> i)
    {
        var ep = i.GetValueOrDefault("Endpoint") as Dictionary<string, object?>;
        var subnet = i.GetValueOrDefault("DBSubnetGroup") as Dictionary<string, object?> ?? new Dictionary<string, object?>();

        var vpcSgXml = new StringBuilder();
        if (i.GetValueOrDefault("VpcSecurityGroups") is List<Dictionary<string, object?>> vpcSgs)
        {
            foreach (var sg in vpcSgs)
            {
                vpcSgXml.Append("<VpcSecurityGroupMembership>"
                    + $"<VpcSecurityGroupId>{sg.GetValueOrDefault("VpcSecurityGroupId", "")}</VpcSecurityGroupId>"
                    + $"<Status>{sg.GetValueOrDefault("Status", "active")}</Status>"
                    + "</VpcSecurityGroupMembership>");
            }
        }

        var paramXml = new StringBuilder();
        if (i.GetValueOrDefault("DBParameterGroups") is List<Dictionary<string, object?>> pGroups)
        {
            foreach (var pg in pGroups)
            {
                paramXml.Append("<DBParameterGroup>"
                    + $"<DBParameterGroupName>{pg.GetValueOrDefault("DBParameterGroupName", "")}</DBParameterGroupName>"
                    + $"<ParameterApplyStatus>{pg.GetValueOrDefault("ParameterApplyStatus", "in-sync")}</ParameterApplyStatus>"
                    + "</DBParameterGroup>");
            }
        }

        var optionXml = new StringBuilder();
        if (i.GetValueOrDefault("OptionGroupMemberships") is List<Dictionary<string, object?>> opts)
        {
            foreach (var og in opts)
            {
                optionXml.Append("<OptionGroupMembership>"
                    + $"<OptionGroupName>{og.GetValueOrDefault("OptionGroupName", "")}</OptionGroupName>"
                    + $"<Status>{og.GetValueOrDefault("Status", "in-sync")}</Status>"
                    + "</OptionGroupMembership>");
            }
        }

        var tagXml = new StringBuilder();
        if (i.GetValueOrDefault("TagList") is List<Dictionary<string, string>> tags)
        {
            foreach (var t in tags)
                tagXml.Append($"<Tag><Key>{Esc(t["Key"])}</Key><Value>{Esc(t["Value"])}</Value></Tag>");
        }

        var subnetXml = new StringBuilder();
        if (subnet.GetValueOrDefault("Subnets") is List<Dictionary<string, object?>> subnets)
        {
            foreach (var s in subnets)
            {
                var az = s.GetValueOrDefault("SubnetAvailabilityZone") is Dictionary<string, object?> azDict
                    ? (string)(azDict.GetValueOrDefault("Name") ?? $"{Region}a")
                    : $"{Region}a";
                subnetXml.Append("<Subnet>"
                    + $"<SubnetIdentifier>{s.GetValueOrDefault("SubnetIdentifier", "")}</SubnetIdentifier>"
                    + $"<SubnetAvailabilityZone><Name>{az}</Name></SubnetAvailabilityZone>"
                    + "<SubnetOutpost/>"
                    + "<SubnetStatus>Active</SubnetStatus>"
                    + "</Subnet>");
            }
        }

        var pendingXml = new StringBuilder();
        if (i.GetValueOrDefault("PendingModifiedValues") is Dictionary<string, object?> pend)
        {
            foreach (var (pk, pv) in pend)
                pendingXml.Append($"<{pk}>{pv}</{pk}>");
        }

        var iopsXml = "";
        if (i.GetValueOrDefault("Iops") is int iops)
            iopsXml = $"<Iops>{iops}</Iops>";

        var certXml = "";
        if (i.GetValueOrDefault("CertificateDetails") is Dictionary<string, object?> cert)
        {
            certXml = "<CertificateDetails>"
                + $"<CAIdentifier>{cert.GetValueOrDefault("CAIdentifier", "")}</CAIdentifier>"
                + $"<ValidTill>{cert.GetValueOrDefault("ValidTill", "")}</ValidTill>"
                + "</CertificateDetails>";
        }

        return $"<DBInstanceIdentifier>{i["DBInstanceIdentifier"]}</DBInstanceIdentifier>"
            + $"<DBInstanceClass>{i["DBInstanceClass"]}</DBInstanceClass>"
            + $"<Engine>{i["Engine"]}</Engine>"
            + $"<EngineVersion>{i["EngineVersion"]}</EngineVersion>"
            + $"<DBInstanceStatus>{i["DBInstanceStatus"]}</DBInstanceStatus>"
            + $"<MasterUsername>{i["MasterUsername"]}</MasterUsername>"
            + $"<DBName>{i.GetValueOrDefault("DBName", "")}</DBName>"
            + "<Endpoint>"
            + $"<Address>{ep?.GetValueOrDefault("Address", "localhost") ?? "localhost"}</Address>"
            + $"<Port>{ep?.GetValueOrDefault("Port", 5432) ?? 5432}</Port>"
            + $"<HostedZoneId>{ep?.GetValueOrDefault("HostedZoneId", "Z2R2ITUGPM61AM") ?? "Z2R2ITUGPM61AM"}</HostedZoneId>"
            + "</Endpoint>"
            + $"<AllocatedStorage>{i["AllocatedStorage"]}</AllocatedStorage>"
            + $"<InstanceCreateTime>{i.GetValueOrDefault("InstanceCreateTime", "")}</InstanceCreateTime>"
            + $"<PreferredBackupWindow>{i.GetValueOrDefault("PreferredBackupWindow", "03:00-04:00")}</PreferredBackupWindow>"
            + $"<BackupRetentionPeriod>{i.GetValueOrDefault("BackupRetentionPeriod", 1)}</BackupRetentionPeriod>"
            + $"<DBSecurityGroups/>"
            + $"<VpcSecurityGroups>{vpcSgXml}</VpcSecurityGroups>"
            + $"<DBParameterGroups>{paramXml}</DBParameterGroups>"
            + $"<AvailabilityZone>{i.GetValueOrDefault("AvailabilityZone", $"{Region}a")}</AvailabilityZone>"
            + "<DBSubnetGroup>"
            + $"<DBSubnetGroupName>{subnet.GetValueOrDefault("DBSubnetGroupName", "default")}</DBSubnetGroupName>"
            + $"<DBSubnetGroupDescription>{subnet.GetValueOrDefault("DBSubnetGroupDescription", "")}</DBSubnetGroupDescription>"
            + $"<VpcId>{subnet.GetValueOrDefault("VpcId", "vpc-00000000")}</VpcId>"
            + $"<SubnetGroupStatus>{subnet.GetValueOrDefault("SubnetGroupStatus", "Complete")}</SubnetGroupStatus>"
            + $"<Subnets>{subnetXml}</Subnets>"
            + $"<DBSubnetGroupArn>{subnet.GetValueOrDefault("DBSubnetGroupArn", "")}</DBSubnetGroupArn>"
            + "</DBSubnetGroup>"
            + $"<PreferredMaintenanceWindow>{i.GetValueOrDefault("PreferredMaintenanceWindow", "sun:05:00-sun:06:00")}</PreferredMaintenanceWindow>"
            + $"<PendingModifiedValues>{pendingXml}</PendingModifiedValues>"
            + $"<LatestRestorableTime>{i.GetValueOrDefault("LatestRestorableTime", "")}</LatestRestorableTime>"
            + $"<MultiAZ>{BoolStr(i.GetValueOrDefault("MultiAZ", false))}</MultiAZ>"
            + $"<AutoMinorVersionUpgrade>{BoolStr(i.GetValueOrDefault("AutoMinorVersionUpgrade", true))}</AutoMinorVersionUpgrade>"
            + "<ReadReplicaDBInstanceIdentifiers/>"
            + $"<ReadReplicaSourceDBInstanceIdentifier>{i.GetValueOrDefault("ReadReplicaSourceDBInstanceIdentifier", "")}</ReadReplicaSourceDBInstanceIdentifier>"
            + "<ReadReplicaDBClusterIdentifiers/>"
            + $"<LicenseModel>{i.GetValueOrDefault("LicenseModel", "general-public-license")}</LicenseModel>"
            + iopsXml
            + $"<OptionGroupMemberships>{optionXml}</OptionGroupMemberships>"
            + $"<PubliclyAccessible>{BoolStr(i.GetValueOrDefault("PubliclyAccessible", false))}</PubliclyAccessible>"
            + "<StatusInfos/>"
            + $"<StorageType>{i.GetValueOrDefault("StorageType", "gp2")}</StorageType>"
            + $"<DbInstancePort>{i.GetValueOrDefault("DbInstancePort", 0)}</DbInstancePort>"
            + $"<DBClusterIdentifier>{i.GetValueOrDefault("DBClusterIdentifier", "")}</DBClusterIdentifier>"
            + $"<StorageEncrypted>{BoolStr(i.GetValueOrDefault("StorageEncrypted", false))}</StorageEncrypted>"
            + $"<KmsKeyId>{i.GetValueOrDefault("KmsKeyId", "")}</KmsKeyId>"
            + $"<DbiResourceId>{i.GetValueOrDefault("DbiResourceId", "")}</DbiResourceId>"
            + $"<CACertificateIdentifier>{i.GetValueOrDefault("CACertificateIdentifier", "rds-ca-rsa2048-g1")}</CACertificateIdentifier>"
            + "<DomainMemberships/>"
            + $"<CopyTagsToSnapshot>{BoolStr(i.GetValueOrDefault("CopyTagsToSnapshot", false))}</CopyTagsToSnapshot>"
            + $"<MonitoringInterval>{i.GetValueOrDefault("MonitoringInterval", 0)}</MonitoringInterval>"
            + $"<MonitoringRoleArn>{i.GetValueOrDefault("MonitoringRoleArn", "")}</MonitoringRoleArn>"
            + $"<PromotionTier>{i.GetValueOrDefault("PromotionTier", 1)}</PromotionTier>"
            + $"<DBInstanceArn>{i["DBInstanceArn"]}</DBInstanceArn>"
            + $"<IAMDatabaseAuthenticationEnabled>{BoolStr(i.GetValueOrDefault("IAMDatabaseAuthenticationEnabled", false))}</IAMDatabaseAuthenticationEnabled>"
            + $"<PerformanceInsightsEnabled>{BoolStr(i.GetValueOrDefault("PerformanceInsightsEnabled", false))}</PerformanceInsightsEnabled>"
            + "<EnabledCloudwatchLogsExports/>"
            + "<ProcessorFeatures/>"
            + $"<DeletionProtection>{BoolStr(i.GetValueOrDefault("DeletionProtection", false))}</DeletionProtection>"
            + "<AssociatedRoles/>"
            + $"<MaxAllocatedStorage>{i.GetValueOrDefault("MaxAllocatedStorage", i.GetValueOrDefault("AllocatedStorage", 20))}</MaxAllocatedStorage>"
            + $"<TagList>{tagXml}</TagList>"
            + certXml
            + $"<BackupTarget>{i.GetValueOrDefault("BackupTarget", "region")}</BackupTarget>"
            + $"<NetworkType>{i.GetValueOrDefault("NetworkType", "IPV4")}</NetworkType>"
            + $"<StorageThroughput>{i.GetValueOrDefault("StorageThroughput", 0)}</StorageThroughput>";
    }

    private static string ClusterXml(Dictionary<string, object?> c)
    {
        var vpcSgXml = new StringBuilder();
        if (c.GetValueOrDefault("VpcSecurityGroups") is List<Dictionary<string, object?>> vpcSgs)
        {
            foreach (var sg in vpcSgs)
            {
                vpcSgXml.Append("<VpcSecurityGroupMembership>"
                    + $"<VpcSecurityGroupId>{sg.GetValueOrDefault("VpcSecurityGroupId", "")}</VpcSecurityGroupId>"
                    + $"<Status>{sg.GetValueOrDefault("Status", "active")}</Status>"
                    + "</VpcSecurityGroupMembership>");
            }
        }

        var memberXml = new StringBuilder();
        if (c.GetValueOrDefault("DBClusterMembers") is List<Dictionary<string, object?>> members)
        {
            foreach (var m in members)
            {
                memberXml.Append("<DBClusterMember>"
                    + $"<DBInstanceIdentifier>{m.GetValueOrDefault("DBInstanceIdentifier", "")}</DBInstanceIdentifier>"
                    + $"<IsClusterWriter>{BoolStr(m.GetValueOrDefault("IsClusterWriter", true))}</IsClusterWriter>"
                    + "<DBClusterParameterGroupStatus>in-sync</DBClusterParameterGroupStatus>"
                    + $"<PromotionTier>{m.GetValueOrDefault("PromotionTier", 1)}</PromotionTier>"
                    + "</DBClusterMember>");
            }
        }

        var azXml = new StringBuilder();
        if (c.GetValueOrDefault("AvailabilityZones") is List<string> azList)
        {
            foreach (var az in azList)
                azXml.Append($"<AvailabilityZone>{az}</AvailabilityZone>");
        }

        var tagXml = new StringBuilder();
        if (c.GetValueOrDefault("TagList") is List<Dictionary<string, string>> tags)
        {
            foreach (var t in tags)
                tagXml.Append($"<Tag><Key>{Esc(t["Key"])}</Key><Value>{Esc(t["Value"])}</Value></Tag>");
        }

        return $"<DBClusterIdentifier>{c["DBClusterIdentifier"]}</DBClusterIdentifier>"
            + $"<DBClusterArn>{c["DBClusterArn"]}</DBClusterArn>"
            + $"<Engine>{c["Engine"]}</Engine>"
            + $"<EngineVersion>{c["EngineVersion"]}</EngineVersion>"
            + $"<EngineMode>{c.GetValueOrDefault("EngineMode", "provisioned")}</EngineMode>"
            + $"<Status>{c["Status"]}</Status>"
            + $"<MasterUsername>{c.GetValueOrDefault("MasterUsername", "admin")}</MasterUsername>"
            + $"<DatabaseName>{c.GetValueOrDefault("DatabaseName", "")}</DatabaseName>"
            + $"<Endpoint>{c.GetValueOrDefault("Endpoint", "")}</Endpoint>"
            + $"<ReaderEndpoint>{c.GetValueOrDefault("ReaderEndpoint", "")}</ReaderEndpoint>"
            + $"<Port>{c["Port"]}</Port>"
            + $"<MultiAZ>{BoolStr(c.GetValueOrDefault("MultiAZ", false))}</MultiAZ>"
            + $"<AvailabilityZones>{azXml}</AvailabilityZones>"
            + $"<DBClusterMembers>{memberXml}</DBClusterMembers>"
            + $"<VpcSecurityGroups>{vpcSgXml}</VpcSecurityGroups>"
            + $"<DBSubnetGroup>{c.GetValueOrDefault("DBSubnetGroup", "default")}</DBSubnetGroup>"
            + $"<DBClusterParameterGroup>{c.GetValueOrDefault("DBClusterParameterGroup", "")}</DBClusterParameterGroup>"
            + $"<BackupRetentionPeriod>{c.GetValueOrDefault("BackupRetentionPeriod", 1)}</BackupRetentionPeriod>"
            + $"<PreferredBackupWindow>{c.GetValueOrDefault("PreferredBackupWindow", "03:00-04:00")}</PreferredBackupWindow>"
            + $"<PreferredMaintenanceWindow>{c.GetValueOrDefault("PreferredMaintenanceWindow", "sun:05:00-sun:06:00")}</PreferredMaintenanceWindow>"
            + $"<ClusterCreateTime>{c.GetValueOrDefault("ClusterCreateTime", "")}</ClusterCreateTime>"
            + $"<EarliestRestorableTime>{c.GetValueOrDefault("EarliestRestorableTime", "")}</EarliestRestorableTime>"
            + $"<LatestRestorableTime>{c.GetValueOrDefault("LatestRestorableTime", "")}</LatestRestorableTime>"
            + $"<StorageEncrypted>{BoolStr(c.GetValueOrDefault("StorageEncrypted", false))}</StorageEncrypted>"
            + $"<KmsKeyId>{c.GetValueOrDefault("KmsKeyId", "")}</KmsKeyId>"
            + $"<DeletionProtection>{BoolStr(c.GetValueOrDefault("DeletionProtection", false))}</DeletionProtection>"
            + $"<IAMDatabaseAuthenticationEnabled>{BoolStr(c.GetValueOrDefault("IAMDatabaseAuthenticationEnabled", false))}</IAMDatabaseAuthenticationEnabled>"
            + $"<HttpEndpointEnabled>{BoolStr(c.GetValueOrDefault("HttpEndpointEnabled", false))}</HttpEndpointEnabled>"
            + $"<CopyTagsToSnapshot>{BoolStr(c.GetValueOrDefault("CopyTagsToSnapshot", false))}</CopyTagsToSnapshot>"
            + $"<CrossAccountClone>{BoolStr(c.GetValueOrDefault("CrossAccountClone", false))}</CrossAccountClone>"
            + $"<DbClusterResourceId>{c.GetValueOrDefault("DbClusterResourceId", "")}</DbClusterResourceId>"
            + $"<HostedZoneId>{c.GetValueOrDefault("HostedZoneId", "Z2R2ITUGPM61AM")}</HostedZoneId>"
            + "<AssociatedRoles/>"
            + $"<TagList>{tagXml}</TagList>"
            + $"<AllocatedStorage>{c.GetValueOrDefault("AllocatedStorage", 1)}</AllocatedStorage>"
            + $"<ActivityStreamStatus>{c.GetValueOrDefault("ActivityStreamStatus", "stopped")}</ActivityStreamStatus>";
    }

    private static string SnapshotXml(Dictionary<string, object?> s)
    {
        var tagXml = new StringBuilder();
        if (s.GetValueOrDefault("TagList") is List<Dictionary<string, string>> tags)
        {
            foreach (var t in tags)
                tagXml.Append($"<Tag><Key>{Esc(t["Key"])}</Key><Value>{Esc(t["Value"])}</Value></Tag>");
        }

        return $"<DBSnapshotIdentifier>{s["DBSnapshotIdentifier"]}</DBSnapshotIdentifier>"
            + $"<DBInstanceIdentifier>{s["DBInstanceIdentifier"]}</DBInstanceIdentifier>"
            + $"<DBSnapshotArn>{s.GetValueOrDefault("DBSnapshotArn", "")}</DBSnapshotArn>"
            + $"<Engine>{s["Engine"]}</Engine>"
            + $"<EngineVersion>{s["EngineVersion"]}</EngineVersion>"
            + $"<SnapshotCreateTime>{s.GetValueOrDefault("SnapshotCreateTime", "")}</SnapshotCreateTime>"
            + $"<InstanceCreateTime>{s.GetValueOrDefault("InstanceCreateTime", "")}</InstanceCreateTime>"
            + $"<Status>{s["Status"]}</Status>"
            + $"<AllocatedStorage>{s.GetValueOrDefault("AllocatedStorage", 20)}</AllocatedStorage>"
            + $"<AvailabilityZone>{s.GetValueOrDefault("AvailabilityZone", $"{Region}a")}</AvailabilityZone>"
            + $"<VpcId>{s.GetValueOrDefault("VpcId", "vpc-00000000")}</VpcId>"
            + $"<Port>{s.GetValueOrDefault("Port", 5432)}</Port>"
            + $"<MasterUsername>{s.GetValueOrDefault("MasterUsername", "admin")}</MasterUsername>"
            + $"<DBName>{s.GetValueOrDefault("DBName", "")}</DBName>"
            + $"<SnapshotType>{s.GetValueOrDefault("SnapshotType", "manual")}</SnapshotType>"
            + $"<LicenseModel>{s.GetValueOrDefault("LicenseModel", "general-public-license")}</LicenseModel>"
            + $"<StorageType>{s.GetValueOrDefault("StorageType", "gp2")}</StorageType>"
            + $"<DBInstanceClass>{s.GetValueOrDefault("DBInstanceClass", "db.t3.micro")}</DBInstanceClass>"
            + $"<StorageEncrypted>{BoolStr(s.GetValueOrDefault("StorageEncrypted", false))}</StorageEncrypted>"
            + $"<KmsKeyId>{s.GetValueOrDefault("KmsKeyId", "")}</KmsKeyId>"
            + $"<Encrypted>{BoolStr(s.GetValueOrDefault("Encrypted", false))}</Encrypted>"
            + $"<IAMDatabaseAuthenticationEnabled>{BoolStr(s.GetValueOrDefault("IAMDatabaseAuthenticationEnabled", false))}</IAMDatabaseAuthenticationEnabled>"
            + $"<PercentProgress>{s.GetValueOrDefault("PercentProgress", 100)}</PercentProgress>"
            + $"<DbiResourceId>{s.GetValueOrDefault("DbiResourceId", "")}</DbiResourceId>"
            + $"<TagList>{tagXml}</TagList>"
            + $"<OriginalSnapshotCreateTime>{s.GetValueOrDefault("OriginalSnapshotCreateTime", "")}</OriginalSnapshotCreateTime>"
            + $"<SnapshotDatabaseTime>{s.GetValueOrDefault("SnapshotDatabaseTime", "")}</SnapshotDatabaseTime>"
            + $"<SnapshotTarget>{s.GetValueOrDefault("SnapshotTarget", "region")}</SnapshotTarget>";
    }

    private static string SubnetGroupXml(Dictionary<string, object?> sg)
    {
        var subnetsXml = new StringBuilder();
        if (sg.GetValueOrDefault("Subnets") is List<Dictionary<string, object?>> subnets)
        {
            foreach (var s in subnets)
            {
                var az = s.GetValueOrDefault("SubnetAvailabilityZone") is Dictionary<string, object?> azDict
                    ? (string)(azDict.GetValueOrDefault("Name") ?? $"{Region}a")
                    : $"{Region}a";
                subnetsXml.Append("<Subnet>"
                    + $"<SubnetIdentifier>{s.GetValueOrDefault("SubnetIdentifier", "")}</SubnetIdentifier>"
                    + $"<SubnetAvailabilityZone><Name>{az}</Name></SubnetAvailabilityZone>"
                    + "<SubnetOutpost/>"
                    + "<SubnetStatus>Active</SubnetStatus>"
                    + "</Subnet>");
            }
        }

        return $"<DBSubnetGroupName>{sg["DBSubnetGroupName"]}</DBSubnetGroupName>"
            + $"<DBSubnetGroupDescription>{sg.GetValueOrDefault("DBSubnetGroupDescription", "")}</DBSubnetGroupDescription>"
            + $"<VpcId>{sg.GetValueOrDefault("VpcId", "vpc-00000000")}</VpcId>"
            + $"<SubnetGroupStatus>{sg.GetValueOrDefault("SubnetGroupStatus", "Complete")}</SubnetGroupStatus>"
            + $"<Subnets>{subnetsXml}</Subnets>"
            + $"<DBSubnetGroupArn>{sg.GetValueOrDefault("DBSubnetGroupArn", "")}</DBSubnetGroupArn>"
            + "<SupportedNetworkTypes><member>IPV4</member></SupportedNetworkTypes>";
    }

    private static string ClusterSnapshotXml(Dictionary<string, object?> s)
    {
        var tagXml = new StringBuilder();
        if (s.GetValueOrDefault("TagList") is List<Dictionary<string, string>> tags)
        {
            foreach (var t in tags)
                tagXml.Append($"<Tag><Key>{Esc(t["Key"])}</Key><Value>{Esc(t["Value"])}</Value></Tag>");
        }

        var azXml = new StringBuilder();
        if (s.GetValueOrDefault("AvailabilityZones") is List<string> azList)
        {
            foreach (var az in azList)
                azXml.Append($"<AvailabilityZone>{az}</AvailabilityZone>");
        }

        return $"<DBClusterSnapshotIdentifier>{s["DBClusterSnapshotIdentifier"]}</DBClusterSnapshotIdentifier>"
            + $"<DBClusterIdentifier>{s["DBClusterIdentifier"]}</DBClusterIdentifier>"
            + $"<DBClusterSnapshotArn>{s.GetValueOrDefault("DBClusterSnapshotArn", "")}</DBClusterSnapshotArn>"
            + $"<Engine>{s["Engine"]}</Engine>"
            + $"<EngineVersion>{s["EngineVersion"]}</EngineVersion>"
            + $"<SnapshotCreateTime>{s.GetValueOrDefault("SnapshotCreateTime", "")}</SnapshotCreateTime>"
            + $"<ClusterCreateTime>{s.GetValueOrDefault("ClusterCreateTime", "")}</ClusterCreateTime>"
            + $"<Status>{s["Status"]}</Status>"
            + $"<Port>{s.GetValueOrDefault("Port", 5432)}</Port>"
            + $"<VpcId>{s.GetValueOrDefault("VpcId", "vpc-00000000")}</VpcId>"
            + $"<MasterUsername>{s.GetValueOrDefault("MasterUsername", "admin")}</MasterUsername>"
            + $"<SnapshotType>{s.GetValueOrDefault("SnapshotType", "manual")}</SnapshotType>"
            + $"<PercentProgress>{s.GetValueOrDefault("PercentProgress", 100)}</PercentProgress>"
            + $"<StorageEncrypted>{BoolStr(s.GetValueOrDefault("StorageEncrypted", false))}</StorageEncrypted>"
            + $"<KmsKeyId>{s.GetValueOrDefault("KmsKeyId", "")}</KmsKeyId>"
            + $"<AvailabilityZones>{azXml}</AvailabilityZones>"
            + $"<LicenseModel>{s.GetValueOrDefault("LicenseModel", "postgresql-license")}</LicenseModel>"
            + $"<DbClusterResourceId>{s.GetValueOrDefault("DbClusterResourceId", "")}</DbClusterResourceId>"
            + $"<IAMDatabaseAuthenticationEnabled>{BoolStr(s.GetValueOrDefault("IAMDatabaseAuthenticationEnabled", false))}</IAMDatabaseAuthenticationEnabled>"
            + $"<AllocatedStorage>{s.GetValueOrDefault("AllocatedStorage", 1)}</AllocatedStorage>"
            + $"<TagList>{tagXml}</TagList>";
    }

    private static string EventSubscriptionXml(Dictionary<string, object?> sub)
    {
        return $"<CustomerAwsId>{sub.GetValueOrDefault("CustomerAwsId", "")}</CustomerAwsId>"
            + $"<CustSubscriptionId>{sub.GetValueOrDefault("CustSubscriptionId", "")}</CustSubscriptionId>"
            + $"<SnsTopicArn>{sub.GetValueOrDefault("SnsTopicArn", "")}</SnsTopicArn>"
            + $"<Status>{sub.GetValueOrDefault("Status", "active")}</Status>"
            + $"<SubscriptionCreationTime>{TimeHelpers.NowIso()}</SubscriptionCreationTime>"
            + $"<SourceType>{sub.GetValueOrDefault("SourceType", "")}</SourceType>"
            + $"<EventSubscriptionArn>{sub.GetValueOrDefault("EventSubscriptionArn", "")}</EventSubscriptionArn>"
            + $"<Enabled>{BoolStr(sub.GetValueOrDefault("Enabled", true))}</Enabled>";
    }

    private static string DbProxyXml(Dictionary<string, object?> proxy)
    {
        return $"<DBProxyName>{proxy["DBProxyName"]}</DBProxyName>"
            + $"<DBProxyArn>{proxy.GetValueOrDefault("DBProxyArn", "")}</DBProxyArn>"
            + $"<EngineFamily>{proxy.GetValueOrDefault("EngineFamily", "POSTGRESQL")}</EngineFamily>"
            + $"<Status>{proxy.GetValueOrDefault("Status", "available")}</Status>"
            + $"<RoleArn>{proxy.GetValueOrDefault("RoleArn", "")}</RoleArn>"
            + $"<VpcId>{proxy.GetValueOrDefault("VpcId", "vpc-00000000")}</VpcId>"
            + $"<Endpoint>{proxy.GetValueOrDefault("Endpoint", "")}</Endpoint>"
            + $"<RequireTLS>{BoolStr(proxy.GetValueOrDefault("RequireTLS", false))}</RequireTLS>"
            + $"<IdleClientTimeout>{proxy.GetValueOrDefault("IdleClientTimeout", 1800)}</IdleClientTimeout>"
            + $"<DebugLogging>{BoolStr(proxy.GetValueOrDefault("DebugLogging", false))}</DebugLogging>";
    }

    private static string OptionGroupXml(Dictionary<string, object?> og)
    {
        return $"<OptionGroupName>{og["OptionGroupName"]}</OptionGroupName>"
            + $"<OptionGroupDescription>{Esc((string)(og.GetValueOrDefault("OptionGroupDescription") ?? ""))}</OptionGroupDescription>"
            + $"<EngineName>{og.GetValueOrDefault("EngineName", "")}</EngineName>"
            + $"<MajorEngineVersion>{og.GetValueOrDefault("MajorEngineVersion", "")}</MajorEngineVersion>"
            + "<Options/>"
            + $"<AllowsVpcAndNonVpcInstanceMemberships>{BoolStr(og.GetValueOrDefault("AllowsVpcAndNonVpcInstanceMemberships", true))}</AllowsVpcAndNonVpcInstanceMemberships>"
            + $"<OptionGroupArn>{og.GetValueOrDefault("OptionGroupArn", "")}</OptionGroupArn>";
    }

    private static string GlobalClusterXml(Dictionary<string, object?> gc)
    {
        var membersXml = new StringBuilder();
        if (gc.GetValueOrDefault("GlobalClusterMembers") is List<Dictionary<string, object?>> members)
        {
            foreach (var m in members)
            {
                membersXml.Append("<GlobalClusterMember>"
                    + $"<DBClusterArn>{m.GetValueOrDefault("DBClusterArn", "")}</DBClusterArn>"
                    + $"<IsWriter>{BoolStr(m.GetValueOrDefault("IsWriter", false))}</IsWriter>"
                    + $"<GlobalWriteForwardingStatus>{m.GetValueOrDefault("GlobalWriteForwardingStatus", "disabled")}</GlobalWriteForwardingStatus>"
                    + "<Readers/>"
                    + "</GlobalClusterMember>");
            }
        }

        return $"<GlobalClusterIdentifier>{gc["GlobalClusterIdentifier"]}</GlobalClusterIdentifier>"
            + $"<GlobalClusterArn>{gc.GetValueOrDefault("GlobalClusterArn", "")}</GlobalClusterArn>"
            + $"<GlobalClusterResourceId>{gc.GetValueOrDefault("GlobalClusterResourceId", "")}</GlobalClusterResourceId>"
            + $"<Engine>{gc.GetValueOrDefault("Engine", "")}</Engine>"
            + $"<EngineVersion>{gc.GetValueOrDefault("EngineVersion", "")}</EngineVersion>"
            + $"<Status>{gc.GetValueOrDefault("Status", "available")}</Status>"
            + $"<DeletionProtection>{BoolStr(gc.GetValueOrDefault("DeletionProtection", false))}</DeletionProtection>"
            + $"<StorageEncrypted>{BoolStr(gc.GetValueOrDefault("StorageEncrypted", false))}</StorageEncrypted>"
            + $"<GlobalClusterMembers>{membersXml}</GlobalClusterMembers>";
    }

    private ServiceResponse SingleInstanceResponse(string rootTag, string resultTag, Dictionary<string, object?> instance)
    {
        return RdsXml(200, rootTag,
            $"<{resultTag}><DBInstance>{InstanceXml(instance)}</DBInstance></{resultTag}>");
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

        // Fall back to Prefix.<AnyMemberName>.N (botocore serializer format)
        var pattern = MemberListFallbackRegex(prefix);
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

    private static Regex MemberListFallbackRegex(string prefix)
    {
        return new Regex($@"^{Regex.Escape(prefix)}\.([^.]+)\.(\d+)$", RegexOptions.Compiled);
    }

    private static List<Dictionary<string, string>> ParseTags(Dictionary<string, string[]> p)
    {
        var tags = new List<Dictionary<string, string>>();
        var tagPrefix = !string.IsNullOrEmpty(P(p, "Tags.member.1.Key")) ? "Tags.member" : "Tags.Tag";
        for (var i = 1; ; i++)
        {
            var key = P(p, $"{tagPrefix}.{i}.Key");
            if (string.IsNullOrEmpty(key)) break;
            var value = P(p, $"{tagPrefix}.{i}.Value");
            tags.Add(new Dictionary<string, string> { ["Key"] = key, ["Value"] = value });
        }
        return tags;
    }

    private static Dictionary<string, List<string>> ParseFilters(Dictionary<string, string[]> p)
    {
        var filters = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        for (var i = 1; ; i++)
        {
            var name = P(p, $"Filters.member.{i}.Name");
            if (string.IsNullOrEmpty(name)) break;
            var vals = new List<string>();
            for (var j = 1; ; j++)
            {
                var v = P(p, $"Filters.member.{i}.Values.member.{j}");
                if (string.IsNullOrEmpty(v)) break;
                vals.Add(v);
            }
            filters[name] = vals;
        }
        return filters;
    }

    // ── Static helpers ────────────────────────────────────────────────────────

    private static string DefaultEngineVersion(string engine)
    {
        return engine switch
        {
            "postgres" => "15.3",
            "mysql" => "8.0.33",
            "mariadb" => "10.6.14",
            "aurora-postgresql" => "15.3",
            "aurora-mysql" => "8.0.mysql_aurora.3.03.0",
            _ => "15.3",
        };
    }

    private static string DefaultPort(string engine)
    {
        if (engine.Contains("mysql", StringComparison.Ordinal)
            || engine.Contains("mariadb", StringComparison.Ordinal))
            return "3306";
        return "5432";
    }

    private static string LicenseModel(string engine)
    {
        if (engine.Contains("postgres", StringComparison.Ordinal)
            || engine.Contains("aurora", StringComparison.Ordinal))
            return "postgresql-license";
        return "general-public-license";
    }

    private static List<Dictionary<string, string>> DefaultParametersForFamily(string family)
    {
        if (family.Contains("mysql", StringComparison.OrdinalIgnoreCase))
        {
            return
            [
                new() { ["name"] = "max_connections", ["default"] = "151", ["description"] = "Max number of connections", ["apply_type"] = "dynamic", ["data_type"] = "integer", ["modifiable"] = "true" },
                new() { ["name"] = "innodb_buffer_pool_size", ["default"] = "134217728", ["description"] = "InnoDB buffer pool size", ["apply_type"] = "static", ["data_type"] = "integer", ["modifiable"] = "true" },
                new() { ["name"] = "character_set_server", ["default"] = "utf8mb4", ["description"] = "Server character set", ["apply_type"] = "dynamic", ["data_type"] = "string", ["modifiable"] = "true" },
                new() { ["name"] = "slow_query_log", ["default"] = "0", ["description"] = "Enable slow query log", ["apply_type"] = "dynamic", ["data_type"] = "boolean", ["modifiable"] = "true" },
                new() { ["name"] = "long_query_time", ["default"] = "10", ["description"] = "Slow query threshold", ["apply_type"] = "dynamic", ["data_type"] = "float", ["modifiable"] = "true" },
            ];
        }

        return
        [
            new() { ["name"] = "max_connections", ["default"] = "100", ["description"] = "Max number of connections", ["apply_type"] = "dynamic", ["data_type"] = "integer", ["modifiable"] = "true" },
            new() { ["name"] = "shared_buffers", ["default"] = "128MB", ["description"] = "Shared memory buffers", ["apply_type"] = "static", ["data_type"] = "string", ["modifiable"] = "true" },
            new() { ["name"] = "work_mem", ["default"] = "4MB", ["description"] = "Memory for internal sort ops", ["apply_type"] = "dynamic", ["data_type"] = "string", ["modifiable"] = "true" },
            new() { ["name"] = "maintenance_work_mem", ["default"] = "64MB", ["description"] = "Memory for maintenance ops", ["apply_type"] = "dynamic", ["data_type"] = "string", ["modifiable"] = "true" },
            new() { ["name"] = "effective_cache_size", ["default"] = "4GB", ["description"] = "Planner effective cache size", ["apply_type"] = "dynamic", ["data_type"] = "string", ["modifiable"] = "true" },
            new() { ["name"] = "log_statement", ["default"] = "none", ["description"] = "Type of statements logged", ["apply_type"] = "dynamic", ["data_type"] = "string", ["modifiable"] = "true" },
            new() { ["name"] = "log_min_duration_statement", ["default"] = "-1", ["description"] = "Min duration before logging", ["apply_type"] = "dynamic", ["data_type"] = "integer", ["modifiable"] = "true" },
        ];
    }

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

    private static ServiceResponse RdsXml(int status, string rootTag, string inner)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{rootTag} xmlns=\"{RdsNs}\">\n    {inner}\n    <ResponseMetadata><RequestId>{requestId}</RequestId></ResponseMetadata>\n</{rootTag}>";
        return new ServiceResponse(status, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }

    private static ServiceResponse Error(string code, string message, int status)
    {
        var requestId = Guid.NewGuid().ToString();
        var body = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ErrorResponse xmlns=\"{RdsNs}\">\n    <Error><Code>{code}</Code><Message>{message}</Message></Error>\n    <RequestId>{requestId}</RequestId>\n</ErrorResponse>";
        return new ServiceResponse(status, XmlHeaders, Encoding.UTF8.GetBytes(body));
    }
}
