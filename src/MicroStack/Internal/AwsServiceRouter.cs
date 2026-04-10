using System.Text.RegularExpressions;

namespace MicroStack.Internal;

/// <summary>
/// Detects which AWS service a request targets, using the same heuristic chain
/// as ministack/core/router.py: Target header → credential scope → Action param
/// → URL path patterns → Host header patterns → default "s3".
/// </summary>
internal sealed partial class AwsServiceRouter
{
    // ── X-Amz-Target prefix → service name ───────────────────────────────────

    private static readonly (string Prefix, string Service)[] TargetPrefixes =
    [
        ("AmazonSQS",                              "sqs"),
        ("AmazonSNS",                              "sns"),
        ("DynamoDB_20120810",                      "dynamodb"),
        ("AWSSecurityTokenService",                "sts"),
        ("secretsmanager",                         "secretsmanager"),
        ("GraniteServiceVersion20100801",          "monitoring"),
        ("Logs_20140328",                          "logs"),
        ("AmazonSSM",                              "ssm"),
        ("AmazonEventBridge",                      "events"),
        ("AWSEvents",                              "events"),
        ("Kinesis_20131202",                       "kinesis"),
        ("AWSStepFunctions",                       "states"),
        ("AmazonEC2ContainerServiceV20141113",     "ecs"),
        ("AWSGlue",                                "glue"),
        ("AmazonAthena",                           "athena"),
        ("Firehose_20150804",                      "firehose"),
        ("AWSCognitoIdentityProviderService",      "cognito-idp"),
        ("AWSCognitoIdentityService",              "cognito-identity"),
        ("ElasticMapReduce",                       "elasticmapreduce"),
        ("AmazonEC2ContainerRegistry_V20150921",   "ecr"),
        ("CertificateManager",                     "acm"),
        ("AWSWAF_20190729",                        "wafv2"),
        ("TrentService",                           "kms"),
        ("Route53AutoNaming_v20170314",            "servicediscovery"),
    ];

    // ── Credential scope → service name ──────────────────────────────────────

    private static readonly Dictionary<string, string> ScopeMap = new(StringComparer.OrdinalIgnoreCase)
    {
        ["monitoring"]            = "monitoring",
        ["execute-api"]           = "apigateway",
        ["ses"]                   = "ses",
        ["states"]                = "states",
        ["kinesis"]               = "kinesis",
        ["events"]                = "events",
        ["ssm"]                   = "ssm",
        ["ecs"]                   = "ecs",
        ["rds"]                   = "rds",
        ["elasticache"]           = "elasticache",
        ["glue"]                  = "glue",
        ["athena"]                = "athena",
        ["kinesis-firehose"]      = "firehose",
        ["route53"]               = "route53",
        ["acm"]                   = "acm",
        ["wafv2"]                 = "wafv2",
        ["cognito-idp"]           = "cognito-idp",
        ["cognito-identity"]      = "cognito-identity",
        ["ecr"]                   = "ecr",
        ["elasticmapreduce"]      = "elasticmapreduce",
        ["elasticloadbalancing"]  = "elasticloadbalancing",
        ["elasticfilesystem"]     = "elasticfilesystem",
        ["cloudformation"]        = "cloudformation",
        ["kms"]                   = "kms",
        ["cloudfront"]            = "cloudfront",
        ["appsync"]               = "appsync",
        ["servicediscovery"]      = "servicediscovery",
        ["s3files"]               = "s3files",
        ["rds-data"]              = "rds-data",
        ["sqs"]                   = "sqs",
        ["sns"]                   = "sns",
        ["dynamodb"]              = "dynamodb",
        ["sts"]                   = "sts",
        ["iam"]                   = "iam",
        ["secretsmanager"]        = "secretsmanager",
        ["logs"]                  = "logs",
        ["s3"]                    = "s3",
        ["lambda"]                = "lambda",
        ["ec2"]                   = "ec2",
    };

    // ── Action query param → service name ────────────────────────────────────

    private static readonly Dictionary<string, string> ActionServiceMap = new(StringComparer.Ordinal)
    {
        // SQS
        ["SendMessage"] = "sqs", ["ReceiveMessage"] = "sqs", ["DeleteMessage"] = "sqs",
        ["CreateQueue"] = "sqs", ["DeleteQueue"] = "sqs", ["ListQueues"] = "sqs",
        ["GetQueueUrl"] = "sqs", ["GetQueueAttributes"] = "sqs", ["SetQueueAttributes"] = "sqs",
        ["PurgeQueue"] = "sqs", ["ChangeMessageVisibility"] = "sqs",
        ["ChangeMessageVisibilityBatch"] = "sqs",
        ["SendMessageBatch"] = "sqs", ["DeleteMessageBatch"] = "sqs",
        ["ListQueueTags"] = "sqs", ["TagQueue"] = "sqs", ["UntagQueue"] = "sqs",
        // SNS
        ["Publish"] = "sns", ["Subscribe"] = "sns", ["Unsubscribe"] = "sns",
        ["CreateTopic"] = "sns", ["DeleteTopic"] = "sns", ["ListTopics"] = "sns",
        ["ListSubscriptions"] = "sns", ["ConfirmSubscription"] = "sns",
        ["SetTopicAttributes"] = "sns", ["GetTopicAttributes"] = "sns",
        ["ListSubscriptionsByTopic"] = "sns",
        ["GetSubscriptionAttributes"] = "sns", ["SetSubscriptionAttributes"] = "sns",
        ["PublishBatch"] = "sns",
        ["TagResource"] = "sns", ["UntagResource"] = "sns",
        ["CreatePlatformApplication"] = "sns", ["CreatePlatformEndpoint"] = "sns",
        // IAM
        ["CreateRole"] = "iam", ["GetRole"] = "iam", ["ListRoles"] = "iam",
        ["DeleteRole"] = "iam", ["CreateUser"] = "iam", ["GetUser"] = "iam",
        ["ListUsers"] = "iam", ["DeleteUser"] = "iam",
        ["CreatePolicy"] = "iam", ["GetPolicy"] = "iam", ["DeletePolicy"] = "iam",
        ["GetPolicyVersion"] = "iam", ["ListPolicyVersions"] = "iam",
        ["CreatePolicyVersion"] = "iam", ["DeletePolicyVersion"] = "iam",
        ["ListPolicies"] = "iam",
        ["AttachRolePolicy"] = "iam", ["DetachRolePolicy"] = "iam",
        ["ListAttachedRolePolicies"] = "iam",
        ["PutRolePolicy"] = "iam", ["GetRolePolicy"] = "iam",
        ["DeleteRolePolicy"] = "iam", ["ListRolePolicies"] = "iam",
        ["CreateAccessKey"] = "iam", ["ListAccessKeys"] = "iam", ["DeleteAccessKey"] = "iam",
        ["CreateInstanceProfile"] = "iam", ["DeleteInstanceProfile"] = "iam",
        ["GetInstanceProfile"] = "iam", ["AddRoleToInstanceProfile"] = "iam",
        ["RemoveRoleFromInstanceProfile"] = "iam",
        ["ListInstanceProfiles"] = "iam", ["ListInstanceProfilesForRole"] = "iam",
        ["UpdateAssumeRolePolicy"] = "iam",
        ["AttachUserPolicy"] = "iam", ["DetachUserPolicy"] = "iam",
        ["ListAttachedUserPolicies"] = "iam",
        ["TagRole"] = "iam", ["UntagRole"] = "iam", ["ListRoleTags"] = "iam",
        ["TagUser"] = "iam", ["UntagUser"] = "iam", ["ListUserTags"] = "iam",
        ["SimulatePrincipalPolicy"] = "iam", ["SimulateCustomPolicy"] = "iam",
        // STS
        ["GetCallerIdentity"] = "sts", ["AssumeRole"] = "sts",
        ["GetSessionToken"] = "sts", ["AssumeRoleWithWebIdentity"] = "sts",
        ["AssumeRoleWithSAML"] = "sts",
        // CloudWatch
        ["PutMetricData"] = "monitoring", ["GetMetricData"] = "monitoring",
        ["ListMetrics"] = "monitoring", ["PutMetricAlarm"] = "monitoring",
        ["DescribeAlarms"] = "monitoring", ["DeleteAlarms"] = "monitoring",
        ["GetMetricStatistics"] = "monitoring", ["SetAlarmState"] = "monitoring",
        ["EnableAlarmActions"] = "monitoring", ["DisableAlarmActions"] = "monitoring",
        ["DescribeAlarmsForMetric"] = "monitoring", ["DescribeAlarmHistory"] = "monitoring",
        ["PutCompositeAlarm"] = "monitoring",
        // SES
        ["SendEmail"] = "ses", ["SendRawEmail"] = "ses",
        ["VerifyEmailIdentity"] = "ses", ["VerifyEmailAddress"] = "ses",
        ["VerifyDomainIdentity"] = "ses", ["VerifyDomainDkim"] = "ses",
        ["ListIdentities"] = "ses", ["DeleteIdentity"] = "ses",
        ["GetSendQuota"] = "ses", ["GetSendStatistics"] = "ses",
        ["ListVerifiedEmailAddresses"] = "ses",
        ["GetIdentityVerificationAttributes"] = "ses",
        ["GetIdentityDkimAttributes"] = "ses",
        ["SetIdentityNotificationTopic"] = "ses",
        ["SetIdentityFeedbackForwardingEnabled"] = "ses",
        ["CreateConfigurationSet"] = "ses", ["DeleteConfigurationSet"] = "ses",
        ["DescribeConfigurationSet"] = "ses", ["ListConfigurationSets"] = "ses",
        ["CreateTemplate"] = "ses",
        ["DeleteTemplate"] = "ses", ["ListTemplates"] = "ses", ["UpdateTemplate"] = "ses",
        ["SendTemplatedEmail"] = "ses", ["SendBulkTemplatedEmail"] = "ses",
        // RDS
        ["CreateDBInstance"] = "rds", ["DeleteDBInstance"] = "rds", ["DescribeDBInstances"] = "rds",
        ["StartDBInstance"] = "rds", ["StopDBInstance"] = "rds", ["RebootDBInstance"] = "rds",
        ["ModifyDBInstance"] = "rds", ["CreateDBCluster"] = "rds", ["DeleteDBCluster"] = "rds",
        ["ModifyDBCluster"] = "rds",
        ["DescribeDBClusters"] = "rds", ["CreateDBSubnetGroup"] = "rds", ["DescribeDBSubnetGroups"] = "rds",
        ["DeleteDBSubnetGroup"] = "rds",
        ["CreateDBParameterGroup"] = "rds", ["DescribeDBParameterGroups"] = "rds",
        ["DeleteDBParameterGroup"] = "rds", ["DescribeDBParameters"] = "rds",
        ["DescribeDBEngineVersions"] = "rds",
        ["DescribeOrderableDBInstanceOptions"] = "rds",
        ["CreateDBSnapshot"] = "rds", ["DeleteDBSnapshot"] = "rds", ["DescribeDBSnapshots"] = "rds",
        ["CreateDBInstanceReadReplica"] = "rds", ["RestoreDBInstanceFromDBSnapshot"] = "rds",
        ["AddTagsToResource"] = "rds", ["RemoveTagsFromResource"] = "rds",
        // ElastiCache
        ["CreateCacheCluster"] = "elasticache", ["DeleteCacheCluster"] = "elasticache",
        ["DescribeCacheClusters"] = "elasticache", ["ModifyCacheCluster"] = "elasticache",
        ["RebootCacheCluster"] = "elasticache",
        ["CreateReplicationGroup"] = "elasticache", ["DeleteReplicationGroup"] = "elasticache",
        ["DescribeReplicationGroups"] = "elasticache", ["ModifyReplicationGroup"] = "elasticache",
        ["CreateCacheSubnetGroup"] = "elasticache", ["DescribeCacheSubnetGroups"] = "elasticache",
        ["DeleteCacheSubnetGroup"] = "elasticache",
        ["CreateCacheParameterGroup"] = "elasticache", ["DescribeCacheParameterGroups"] = "elasticache",
        ["DeleteCacheParameterGroup"] = "elasticache",
        ["DescribeCacheParameters"] = "elasticache", ["ModifyCacheParameterGroup"] = "elasticache",
        ["DescribeCacheEngineVersions"] = "elasticache",
        ["CreateSnapshot"] = "elasticache", ["DeleteSnapshot"] = "elasticache",
        ["DescribeSnapshots"] = "elasticache",
        ["IncreaseReplicaCount"] = "elasticache", ["DecreaseReplicaCount"] = "elasticache",
        // EC2
        ["RunInstances"] = "ec2", ["DescribeInstances"] = "ec2", ["TerminateInstances"] = "ec2",
        ["StopInstances"] = "ec2", ["StartInstances"] = "ec2", ["RebootInstances"] = "ec2",
        ["DescribeImages"] = "ec2",
        ["CreateSecurityGroup"] = "ec2", ["DeleteSecurityGroup"] = "ec2",
        ["DescribeSecurityGroups"] = "ec2",
        ["AuthorizeSecurityGroupIngress"] = "ec2", ["RevokeSecurityGroupIngress"] = "ec2",
        ["AuthorizeSecurityGroupEgress"] = "ec2", ["RevokeSecurityGroupEgress"] = "ec2",
        ["CreateKeyPair"] = "ec2", ["DeleteKeyPair"] = "ec2", ["DescribeKeyPairs"] = "ec2",
        ["ImportKeyPair"] = "ec2",
        ["DescribeVpcs"] = "ec2", ["CreateVpc"] = "ec2", ["DeleteVpc"] = "ec2",
        ["DescribeSubnets"] = "ec2", ["CreateSubnet"] = "ec2", ["DeleteSubnet"] = "ec2",
        ["CreateInternetGateway"] = "ec2", ["DeleteInternetGateway"] = "ec2",
        ["DescribeInternetGateways"] = "ec2",
        ["AttachInternetGateway"] = "ec2", ["DetachInternetGateway"] = "ec2",
        ["DescribeAvailabilityZones"] = "ec2",
        ["AllocateAddress"] = "ec2", ["ReleaseAddress"] = "ec2",
        ["AssociateAddress"] = "ec2", ["DisassociateAddress"] = "ec2",
        ["DescribeAddresses"] = "ec2",
        ["CreateTags"] = "ec2", ["DeleteTags"] = "ec2", ["DescribeTags"] = "ec2",
        ["ModifyVpcAttribute"] = "ec2", ["ModifySubnetAttribute"] = "ec2",
        ["CreateRouteTable"] = "ec2", ["DeleteRouteTable"] = "ec2", ["DescribeRouteTables"] = "ec2",
        ["AssociateRouteTable"] = "ec2", ["DisassociateRouteTable"] = "ec2",
        ["CreateRoute"] = "ec2", ["ReplaceRoute"] = "ec2", ["DeleteRoute"] = "ec2",
        ["CreateNetworkInterface"] = "ec2", ["DeleteNetworkInterface"] = "ec2",
        ["DescribeNetworkInterfaces"] = "ec2",
        ["AttachNetworkInterface"] = "ec2", ["DetachNetworkInterface"] = "ec2",
        ["CreateVpcEndpoint"] = "ec2", ["DeleteVpcEndpoints"] = "ec2",
        ["DescribeVpcEndpoints"] = "ec2",
        // ELBv2 / ALB
        ["CreateLoadBalancer"] = "elasticloadbalancing",
        ["DescribeLoadBalancers"] = "elasticloadbalancing",
        ["DeleteLoadBalancer"] = "elasticloadbalancing",
        ["DescribeLoadBalancerAttributes"] = "elasticloadbalancing",
        ["ModifyLoadBalancerAttributes"] = "elasticloadbalancing",
        ["CreateTargetGroup"] = "elasticloadbalancing",
        ["DescribeTargetGroups"] = "elasticloadbalancing",
        ["ModifyTargetGroup"] = "elasticloadbalancing",
        ["DeleteTargetGroup"] = "elasticloadbalancing",
        ["DescribeTargetGroupAttributes"] = "elasticloadbalancing",
        ["ModifyTargetGroupAttributes"] = "elasticloadbalancing",
        ["CreateListener"] = "elasticloadbalancing",
        ["DescribeListeners"] = "elasticloadbalancing",
        ["ModifyListener"] = "elasticloadbalancing",
        ["DeleteListener"] = "elasticloadbalancing",
        ["CreateRule"] = "elasticloadbalancing",
        ["DescribeRules"] = "elasticloadbalancing",
        ["ModifyRule"] = "elasticloadbalancing",
        ["DeleteRule"] = "elasticloadbalancing",
        ["SetRulePriorities"] = "elasticloadbalancing",
        ["RegisterTargets"] = "elasticloadbalancing",
        ["DeregisterTargets"] = "elasticloadbalancing",
        ["DescribeTargetHealth"] = "elasticloadbalancing",
        ["AddTags"] = "elasticloadbalancing",
        ["RemoveTags"] = "elasticloadbalancing",
        // EBS Volumes
        ["CreateVolume"] = "ec2", ["DeleteVolume"] = "ec2", ["DescribeVolumes"] = "ec2",
        ["DescribeVolumeStatus"] = "ec2", ["AttachVolume"] = "ec2", ["DetachVolume"] = "ec2",
        ["ModifyVolume"] = "ec2", ["DescribeVolumesModifications"] = "ec2",
        ["EnableVolumeIO"] = "ec2", ["ModifyVolumeAttribute"] = "ec2",
        ["DescribeVolumeAttribute"] = "ec2",
        // CloudFormation
        ["CreateStack"] = "cloudformation", ["DescribeStacks"] = "cloudformation",
        ["UpdateStack"] = "cloudformation", ["DeleteStack"] = "cloudformation",
        ["ListStacks"] = "cloudformation",
        ["DescribeStackEvents"] = "cloudformation",
        ["DescribeStackResource"] = "cloudformation", ["DescribeStackResources"] = "cloudformation",
        ["ListStackResources"] = "cloudformation",
        ["GetTemplateSummary"] = "cloudformation",
        ["ValidateTemplate"] = "cloudformation",
        ["CreateChangeSet"] = "cloudformation", ["DescribeChangeSet"] = "cloudformation",
        ["ExecuteChangeSet"] = "cloudformation", ["DeleteChangeSet"] = "cloudformation",
        ["ListChangeSets"] = "cloudformation",
        ["ListExports"] = "cloudformation", ["ListImports"] = "cloudformation",
        ["UpdateTerminationProtection"] = "cloudformation",
        ["SetStackPolicy"] = "cloudformation", ["GetStackPolicy"] = "cloudformation",
        // EBS Snapshots (omit CreateSnapshot/DeleteSnapshot/DescribeSnapshots — conflict with ElastiCache)
        ["CopySnapshot"] = "ec2", ["ModifySnapshotAttribute"] = "ec2",
        ["DescribeSnapshotAttribute"] = "ec2",
    };

    // ── Host header patterns ──────────────────────────────────────────────────

    private static readonly (Regex Pattern, string Service)[] HostPatterns =
    [
        (S3HostRegex(),              "s3"),
        (SqsHostRegex(),             "sqs"),
        (SnsHostRegex(),             "sns"),
        (DynamoDbHostRegex(),        "dynamodb"),
        (LambdaHostRegex(),          "lambda"),
        (IamHostRegex(),             "iam"),
        (StsHostRegex(),             "sts"),
        (SecretsManagerHostRegex(),  "secretsmanager"),
        (MonitoringHostRegex(),      "monitoring"),
        (LogsHostRegex(),            "logs"),
        (SsmHostRegex(),             "ssm"),
        (EventsHostRegex(),          "events"),
        (KinesisHostRegex(),         "kinesis"),
        (EmailHostRegex(),           "ses"),
        (StatesHostRegex(),          "states"),
        (EcsHostRegex(),             "ecs"),
        (RdsHostRegex(),             "rds"),
        (ElastiCacheHostRegex(),     "elasticache"),
        (GlueHostRegex(),            "glue"),
        (AthenaHostRegex(),          "athena"),
        (FirehoseHostRegex(),        "firehose"),
        (KinesisFirehoseHostRegex(), "firehose"),
        (ApiGatewayHostRegex(),      "apigateway"),
        (ExecuteApiHostRegex(),      "apigateway"),
        (Route53HostRegex(),         "route53"),
        (CognitoIdpHostRegex(),      "cognito-idp"),
        (CognitoIdentityHostRegex(), "cognito-identity"),
        (EmrHostRegex(),             "elasticmapreduce"),
        (EfsHostRegex(),             "elasticfilesystem"),
        (EcrApiHostRegex(),          "ecr"),
        (EcrHostRegex(),             "ecr"),
        (Ec2HostRegex(),             "ec2"),
        (ElbHostRegex(),             "elasticloadbalancing"),
        (AcmHostRegex(),             "acm"),
        (WafV2HostRegex(),           "wafv2"),
        (CloudFormationHostRegex(),  "cloudformation"),
        (KmsHostRegex(),             "kms"),
        (CloudFrontHostRegex(),      "cloudfront"),
        (AppSyncHostRegex(),         "appsync"),
        (ServiceDiscoveryHostRegex(),"servicediscovery"),
        (S3FilesHostRegex(),         "s3files"),
        (RdsDataHostRegex(),         "rds-data"),
    ];

    // ── Credential scope regex ────────────────────────────────────────────────

    [GeneratedRegex(@"Credential=[^/]+/[^/]+/[^/]+/([^/]+)/")]
    private static partial Regex CredentialScopeRegex();

    [GeneratedRegex(@"Credential=([^/]+)/")]
    private static partial Regex AccessKeyRegex();

    [GeneratedRegex(@"Credential=[^/]+/[^/]+/([^/]+)/")]
    private static partial Regex RegionRegex();

    // ── Host pattern regexes (generated) ─────────────────────────────────────

    [GeneratedRegex(@"s3[\.\-]|\.s3\.",           RegexOptions.IgnoreCase)] private static partial Regex S3HostRegex();
    [GeneratedRegex(@"sqs\.",                      RegexOptions.IgnoreCase)] private static partial Regex SqsHostRegex();
    [GeneratedRegex(@"sns\.",                      RegexOptions.IgnoreCase)] private static partial Regex SnsHostRegex();
    [GeneratedRegex(@"dynamodb\.",                 RegexOptions.IgnoreCase)] private static partial Regex DynamoDbHostRegex();
    [GeneratedRegex(@"lambda\.",                   RegexOptions.IgnoreCase)] private static partial Regex LambdaHostRegex();
    [GeneratedRegex(@"iam\.",                      RegexOptions.IgnoreCase)] private static partial Regex IamHostRegex();
    [GeneratedRegex(@"sts\.",                      RegexOptions.IgnoreCase)] private static partial Regex StsHostRegex();
    [GeneratedRegex(@"secretsmanager\.",           RegexOptions.IgnoreCase)] private static partial Regex SecretsManagerHostRegex();
    [GeneratedRegex(@"monitoring\.",               RegexOptions.IgnoreCase)] private static partial Regex MonitoringHostRegex();
    [GeneratedRegex(@"logs\.",                     RegexOptions.IgnoreCase)] private static partial Regex LogsHostRegex();
    [GeneratedRegex(@"ssm\.",                      RegexOptions.IgnoreCase)] private static partial Regex SsmHostRegex();
    [GeneratedRegex(@"events\.",                   RegexOptions.IgnoreCase)] private static partial Regex EventsHostRegex();
    [GeneratedRegex(@"kinesis\.",                  RegexOptions.IgnoreCase)] private static partial Regex KinesisHostRegex();
    [GeneratedRegex(@"email\.",                    RegexOptions.IgnoreCase)] private static partial Regex EmailHostRegex();
    [GeneratedRegex(@"states\.",                   RegexOptions.IgnoreCase)] private static partial Regex StatesHostRegex();
    [GeneratedRegex(@"ecs\.",                      RegexOptions.IgnoreCase)] private static partial Regex EcsHostRegex();
    [GeneratedRegex(@"rds\.",                      RegexOptions.IgnoreCase)] private static partial Regex RdsHostRegex();
    [GeneratedRegex(@"elasticache\.",              RegexOptions.IgnoreCase)] private static partial Regex ElastiCacheHostRegex();
    [GeneratedRegex(@"glue\.",                     RegexOptions.IgnoreCase)] private static partial Regex GlueHostRegex();
    [GeneratedRegex(@"athena\.",                   RegexOptions.IgnoreCase)] private static partial Regex AthenaHostRegex();
    [GeneratedRegex(@"firehose\.",                 RegexOptions.IgnoreCase)] private static partial Regex FirehoseHostRegex();
    [GeneratedRegex(@"kinesis-firehose\.",         RegexOptions.IgnoreCase)] private static partial Regex KinesisFirehoseHostRegex();
    [GeneratedRegex(@"apigateway\.",               RegexOptions.IgnoreCase)] private static partial Regex ApiGatewayHostRegex();
    [GeneratedRegex(@"execute-api\.",              RegexOptions.IgnoreCase)] private static partial Regex ExecuteApiHostRegex();
    [GeneratedRegex(@"route53\.",                  RegexOptions.IgnoreCase)] private static partial Regex Route53HostRegex();
    [GeneratedRegex(@"cognito-idp\.",              RegexOptions.IgnoreCase)] private static partial Regex CognitoIdpHostRegex();
    [GeneratedRegex(@"cognito-identity\.",         RegexOptions.IgnoreCase)] private static partial Regex CognitoIdentityHostRegex();
    [GeneratedRegex(@"elasticmapreduce\.",         RegexOptions.IgnoreCase)] private static partial Regex EmrHostRegex();
    [GeneratedRegex(@"elasticfilesystem\.",        RegexOptions.IgnoreCase)] private static partial Regex EfsHostRegex();
    [GeneratedRegex(@"api\.ecr\.",                 RegexOptions.IgnoreCase)] private static partial Regex EcrApiHostRegex();
    [GeneratedRegex(@"ecr\.",                      RegexOptions.IgnoreCase)] private static partial Regex EcrHostRegex();
    [GeneratedRegex(@"ec2\.",                      RegexOptions.IgnoreCase)] private static partial Regex Ec2HostRegex();
    [GeneratedRegex(@"elasticloadbalancing\.",     RegexOptions.IgnoreCase)] private static partial Regex ElbHostRegex();
    [GeneratedRegex(@"acm\.",                      RegexOptions.IgnoreCase)] private static partial Regex AcmHostRegex();
    [GeneratedRegex(@"wafv2\.",                    RegexOptions.IgnoreCase)] private static partial Regex WafV2HostRegex();
    [GeneratedRegex(@"cloudformation\.",           RegexOptions.IgnoreCase)] private static partial Regex CloudFormationHostRegex();
    [GeneratedRegex(@"kms\.",                      RegexOptions.IgnoreCase)] private static partial Regex KmsHostRegex();
    [GeneratedRegex(@"cloudfront\.",               RegexOptions.IgnoreCase)] private static partial Regex CloudFrontHostRegex();
    [GeneratedRegex(@"appsync\.",                  RegexOptions.IgnoreCase)] private static partial Regex AppSyncHostRegex();
    [GeneratedRegex(@"servicediscovery\.",         RegexOptions.IgnoreCase)] private static partial Regex ServiceDiscoveryHostRegex();
    [GeneratedRegex(@"s3files\.",                  RegexOptions.IgnoreCase)] private static partial Regex S3FilesHostRegex();
    [GeneratedRegex(@"rds-data\.",                 RegexOptions.IgnoreCase)] private static partial Regex RdsDataHostRegex();

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>Detect the target service for a request.</summary>
    internal string DetectService(ServiceRequest request)
    {
        var host   = request.GetHeader("host") ?? "";
        var target = request.GetHeader("x-amz-target") ?? "";
        var auth   = request.GetHeader("authorization") ?? "";
        var path   = request.Path;

        // 1. X-Amz-Target header (most reliable for JSON-protocol services)
        if (!string.IsNullOrEmpty(target))
        {
            foreach (var (prefix, service) in TargetPrefixes)
                if (target.StartsWith(prefix, StringComparison.Ordinal))
                    return service;
        }

        // 2. Authorization credential scope
        if (!string.IsNullOrEmpty(auth))
        {
            var scopeMatch = CredentialScopeRegex().Match(auth);
            if (scopeMatch.Success)
            {
                var svcName = scopeMatch.Groups[1].Value;
                if (ScopeMap.TryGetValue(svcName, out var mapped))
                    return mapped;
            }
        }

        // 3. Action query param (SQS, SNS, IAM, STS, CloudWatch, SES, RDS, EC2, ELB, CFN)
        var action = request.GetQueryParam("Action") ?? "";
        if (!string.IsNullOrEmpty(action) && ActionServiceMap.TryGetValue(action, out var actionService))
            return actionService;

        // 4. URL path patterns
        var pathLower = path.ToLowerInvariant();

        if (pathLower.StartsWith("/v1/apis", StringComparison.Ordinal)
            || pathLower.StartsWith("/v1/tags/arn:aws:appsync", StringComparison.Ordinal))
            return "appsync";
        if (pathLower.StartsWith("/2020-05-31/", StringComparison.Ordinal))
            return "cloudfront";
        if (pathLower.StartsWith("/2013-04-01/", StringComparison.Ordinal))
            return "route53";
        if (pathLower.StartsWith("/v2/apis", StringComparison.Ordinal))
            return "apigateway";
        if (pathLower.StartsWith("/restapis", StringComparison.Ordinal)
            || pathLower.StartsWith("/apikeys", StringComparison.Ordinal)
            || pathLower.StartsWith("/usageplans", StringComparison.Ordinal)
            || pathLower.StartsWith("/domainnames", StringComparison.Ordinal))
            return "apigateway";
        if (pathLower.StartsWith("/2015-03-31/functions", StringComparison.Ordinal))
            return "lambda";
        if (pathLower.StartsWith("/oauth2/token", StringComparison.Ordinal))
            return "cognito-idp";
        if (pathLower.StartsWith("/clusters", StringComparison.Ordinal)
            || pathLower.StartsWith("/taskdefinitions", StringComparison.Ordinal)
            || pathLower.StartsWith("/tasks", StringComparison.Ordinal)
            || pathLower.StartsWith("/services", StringComparison.Ordinal)
            || pathLower.StartsWith("/stoptask", StringComparison.Ordinal))
            return "ecs";
        if (pathLower.Contains("/service/", StringComparison.Ordinal)
            && pathLower.Contains("/operation/", StringComparison.Ordinal)
            && (pathLower.Contains("granite", StringComparison.Ordinal)
                || pathLower.Contains("cloudwatch", StringComparison.Ordinal)))
            return "monitoring";

        // 5. Host header patterns
        foreach (var (pattern, service) in HostPatterns)
            if (pattern.IsMatch(host))
                return service;

        // 6. Default → S3
        return "s3";
    }

    /// <summary>Extract the AWS region from the Authorization header.</summary>
    internal static string ExtractRegion(ServiceRequest request)
    {
        var auth = request.GetHeader("authorization") ?? "";
        var m = RegionRegex().Match(auth);
        return m.Success
            ? m.Groups[1].Value
            : (Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1");
    }

    /// <summary>Extract the AWS access key ID from the Authorization header.</summary>
    internal static string ExtractAccessKeyId(ServiceRequest request)
    {
        var auth = request.GetHeader("authorization") ?? "";
        if (string.IsNullOrEmpty(auth)) return "";
        var m = AccessKeyRegex().Match(auth);
        return m.Success ? m.Groups[1].Value : "";
    }
}
