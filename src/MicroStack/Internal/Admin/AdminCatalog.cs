using MicroStack.Admin.Contracts;

namespace MicroStack.Internal.Admin;

internal sealed record AdminCatalogEntry(
    string Id, string Name, string Label, string Category, string Icon,
    string CanonicalHandler, string? Variant = null, string Scope = "account");

internal static class AdminCatalog
{
    internal static readonly IReadOnlyList<AdminCatalogEntry> Entries =
    [
        E("sqs", "Simple Queue Service", "SQS", "Messaging & workflows", "sqs"),
        E("sns", "Simple Notification Service", "SNS", "Messaging & workflows", "sns"),
        E("events", "EventBridge", "EventBridge", "Messaging & workflows", "events"),
        E("ses", "SES", "SES", "Messaging & workflows", "ses"),
        E("stepfunctions", "Step Functions", "Step Functions", "Messaging & workflows", "stepfunctions", "states"),

        E("s3", "Simple Storage Service", "S3", "Storage & databases", "s3"),
        E("s3files", "S3 Files", "S3 Files", "Storage & databases", "s3files"),
        E("efs", "EFS", "EFS", "Storage & databases", "efs", "elasticfilesystem"),
        E("dynamodb", "DynamoDB", "DynamoDB", "Storage & databases", "dynamodb"),
        E("rds", "RDS", "RDS", "Storage & databases", "rds"),
        E("rdsdata", "RDS Data", "RDS Data", "Storage & databases", "rdsdata", "rds-data"),
        E("elasticache", "ElastiCache", "ElastiCache", "Storage & databases", "elasticache"),

        E("lambda", "Lambda", "Lambda", "Compute & containers", "lambda"),
        E("ec2", "EC2", "EC2", "Compute & containers", "ec2"),
        E("ecs", "ECS", "ECS", "Compute & containers", "ecs"),
        E("ecr", "ECR", "ECR", "Compute & containers", "ecr"),

        E("apigateway", "API Gateway REST", "API Gateway REST", "Networking & delivery", "apigateway", variant: "rest"),
        E("apigatewayv2", "API Gateway HTTP / WebSocket", "API Gateway HTTP / WebSocket", "Networking & delivery", "apigatewayv2", "apigateway", "v2"),
        E("alb", "Application Load Balancer", "Application Load Balancer", "Networking & delivery", "alb", "elasticloadbalancing"),
        E("appsync", "AppSync", "AppSync", "Networking & delivery", "appsync"),
        E("cloudfront", "CloudFront", "CloudFront", "Networking & delivery", "cloudfront"),
        E("route53", "Route 53", "Route 53", "Networking & delivery", "route53"),
        E("servicediscovery", "Cloud Map", "Cloud Map", "Networking & delivery", "servicediscovery"),

        E("acm", "Certificate Manager", "Certificate Manager", "Security & identity", "acm"),
        E("cognitoidp", "Cognito User Pools", "Cognito User Pools", "Security & identity", "cognitoidp", "cognito-idp"),
        E("cognitoidentity", "Cognito Identity Pools", "Cognito Identity Pools", "Security & identity", "cognitoidentity", "cognito-identity"),
        E("iam", "IAM", "IAM", "Security & identity", "iam"),
        E("kms", "KMS", "KMS", "Security & identity", "kms"),
        E("secretsmanager", "Secrets Manager", "Secrets Manager", "Security & identity", "secretsmanager"),
        E("sts", "STS", "STS", "Security & identity", "sts"),
        E("waf", "WAF", "WAF", "Security & identity", "waf", "wafv2"),

        E("athena", "Athena", "Athena", "Analytics & streaming", "athena"),
        E("emr", "EMR", "EMR", "Analytics & streaming", "emr", "elasticmapreduce"),
        E("firehose", "Data Firehose", "Data Firehose", "Analytics & streaming", "firehose", scope: "global"),
        E("glue", "Glue", "Glue", "Analytics & streaming", "glue"),
        E("kinesis", "Kinesis", "Kinesis", "Analytics & streaming", "kinesis"),

        E("cloudformation", "CloudFormation", "CloudFormation", "Management & observability", "cloudformation"),
        E("cloudwatch", "CloudWatch", "CloudWatch", "Management & observability", "cloudwatch", "monitoring"),
        E("logs", "CloudWatch Logs", "CloudWatch Logs", "Management & observability", "logs"),
        E("ssm", "Systems Manager", "Systems Manager", "Management & observability", "ssm")
    ];

    internal static AdminCatalogEntry? Find(string id) =>
        Entries.FirstOrDefault(entry => string.Equals(entry.Id, id, StringComparison.OrdinalIgnoreCase));

    internal static AdminService Describe(AdminCatalogEntry entry, ServiceRegistry registry)
    {
        var handler = registry.Resolve(entry.CanonicalHandler);
        var source = handler as IAdminResourceSource;
        var available = handler is not null;
        return new(entry.Id, entry.Name, entry.Label, entry.Category, entry.Icon,
            entry.CanonicalHandler, available ? "available" : "disabled", entry.Scope,
            available ? source?.GetAdminNotice(entry.Id) : "This service is disabled.")
        {
            Kinds = source?.GetAdminResourceKinds(entry.Id) ?? []
        };
    }

    private static AdminCatalogEntry E(
        string id, string name, string label, string category, string icon,
        string? canonical = null, string? variant = null, string scope = "account") =>
        new(id, name, label, category, icon, canonical ?? id, variant, scope);
}
