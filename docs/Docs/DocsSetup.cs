using Atoll.Lagoon.Configuration;

namespace Docs;

/// <summary>
/// Lagoon theme configuration for the MicroStack documentation site.
/// </summary>
public static class DocsSetup
{
    public static DocsConfig Config { get; } = new DocsConfig
    {
        Title = "MicroStack",
        Description = "A lightweight local AWS service emulator for .NET.",
        BasePath = "/microstack",
        LogoSrc = "/logo.png",
        LogoAlt = "MicroStack",
        FaviconHref = "/logo.png",
        EnableSyntaxHighlighting = true,
        Social =
        [
            new SocialLink("GitHub", "https://github.com/damianh/microstack", SocialIcon.GitHub),
        ],
        Sidebar =
        [
            new SidebarItem
            {
                Label = "Getting Started",
                Link = "/docs/getting-started",
            },
            new SidebarItem
            {
                Label = "Guides",
                Items =
                [
                    new SidebarItem { Label = "Configuration",  Link = "/docs/configuration" },
                    new SidebarItem { Label = "Docker",         Link = "/docs/docker" },
                    new SidebarItem { Label = "Testing",        Link = "/docs/testing" },
                ],
            },
            new SidebarItem
            {
                Label = "Services",
                Items =
                [
                    new SidebarItem { Label = "Overview",          Link = "/docs/services/overview" },
                    new SidebarItem { Label = "ACM",               Link = "/docs/services/acm" },
                    new SidebarItem { Label = "ALB (ELBv2)",       Link = "/docs/services/alb" },
                    new SidebarItem { Label = "API Gateway",       Link = "/docs/services/api-gateway" },
                    new SidebarItem { Label = "AppSync",           Link = "/docs/services/appsync" },
                    new SidebarItem { Label = "Athena",            Link = "/docs/services/athena" },
                    new SidebarItem { Label = "CloudFormation",    Link = "/docs/services/cloudformation" },
                    new SidebarItem { Label = "CloudFront",        Link = "/docs/services/cloudfront" },
                    new SidebarItem { Label = "CloudWatch Logs",   Link = "/docs/services/cloudwatch-logs" },
                    new SidebarItem { Label = "CloudWatch Metrics", Link = "/docs/services/cloudwatch" },
                    new SidebarItem { Label = "Cognito",           Link = "/docs/services/cognito" },
                    new SidebarItem { Label = "DynamoDB",          Link = "/docs/services/dynamodb" },
                    new SidebarItem { Label = "EC2",               Link = "/docs/services/ec2" },
                    new SidebarItem { Label = "ECR",               Link = "/docs/services/ecr" },
                    new SidebarItem { Label = "ECS",               Link = "/docs/services/ecs" },
                    new SidebarItem { Label = "EFS",               Link = "/docs/services/efs" },
                    new SidebarItem { Label = "ElastiCache",       Link = "/docs/services/elasticache" },
                    new SidebarItem { Label = "EMR",               Link = "/docs/services/emr" },
                    new SidebarItem { Label = "EventBridge",       Link = "/docs/services/eventbridge" },
                    new SidebarItem { Label = "Firehose",          Link = "/docs/services/firehose" },
                    new SidebarItem { Label = "Glue",              Link = "/docs/services/glue" },
                    new SidebarItem { Label = "IAM",               Link = "/docs/services/iam" },
                    new SidebarItem { Label = "Kinesis",           Link = "/docs/services/kinesis" },
                    new SidebarItem { Label = "KMS",               Link = "/docs/services/kms" },
                    new SidebarItem { Label = "Lambda",            Link = "/docs/services/lambda" },
                    new SidebarItem { Label = "RDS",               Link = "/docs/services/rds" },
                    new SidebarItem { Label = "RDS Data API",      Link = "/docs/services/rds-data" },
                    new SidebarItem { Label = "Route 53",          Link = "/docs/services/route53" },
                    new SidebarItem { Label = "S3",                Link = "/docs/services/s3" },
                    new SidebarItem { Label = "S3Files",           Link = "/docs/services/s3files" },
                    new SidebarItem { Label = "Secrets Manager",   Link = "/docs/services/secrets-manager" },
                    new SidebarItem { Label = "Service Discovery", Link = "/docs/services/service-discovery" },
                    new SidebarItem { Label = "SES",               Link = "/docs/services/ses" },
                    new SidebarItem { Label = "SNS",               Link = "/docs/services/sns" },
                    new SidebarItem { Label = "SQS",               Link = "/docs/services/sqs" },
                    new SidebarItem { Label = "SSM Parameter Store", Link = "/docs/services/ssm" },
                    new SidebarItem { Label = "Step Functions",    Link = "/docs/services/step-functions" },
                    new SidebarItem { Label = "STS",               Link = "/docs/services/sts" },
                    new SidebarItem { Label = "WAF v2",            Link = "/docs/services/waf" },
                ],
            },
            new SidebarItem
            {
                Label = "Architecture",
                Items =
                [
                    new SidebarItem { Label = "Design",         Link = "/docs/architecture/design" },
                    new SidebarItem { Label = "Persistence",    Link = "/docs/architecture/persistence" },
                    new SidebarItem { Label = "Multi-Tenancy",  Link = "/docs/architecture/multi-tenancy" },
                ],
            },
        ],
    };
}
