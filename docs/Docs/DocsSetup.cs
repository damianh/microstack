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
                    new SidebarItem { Label = "Overview",       Link = "/docs/services/overview" },
                    new SidebarItem { Label = "S3",             Link = "/docs/services/s3" },
                    new SidebarItem { Label = "SQS",            Link = "/docs/services/sqs" },
                    new SidebarItem { Label = "DynamoDB",       Link = "/docs/services/dynamodb" },
                    new SidebarItem { Label = "Lambda",         Link = "/docs/services/lambda" },
                    new SidebarItem { Label = "API Gateway",    Link = "/docs/services/api-gateway" },
                    new SidebarItem { Label = "Step Functions",  Link = "/docs/services/step-functions" },
                    new SidebarItem { Label = "All Services",   Link = "/docs/services/all" },
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
