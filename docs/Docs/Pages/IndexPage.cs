using Atoll.Build.Content.Collections;
using Atoll.Components;
using Atoll.Routing;
using Atoll.Lagoon.Components;
using Docs.Layouts;

namespace Docs.Pages;

/// <summary>
/// Landing page with hero section.
/// </summary>
[Layout(typeof(SplashSiteLayout))]
[PageRoute("/")]
public sealed class IndexPage : AtollComponent, IAtollPage
{
    [Parameter(Required = true)]
    public CollectionQuery Query { get; set; } = null!;

    protected override async Task RenderCoreAsync(RenderContext context)
    {
        var actions = new List<HeroAction>
        {
            new("Get Started", "/docs/getting-started", HeroActionVariant.Primary),
            new("View on GitHub", "https://github.com/damianh/microstack", HeroActionVariant.Secondary),
        };

        var heroFragment = ComponentRenderer.ToFragment<Hero>(new Dictionary<string, object?>
        {
            ["Title"] = "MicroStack",
            ["Tagline"] = "A lightweight local AWS service emulator for .NET. 39 services, single port, zero dependencies. Build and test AWS integrations without the cloud.",
            ["Actions"] = (IReadOnlyList<HeroAction>)actions,
        });

        await RenderAsync(heroFragment);
    }
}
