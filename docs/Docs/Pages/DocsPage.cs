using Atoll.Build.Content.Collections;
using Atoll.Build.Content.Markdown;
using Atoll.Components;
using Atoll.Routing;
using Docs.Layouts;

namespace Docs.Pages;

/// <summary>
/// Individual documentation page. Renders a Markdown entry identified by URL slug.
/// Route: /docs/[...slug]
/// </summary>
[Layout(typeof(SiteLayout))]
[PageRoute("/docs/[...slug]")]
public sealed class DocsPage : AtollComponent, IAtollPage, IStaticPathsProvider, IPageStatusCodeProvider
{
    internal const string NotFoundSlug = "404";

    [Parameter(Required = true)]
    public string Slug { get; set; } = "";

    [Parameter(Required = true)]
    public CollectionQuery Query { get; set; } = null!;

    [Parameter]
    public string PageTitle { get; set; } = "";

    [Parameter]
    public string? PageDescription { get; set; }

    [Parameter]
    public IReadOnlyList<MarkdownHeading> Headings { get; set; } = [];

    public int ResponseStatusCode { get; private set; } = 200;

    public Task<IReadOnlyList<StaticPath>> GetStaticPathsAsync()
    {
        var docs = Query.GetCollection<DocSchema>("docs");
        var paths = docs
            .Where(entry => entry.Slug != NotFoundSlug)
            .Select(entry => new StaticPath(
                new Dictionary<string, string> { ["slug"] = entry.Slug }))
            .ToList();

        return Task.FromResult<IReadOnlyList<StaticPath>>(paths);
    }

    protected override async Task RenderCoreAsync(RenderContext context)
    {
        var entry = Query.GetEntry<DocSchema>("docs", Slug);

        if (entry is null)
        {
            ResponseStatusCode = 404;
            PageTitle = "Page Not Found";
            WriteHtml("""
                <div class="not-found">
                  <h1>Page Not Found</h1>
                  <p>Sorry, we couldn&rsquo;t find the page you&rsquo;re looking for.</p>
                  <p><a href="/docs/getting-started">Return to the documentation</a></p>
                </div>
                """);
            return;
        }

        var renderedEntry = Query.Render(entry);
        PageTitle = entry.Data.Title;
        PageDescription = entry.Data.Description;
        Headings = renderedEntry.Headings;

        var contentComponent = ContentComponent.FromRenderedContent(renderedEntry);
        await contentComponent.RenderAsync(context);
    }
}
