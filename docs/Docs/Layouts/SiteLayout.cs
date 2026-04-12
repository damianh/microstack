using Atoll.Build.Content.Collections;
using Atoll.Build.Content.Markdown;
using Atoll.Components;
using Atoll.Lagoon.Navigation;
using Atoll.Slots;
using Docs.Pages;
using AddonLayout = Atoll.Lagoon.Layouts.DocsLayout;

namespace Docs.Layouts;

/// <summary>
/// Site-specific wrapper layout for documentation pages.
/// Wires <see cref="DocsSetup.Config"/> into the Atoll.Lagoon DocsLayout.
/// </summary>
public sealed class SiteLayout : AtollComponent
{
    [Parameter(Required = true)]
    public CollectionQuery Query { get; set; } = null!;

    [Parameter]
    public string Slug { get; set; } = "";

    [Parameter]
    public string PageTitle { get; set; } = "";

    [Parameter]
    public string? PageDescription { get; set; }

    protected override async Task RenderCoreAsync(RenderContext context)
    {
        var config = DocsSetup.Config;
        var currentHref = string.IsNullOrEmpty(Slug) ? "/" : $"/docs/{Slug}";

        var currentEntry = !string.IsNullOrEmpty(Slug)
            ? Query.GetEntry<DocSchema>("docs", Slug)
            : null;

        var headings = currentEntry is not null
            ? Query.Render(currentEntry).Headings
            : (IReadOnlyList<MarkdownHeading>)[];

        var entries = Query.GetCollection<DocSchema>("docs")
            .Where(e => e.Slug != DocsPage.NotFoundSlug)
            .Select(e => new SidebarEntry(e.Data.Title, $"/docs/{e.Slug}", e.Slug, e.Data.Order, null))
            .ToList();

        var sidebarItems = new SidebarBuilder(config.Sidebar, entries).Build(currentHref);
        var pagination = new PaginationResolver(sidebarItems, flatten: true).Resolve(currentHref);
        var breadcrumbs = new BreadcrumbBuilder(sidebarItems).Build(currentHref);

        var pageSlot = context.Slots.GetSlotFragment(SlotCollection.DefaultSlotName);

        var addonProps = new Dictionary<string, object?>
        {
            ["Config"] = config,
            ["PageTitle"] = currentEntry?.Data.Title ?? PageTitle,
            ["PageDescription"] = currentEntry?.Data.Description ?? PageDescription,
            ["Headings"] = headings,
            ["SidebarItems"] = sidebarItems,
            ["Previous"] = pagination.Previous,
            ["Next"] = pagination.Next,
            ["BreadcrumbItems"] = breadcrumbs,
            ["PageHeadContent"] = currentEntry?.Data.Head,
        };

        var addonSlots = SlotCollection.FromDefault(pageSlot);
        await RenderAsync(ComponentRenderer.ToFragment<AddonLayout>(addonProps, addonSlots));
    }
}
