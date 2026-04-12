using Atoll.Build.Content.Collections;
using Atoll.Build.Content.Markdown;
using Atoll.Lagoon.Components;
using Atoll.Lagoon.Islands;
using Atoll.Lagoon.Markdown;

namespace Docs;

/// <summary>
/// Content collection configuration for the MicroStack documentation site.
/// </summary>
public sealed class ContentConfig : IContentConfiguration
{
    public CollectionConfig Configure()
    {
        var config = new CollectionConfig("Content")
            .AddCollection(ContentCollection.Define<DocSchema>("docs"));

        var markdownOptions = DocsMarkdownRenderer.CreateMarkdownOptions(DocsSetup.Config)
            ?? new MarkdownOptions();

        markdownOptions.Components = new ComponentMap()
            .Add<Aside>("aside")
            .Add<Card>("card")
            .Add<CardGrid>("card-grid")
            .Add<Steps>("steps")
            .Add<LinkCard>("link-card")
            .Add<LinkButton>("link-button")
            .Add<Icon>("icon")
            .Add<Tabs>("tabs")
            .Add<TabItem>("tab-item");

        config.Markdown = markdownOptions;

        return config;
    }
}
