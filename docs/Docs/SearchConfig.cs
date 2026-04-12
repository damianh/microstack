using Atoll.Build.Content.Collections;
using Atoll.Lagoon.Search;
using Docs.Pages;

namespace Docs;

/// <summary>
/// Search index configuration for the MicroStack documentation site.
/// </summary>
public sealed class SearchConfig : ISearchIndexConfiguration
{
    public IEnumerable<SearchDocumentInput> GetDocuments(CollectionQuery query)
    {
        var docs = query.GetCollection<DocSchema>("docs");
        foreach (var entry in docs)
        {
            if (entry.Slug == DocsPage.NotFoundSlug)
            {
                continue;
            }

            var rendered = query.Render(entry);
            yield return new SearchDocumentInput(entry.Data.Title, $"/docs/{entry.Slug}")
            {
                Description = entry.Data.Description,
                Section = entry.Data.Section.Length > 0 ? entry.Data.Section : null,
                HtmlBody = rendered.Html,
                Topics = entry.Data.Topics is { Count: > 0 } topics ? topics : [],
            };
        }
    }
}
