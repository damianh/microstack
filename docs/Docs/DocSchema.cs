using System.ComponentModel.DataAnnotations;

namespace Docs;

/// <summary>
/// Frontmatter schema for documentation content entries.
/// </summary>
public sealed class DocSchema
{
    [Required]
    public string Title { get; set; } = "";

    [Required]
    public string Description { get; set; } = "";

    public int Order { get; set; }

    public string Section { get; set; } = "";

    public List<string>? Topics { get; set; }

    public string? Head { get; set; }
}
