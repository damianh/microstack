using System.Reflection;

namespace MicroStack.Internal;

/// <summary>
/// The MicroStack release version (YYYY.MMDD.N for releases), as stamped into the server assembly at build time.
/// </summary>
internal static class MicroStackVersion
{
    public static string Current { get; } = Resolve();

    private static string Resolve()
    {
        var informational = typeof(MicroStackVersion).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (string.IsNullOrEmpty(informational))
        {
            throw new InvalidOperationException("The MicroStack assembly has no informational version.");
        }

        // Drop build metadata such as the source revision ("+<commit>").
        var plus = informational.IndexOf('+');
        return plus >= 0 ? informational[..plus] : informational;
    }
}
