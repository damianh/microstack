using System.Reflection;
using System.Runtime.Loader;

namespace MicroStack.LambdaBootstrap;

/// <summary>
/// Custom <see cref="AssemblyLoadContext"/> that resolves handler assemblies and their dependencies
/// from a flat code directory (the extracted Lambda deployment ZIP).
///
/// Resolution order:
///   1. Try to load the assembly from the code directory (flat layout typical of Lambda ZIPs).
///   2. Fall back to the default context for framework and shared assemblies.
/// </summary>
internal sealed class LambdaAssemblyLoadContext : AssemblyLoadContext
{
    private readonly string _codeDir;

    internal LambdaAssemblyLoadContext(string codeDir) : base(isCollectible: false)
    {
        _codeDir = codeDir;
    }

    protected override Assembly? Load(AssemblyName assemblyName)
    {
        if (assemblyName.Name is null)
        {
            return null;
        }

        // Check if the assembly DLL exists in the code directory
        var candidatePath = Path.Combine(_codeDir, assemblyName.Name + ".dll");
        if (File.Exists(candidatePath))
        {
            return LoadFromAssemblyPath(candidatePath);
        }

        // Fall back to default context (framework assemblies, shared runtimes, etc.)
        return null;
    }

    protected override nint LoadUnmanagedDll(string unmanagedDllName)
    {
        // Try to find native libraries in the code directory
        var candidatePath = Path.Combine(_codeDir, unmanagedDllName);
        if (File.Exists(candidatePath))
        {
            return LoadUnmanagedDllFromPath(candidatePath);
        }

        // Platform-specific suffixes
        string[] candidates =
        [
            Path.Combine(_codeDir, unmanagedDllName + ".dll"),   // Windows
            Path.Combine(_codeDir, "lib" + unmanagedDllName + ".so"),  // Linux
            Path.Combine(_codeDir, "lib" + unmanagedDllName + ".dylib"), // macOS
        ];

        foreach (var path in candidates)
        {
            if (File.Exists(path))
            {
                return LoadUnmanagedDllFromPath(path);
            }
        }

        return nint.Zero;
    }
}
