using Xunit.MicrosoftTestingPlatform;

namespace MicroStack.Testcontainers.Example;

internal static class Program
{
    public static int Main(string[] args)
    {
        var platformArgs = args
            .Where(arg => !arg.Equals("--nologo", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        return TestPlatformTestFramework
            .RunAsync(platformArgs, SelfRegisteredExtensions.AddSelfRegisteredExtensions)
            .GetAwaiter()
            .GetResult();
    }
}
