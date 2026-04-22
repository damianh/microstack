using System.Diagnostics;
using System.Text;

namespace MicroStack.Tests;

/// <summary>
/// Publishes the MicroStack container image via WSL (native AOT requires Linux),
/// starts the container, verifies startup via console log output, and hits the health endpoint.
/// </summary>
public sealed class ContainerSmokeTests : IAsyncLifetime
{
    private const string ImageTag = "microstack-smoketest:test";

    private string? _containerId;
    private readonly List<string> _logs = [];

    public async ValueTask InitializeAsync()
    {
        var repoRoot = FindRepoRoot();
        var wslPath = ToWslPath(repoRoot);

        // Step 1: Publish native AOT binary in WSL (cross-compile requires Linux toolchain).
        var publish = await RunProcessAsync(
            "wsl",
            $"bash -c 'export DOTNET_ROOT=$HOME/.dotnet && export PATH=$DOTNET_ROOT:$PATH && cd \"{wslPath}\" && dotnet publish src/MicroStack/MicroStack.csproj -r linux-x64 -c Release --nologo -v:q'",
            timeoutSeconds: 600);

        publish.ExitCode.ShouldBe(0, $"dotnet publish via WSL failed:\n{publish.Output}");

        // Step 2: Build Docker image from Windows using the published binary.
        var publishDir = Path.Combine(repoRoot, "src", "MicroStack", "bin", "Release", "net10.0", "linux-x64", "publish");
        Directory.Exists(publishDir).ShouldBeTrue($"Publish directory not found: {publishDir}");

        var dockerfilePath = Path.Combine(publishDir, "Dockerfile");
        File.WriteAllText(dockerfilePath,
            """
            FROM docker.io/debian:bookworm-slim
            RUN apt-get update -qq && apt-get install -y --no-install-recommends libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
            COPY MicroStack /app/MicroStack
            RUN chmod +x /app/MicroStack
            ENV ASPNETCORE_HTTP_PORTS=4566
            EXPOSE 4566
            ENTRYPOINT ["/app/MicroStack"]
            """);

        var build = await RunProcessAsync(
            "docker",
            $"build -t {ImageTag} \"{publishDir}\"",
            timeoutSeconds: 120);

        build.ExitCode.ShouldBe(0, $"docker build failed:\n{build.Output}");
    }

    public async ValueTask DisposeAsync()
    {
        if (_containerId is not null)
        {
            await RunProcessAsync("docker", $"rm -f {_containerId}", timeoutSeconds: 10);
        }

        await RunProcessAsync("docker", $"rmi -f {ImageTag}", timeoutSeconds: 10);
    }

    [Fact]
    public async Task ContainerStartsSuccessfully()
    {
        // Start container in detached mode with an ephemeral host port
        var run = await RunProcessAsync(
            "docker",
            $"run -d -p 0:4566 {ImageTag}",
            timeoutSeconds: 30);

        run.ExitCode.ShouldBe(0, $"docker run failed:\n{run.Output}");
        _containerId = run.Output.Trim();
        _containerId.ShouldNotBeNullOrWhiteSpace();

        // Resolve the mapped host port
        var portResult = await RunProcessAsync(
            "docker",
            $"port {_containerId} 4566",
            timeoutSeconds: 10);

        portResult.ExitCode.ShouldBe(0, $"docker port failed:\n{portResult.Output}");

        // Parse "0.0.0.0:XXXXX" or ":::XXXXX" → extract port number
        var portLine = portResult.Output
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .First(l => l.Contains("0.0.0.0") || l.Contains(":::"));
        var hostPort = int.Parse(portLine[(portLine.LastIndexOf(':') + 1)..].Trim());

        // Poll container logs for the ASP.NET Core startup message
        var started = await WaitForLogAsync("Now listening on:", timeoutSeconds: 60);

        started.ShouldBeTrue(
            $"Container did not emit 'Now listening on:' within timeout.\nCaptured logs:\n{string.Join('\n', _logs)}");

        // Hit the health endpoint to confirm the service is responsive
        using var httpClient = new HttpClient
        {
            BaseAddress = new Uri($"http://localhost:{hostPort}"),
        };
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        HttpResponseMessage? healthResponse = null;
        while (!cts.Token.IsCancellationRequested)
        {
            try
            {
                healthResponse = await httpClient.GetAsync("/_microstack/health", cts.Token);
                if (healthResponse.IsSuccessStatusCode)
                {
                    break;
                }
            }
            catch (HttpRequestException)
            {
                // Container not ready yet — retry
            }
            catch (TaskCanceledException)
            {
                break;
            }

            await Task.Delay(500, cts.Token);
        }

        healthResponse.ShouldNotBeNull("Health endpoint never responded");
        healthResponse.StatusCode.ShouldBe(HttpStatusCode.OK);

        var body = await healthResponse.Content.ReadFromJsonAsync<JsonElement>();
        body.GetProperty("edition").GetString().ShouldBe("light");
    }

    private async Task<bool> WaitForLogAsync(string marker, int timeoutSeconds)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));

        while (!cts.Token.IsCancellationRequested)
        {
            var logResult = await RunProcessAsync(
                "docker",
                $"logs {_containerId}",
                timeoutSeconds: 10);

            if (logResult.ExitCode == 0)
            {
                var lines = logResult.Output.Split('\n');
                _logs.Clear();
                _logs.AddRange(lines);

                if (lines.Any(l => l.Contains(marker, StringComparison.OrdinalIgnoreCase)))
                {
                    return true;
                }
            }

            try
            {
                await Task.Delay(1000, cts.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        return false;
    }

    private static string ToWslPath(string windowsPath)
    {
        // D:\repos\foo → /mnt/d/repos/foo
        var drive = char.ToLowerInvariant(windowsPath[0]);
        var rest = windowsPath[2..].Replace('\\', '/');
        return $"/mnt/{drive}{rest}";
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);

        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "MicroStack.slnx")))
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        throw new InvalidOperationException(
            "Could not find repo root (looked for MicroStack.slnx). Ensure the test runs from within the repository.");
    }

    private static async Task<ProcessResult> RunProcessAsync(
        string fileName,
        string arguments,
        int timeoutSeconds)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = fileName,
                Arguments = arguments,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            },
        };

        var stdout = new StringBuilder();
        var stderr = new StringBuilder();

        process.OutputDataReceived += (_, e) =>
        {
            if (e.Data is not null)
            {
                stdout.AppendLine(e.Data);
            }
        };
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is not null)
            {
                stderr.AppendLine(e.Data);
            }
        };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
        try
        {
            await process.WaitForExitAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
        }

        return new ProcessResult(process.ExitCode, $"{stdout}{stderr}");
    }

    private sealed record ProcessResult(int ExitCode, string Output);
}
