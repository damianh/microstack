using System.Net;
using System.Net.Http.Headers;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text.RegularExpressions;
using System.Text.Json;

if (args is ["--test"])
{
    CalendarVersionTests.Run();
    CalendarVersionTests.ContainerSmokeTests.Run();
    return;
}

if (args.Length > 0 && args[0] == "--smoke-container")
{
    Environment.ExitCode = await ContainerSmokeCli.RunAsync(args[1..], Console.Out, Console.Error);
    return;
}

Environment.ExitCode = CalendarVersionCli.Run(args, Console.Out, Console.Error);

internal static class CalendarVersionCli
{
    public static int Run(string[] args, TextWriter output, TextWriter error)
    {
        try
        {
            var options = CalendarVersionOptions.Parse(args);
            var releaseDate = options.Date ?? DateOnly.FromDateTime(DateTime.UtcNow);
            var tags = options.TagsFile is null
                ? CalendarVersion.ReadRemoteTags()
                : File.ReadAllText(options.TagsFile);
            output.WriteLine(CalendarVersion.Next(releaseDate, tags));
            return 0;
        }
        catch (Exception exception) when (exception is ArgumentException or IOException or InvalidOperationException or UnauthorizedAccessException)
        {
            error.WriteLine($"calendar-version: {exception.Message}");
            return 1;
        }
    }
}

internal sealed record ContainerSmokeOptions(string Image, string? ExpectedVersion, int HostPort)
{
    public static ContainerSmokeOptions Parse(string[] args)
    {
        if (args.Length == 0 || string.IsNullOrWhiteSpace(args[0]) || args[0].StartsWith("--", StringComparison.Ordinal))
        {
            throw new ArgumentException("usage: --smoke-container IMAGE [--expected-version VERSION] [--port PORT]");
        }

        var image = args[0];
        var expectedVersion = Environment.GetEnvironmentVariable("MICROSTACK_EXPECTED_VERSION");
        if (string.IsNullOrEmpty(expectedVersion))
        {
            expectedVersion = null;
        }

        var configuredPort = Environment.GetEnvironmentVariable("MICROSTACK_SMOKE_API_PORT");
        var port = 4566;
        if (!string.IsNullOrEmpty(configuredPort) &&
            (!int.TryParse(configuredPort, out port) || port is < 1 or > 65535))
        {
            throw new ArgumentException("MICROSTACK_SMOKE_API_PORT must be between 1 and 65535");
        }

        for (var index = 1; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--expected-version":
                    if (++index >= args.Length || string.IsNullOrWhiteSpace(args[index]))
                    {
                        throw new ArgumentException("--expected-version requires a value");
                    }

                    expectedVersion = args[index];
                    break;

                case "--port":
                    if (++index >= args.Length || !int.TryParse(args[index], out port) || port is < 1 or > 65535)
                    {
                        throw new ArgumentException("--port requires a number between 1 and 65535");
                    }

                    break;

                default:
                    throw new ArgumentException($"unknown smoke-container argument: {args[index]}");
            }
        }

        if (port is < 1 or > 65535)
        {
            throw new ArgumentException("MICROSTACK_SMOKE_API_PORT must be between 1 and 65535");
        }

        return new ContainerSmokeOptions(image, expectedVersion, port);
    }
}

internal static class ContainerSmokeCli
{
    public static async Task<int> RunAsync(string[] args, TextWriter output, TextWriter error)
    {
        string? containerId = null;
        var passed = false;
        try
        {
            var options = ContainerSmokeOptions.Parse(args);
            containerId = await Docker.RunAsync(
                ["run", "--rm", "-d", "-p", $"127.0.0.1:{options.HostPort}:4566", options.Image]);
            await ContainerSmoke.RunAsync(options);
            output.WriteLine("API, same-port UI, deep links, framework, icons, and browser redirect passed.");
            passed = true;
            return 0;
        }
        catch (Exception exception) when (exception is ArgumentException or IOException or InvalidOperationException or HttpRequestException or JsonException or TaskCanceledException)
        {
            error.WriteLine($"container-smoke: {exception.Message}");
            return 1;
        }
        finally
        {
            if (containerId is not null)
            {
                if (!passed)
                {
                    try
                    {
                        error.WriteLine(await Docker.RunAsync(["logs", containerId]));
                    }
                    catch (Exception exception) when (exception is IOException or InvalidOperationException)
                    {
                        error.WriteLine($"container-smoke: could not read container logs: {exception.Message}");
                    }
                }

                try
                {
                    await Docker.RunAsync(["rm", "-f", containerId]);
                }
                catch (Exception exception) when (exception is IOException or InvalidOperationException)
                {
                    error.WriteLine($"container-smoke: could not remove container: {exception.Message}");
                }
            }
        }
    }
}

internal static class Docker
{
    public static async Task<string> RunAsync(string[] args)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo("docker")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            },
        };
        foreach (var argument in args)
        {
            process.StartInfo.ArgumentList.Add(argument);
        }

        try
        {
            if (!process.Start())
            {
                throw new InvalidOperationException("failed to start docker");
            }
        }
        catch (Exception exception) when (exception is InvalidOperationException or System.ComponentModel.Win32Exception)
        {
            throw new InvalidOperationException("failed to run docker", exception);
        }

        var outputTask = process.StandardOutput.ReadToEndAsync();
        var errorTask = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        var output = await outputTask;
        var error = await errorTask;
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"docker {string.Join(' ', args)} failed{(string.IsNullOrWhiteSpace(error) ? "" : $": {error.Trim()}")}");
        }

        return output.Trim();
    }
}

internal static class ContainerSmoke
{
    private static readonly string[] HealthPaths = ["/_microstack/health", "/_localstack/health", "/health"];

    public static async Task RunAsync(ContainerSmokeOptions options)
    {
        using var handler = new HttpClientHandler { AllowAutoRedirect = false };
        using var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(5) };
        var baseAddress = new Uri($"http://localhost:{options.HostPort}");

        var health = await GetWithRetryAsync(client, new Uri(baseAddress, HealthPaths[0]));
        if (options.ExpectedVersion is not null)
        {
            ValidateHealthVersion(HealthPaths[0], health, options.ExpectedVersion);
            foreach (var path in HealthPaths.Skip(1))
            {
                ValidateHealthVersion(path, await GetAsync(client, new Uri(baseAddress, path)), options.ExpectedVersion);
            }
        }

        var ui = await GetAsync(client, new Uri(baseAddress, "/ui/"));
        _ = await GetAsync(client, new Uri(baseAddress, "/_microstack/admin/v1/services"));

        var deepLink = await GetAsync(client, new Uri(baseAddress, "/ui/services/s3"));
        if (!deepLink.Contains("<html", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("deep link /ui/services/s3 did not return HTML");
        }

        var icon = await GetAsync(client, new Uri(baseAddress, "/ui/icons/s3.svg"));
        if (!icon.Contains("<svg", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("UI icon /ui/icons/s3.svg did not return SVG");
        }

        var frameworkMatch = Regex.Match(ui, "src=\"(?<path>_framework/[^\"]+)\"", RegexOptions.CultureInvariant);
        if (!frameworkMatch.Success)
        {
            throw new InvalidOperationException("UI did not reference a framework script");
        }

        using (var frameworkResponse = await client.GetAsync(
                   new Uri(baseAddress, $"/ui/{frameworkMatch.Groups["path"].Value}")))
        {
            frameworkResponse.EnsureSuccessStatusCode();
            var contentType = frameworkResponse.Content.Headers.ContentType?.MediaType ?? "";
            if (!contentType.Contains("javascript", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"framework script returned {contentType}, not JavaScript");
            }
        }

        using var redirectRequest = new HttpRequestMessage(HttpMethod.Get, baseAddress);
        redirectRequest.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/html"));
        using var redirectResponse = await client.SendAsync(redirectRequest);
        if (redirectResponse.StatusCode != HttpStatusCode.Found ||
            redirectResponse.Headers.Location?.OriginalString != "/ui/")
        {
            throw new InvalidOperationException("browser request to / did not redirect to /ui/");
        }
    }

    public static void ValidateHealthVersion(string path, string json, string expectedVersion)
    {
        using var document = JsonDocument.Parse(json);
        var version = document.RootElement.ValueKind == JsonValueKind.Object &&
                      document.RootElement.TryGetProperty("version", out var versionElement) &&
                      versionElement.ValueKind == JsonValueKind.String
            ? versionElement.GetString()
            : null;
        if (!string.Equals(version, expectedVersion, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"{path} reported an unexpected version (expected {expectedVersion}, got {version ?? "missing"}): {json}");
        }
    }

    private static async Task<string> GetAsync(HttpClient client, Uri uri)
    {
        using var response = await client.GetAsync(uri);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync();
    }

    private static async Task<string> GetWithRetryAsync(HttpClient client, Uri uri)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        Exception? lastError = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                return await GetAsync(client, uri);
            }
            catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException)
            {
                lastError = exception;
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }

        throw new IOException($"container did not become ready at {uri.PathAndQuery}", lastError);
    }
}

internal sealed record CalendarVersionOptions(DateOnly? Date, string? TagsFile)
{
    public static CalendarVersionOptions Parse(string[] args)
    {
        DateOnly? date = null;
        string? tagsFile = null;

        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--date":
                    if (++index >= args.Length || string.IsNullOrWhiteSpace(args[index]))
                    {
                        throw new ArgumentException("--date requires a value");
                    }

                    if (!DateOnly.TryParseExact(args[index], "yyyy-MM-dd", CultureInfo.InvariantCulture,
                            DateTimeStyles.None, out var parsedDate))
                    {
                        throw new ArgumentException(
                            $"invalid date '{args[index]}'; expected a valid date in YYYY-MM-DD format");
                    }

                    if (parsedDate.Year < 1000)
                    {
                        throw new ArgumentException("release year must be a four-digit version component");
                    }

                    date = parsedDate;
                    break;

                case "--tags-file":
                    if (++index >= args.Length || string.IsNullOrWhiteSpace(args[index]))
                    {
                        throw new ArgumentException("--tags-file requires a value");
                    }

                    tagsFile = args[index];
                    break;

                default:
                    throw new ArgumentException($"unknown argument: {args[index]}");
            }
        }

        return new CalendarVersionOptions(date, tagsFile);
    }
}

internal static partial class CalendarVersion
{
    private const int MaximumComponent = 65534;

    [GeneratedRegex(@"^v(?<year>[1-9][0-9]{3})\.(?<monthDay>[1-9][0-9]{2,3})\.(?<counter>[1-9][0-9]*)$",
        RegexOptions.CultureInvariant)]
    private static partial Regex CalendarTagPattern();

    public static string Next(DateOnly releaseDate, string tags)
    {
        if (releaseDate.Year < 1000)
        {
            throw new ArgumentException("release year must be a four-digit version component");
        }

        var releases = ParseTags(tags);
        var latest = releases
            .OrderBy(release => release.Date)
            .ThenBy(release => release.Counter)
            .LastOrDefault();

        if (latest is not null && releaseDate < latest.Date)
        {
            throw new InvalidOperationException(
                $"release date {releaseDate:yyyy-MM-dd} is earlier than the latest release v{latest.Tag}");
        }

        var counter = BigInteger.One;
        if (latest is not null && releaseDate == latest.Date)
        {
            if (latest.Counter >= MaximumComponent)
            {
                throw new InvalidOperationException(
                    $"release counter for {releaseDate:yyyy-MM-dd} exceeds the .NET version component limit");
            }

            counter = latest.Counter + BigInteger.One;
        }

        var monthDay = releaseDate.Month * 100 + releaseDate.Day;
        return string.Create(CultureInfo.InvariantCulture, $"{releaseDate.Year}.{monthDay}.{counter}");
    }

    public static string ReadRemoteTags(string remote = "origin")
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo("git", ["ls-remote", "--tags", "--refs", remote])
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            },
        };

        try
        {
            if (!process.Start())
            {
                throw new InvalidOperationException($"failed to start git while listing tags from {remote}");
            }
        }
        catch (Exception exception) when (exception is InvalidOperationException or System.ComponentModel.Win32Exception)
        {
            throw new InvalidOperationException($"failed to list tags from {remote}", exception);
        }

        var outputTask = process.StandardOutput.ReadToEndAsync();
        var errorTask = process.StandardError.ReadToEndAsync();
        process.WaitForExit();
        var output = outputTask.GetAwaiter().GetResult();
        var error = errorTask.GetAwaiter().GetResult();
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"failed to list tags from {remote}{(string.IsNullOrWhiteSpace(error) ? "" : $": {error.Trim()}")}");
        }

        return output;
    }

    private static List<ReleaseTag> ParseTags(string tags)
    {
        var releases = new List<ReleaseTag>();
        foreach (var line in tags.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var tab = line.LastIndexOf('\t');
            var tag = tab >= 0 ? line[(tab + 1)..].TrimEnd('\r') : line.TrimEnd('\r');
            if (tag.StartsWith("refs/tags/", StringComparison.Ordinal))
            {
                tag = tag["refs/tags/".Length..];
            }

            if (tag.EndsWith("^{}", StringComparison.Ordinal))
            {
                tag = tag[..^3];
            }

            var match = CalendarTagPattern().Match(tag);
            if (!match.Success ||
                !int.TryParse(match.Groups["year"].Value, NumberStyles.None, CultureInfo.InvariantCulture,
                    out var year) ||
                !int.TryParse(match.Groups["monthDay"].Value, NumberStyles.None, CultureInfo.InvariantCulture,
                    out var monthDay) ||
                !BigInteger.TryParse(match.Groups["counter"].Value, NumberStyles.None, CultureInfo.InvariantCulture,
                    out var counter))
            {
                continue;
            }

            var month = monthDay / 100;
            var day = monthDay % 100;
            if (year < 1000 || month is < 1 or > 12 || day < 1 ||
                !DateOnly.TryParseExact(
                    $"{year:D4}-{month:D2}-{day:D2}",
                    "yyyy-MM-dd",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out var releaseDate))
            {
                throw new InvalidOperationException($"invalid date in release tag {tag}");
            }

            releases.Add(new ReleaseTag(releaseDate, counter, tag));
        }

        return releases;
    }

    private sealed record ReleaseTag(DateOnly Date, BigInteger Counter, string Tag);
}

internal static class CalendarVersionTests
{
    private static int failures;

    public static void Run()
    {
        Expect("first release with no tags", "2026-11-01", "", "2026.1101.1");
        Expect("legacy tags are ignored", "2026-11-01", "v0.1.0\nv0.4.0\nv0.5.0-beta.1", "2026.1101.1");
        Expect("second release on the same day", "2026-11-01", "v0.4.0\nv2026.1101.1", "2026.1101.2");
        Expect("counter uses numeric ordering",
            "2026-11-01",
            "v2026.1101.1\nv2026.1101.2\nv2026.1101.10\nv2026.1101.9",
            "2026.1101.11");
        Expect("counter resets on a new day", "2026-11-02", "v2026.1101.3", "2026.1102.1");
        Expect("year rollover with January date", "2027-01-05", "v2026.1231.2", "2027.105.1");
        Expect("January after December of the previous year",
            "2028-01-01",
            "v2027.105.1\nv2027.1231.1",
            "2028.101.1");
        Expect("January counter", "2027-01-05", "v2027.105.4", "2027.105.5");
        Expect("month-day uses numeric rather than lexical ordering",
            "2027-10-01",
            "v2027.105.3\nv2027.1001.1",
            "2027.1001.2");
        ExpectFailure("month-day ordering prevents backdating",
            "2027-09-30",
            "v2027.105.3\nv2027.1001.1",
            "earlier than the latest release");
        Expect("leap day", "2028-02-29", "", "2028.229.1");
        Expect("leap-day release tag is recognized", "2028-02-29", "v2028.229.1", "2028.229.2");
        Expect("git ls-remote output",
            "2026-11-01",
            "abc123\trefs/tags/v2026.1101.1\ndef456\trefs/tags/v2026.1101.1^{}\nfed789\trefs/tags/v0.4.0",
            "2026.1101.2");
        Expect("non-canonical and prerelease tags are ignored",
            "2026-11-01",
            "v2026.1101.1-beta.1\nv2026.01101.5\nv2026.1101.01\nv2026.99.1\nv0001.101.1",
            "2026.1101.1");
        Expect("nested refs do not masquerade as release tags",
            "2026-11-01",
            "abc123\trefs/tags/archive/refs/tags/v2026.1101.99",
            "2026.1101.1");
        Expect("bare refs are recognized", "2026-11-01", "refs/tags/v2026.1101.2", "2026.1101.3");
        ExpectFailure("invalid tag dates are rejected", "2026-11-30", "v2026.1131.1", "invalid date in release tag");
        ExpectFailure("non-leap-year tag dates are rejected", "2027-03-01", "v2027.229.1", "invalid date in release tag");
        ExpectFailure("non-leap century tag dates are rejected", "2100-03-01", "v2100.229.1", "invalid date in release tag");
        ExpectFailure("backdated release is rejected", "2026-11-01", "v2026.1102.1", "earlier than the latest release");
        ExpectFailure("backdated release across years is rejected",
            "2026-12-31",
            "v2027.105.1",
            "earlier than the latest release");
        ExpectFailure("counter overflow is rejected",
            "2026-11-01",
            "v2026.1101.65534",
            "exceeds the .NET version component limit");
        Expect("largest supported counter", "2026-11-01", "v2026.1101.65533", "2026.1101.65534");
        ExpectFailure("oversized counter cannot wrap around",
            "2026-11-01",
            "v2026.1101.65533\nv2026.1101.18446744073709551615",
            "exceeds the .NET version component limit");
        ExpectFailure("arbitrarily large counter is rejected",
            "2026-11-01",
            "v2026.1101.184467440737095516160000000000000000000",
            "exceeds the .NET version component limit");
        ExpectFailure("invalid date is rejected", "2027-02-29", "", "invalid date");
        ExpectFailure("malformed date is rejected", "2026-1-1", "", "invalid date");
        ExpectFailure("zero month is rejected explicitly", "2026-00-01", "", "invalid date");
        ExpectFailure("zero day is rejected explicitly", "2026-11-00", "", "invalid date");
        ExpectFailure("year must remain a four-digit component", "0001-01-01", "", "four-digit version component");
        ExpectOptionFailure("empty date fails explicitly", ["--date", ""], "--date requires a value");
        ExpectOptionFailure("empty tags file fails explicitly", ["--tags-file", ""], "--tags-file requires a value");
        ExpectOptionFailure("invalid CLI date is rejected", ["--date", "2027-02-29"], "invalid date");
        ExpectOptionFailure("CLI year must be a four-digit component",
            ["--date", "0001-01-01"],
            "four-digit version component");
        var missingTags = Path.Combine(Path.GetTempPath(), $"microstack-missing-tags-{Guid.NewGuid():N}");
        ExpectCliFailure("missing tag file fails cleanly", ["--tags-file", missingTags]);
        ExpectCliFailure("directory tag file fails cleanly", ["--tags-file", Path.GetTempPath()]);
        var missingRemote = Path.Combine(Path.GetTempPath(), $"microstack-missing-remote-{Guid.NewGuid():N}");
        ExpectFailure("remote lookup failure is reported",
            "2026-11-01",
            "",
            "failed to list tags from",
            () => CalendarVersion.ReadRemoteTags(missingRemote));

        if (failures != 0)
        {
            throw new InvalidOperationException($"{failures} calendar-version test(s) failed");
        }

        Console.WriteLine("All calendar-version tests passed.");
    }

    private static void Expect(string name, string date, string tags, string expected)
    {
        var actual = CalendarVersion.Next(ParseDate(date), tags);
        if (actual != expected)
        {
            Fail($"{name}: expected '{expected}', got '{actual}'");
            return;
        }

        Console.WriteLine($"ok   - {name}");
    }

    private static void ExpectFailure(
        string name,
        string date,
        string tags,
        string message,
        Func<string>? tagSource = null)
    {
        try
        {
            var input = tagSource is null ? tags : tagSource();
            CalendarVersion.Next(ParseDate(date), input);
            Fail($"{name}: expected failure");
        }
        catch (Exception exception) when (exception.Message.Contains(message, StringComparison.Ordinal))
        {
            Console.WriteLine($"ok   - {name}");
        }
    }

    internal static class ContainerSmokeTests
    {
        public static void Run()
        {
            ExpectHealthVersion(
                "matching health version passes",
                "/_microstack/health",
                """{"edition":"light","version":"2026.1101.7"}""",
                "2026.1101.7",
                shouldPass: true);
            ExpectHealthVersion(
                "nested or regex-like version cannot mask a mismatch",
                "/_microstack/health",
                """{"services":{"version":"2026.1101.7"},"version":"2026X1101X7"}""",
                "2026.1101.7",
                shouldPass: false);
            ExpectHealthVersion(
                "health alias version mismatch fails",
                "/_localstack/health",
                """{"version":"2026.1101.8"}""",
                "2026.1101.7",
                shouldPass: false);
            ExpectHealthVersion(
                "missing health version fails",
                "/health",
                """{"edition":"light"}""",
                "2026.1101.7",
                shouldPass: false);
            ExpectOptionFailure("smoke command requires an image", [], "usage:");
            ExpectOptionFailure("invalid smoke port is rejected", ["image", "--port", "0"], "--port requires");
            ExpectOptionFailure("invalid smoke port text is rejected", ["image", "--port", "nope"], "--port requires");
            var originalPort = Environment.GetEnvironmentVariable("MICROSTACK_SMOKE_API_PORT");
            try
            {
                Environment.SetEnvironmentVariable("MICROSTACK_SMOKE_API_PORT", "nope");
                ExpectOptionFailure("invalid smoke port environment is rejected",
                    ["image"],
                    "MICROSTACK_SMOKE_API_PORT");
            }
            finally
            {
                Environment.SetEnvironmentVariable("MICROSTACK_SMOKE_API_PORT", originalPort);
            }
            Console.WriteLine("All container-smoke tests passed.");
        }

        private static void ExpectHealthVersion(
            string name,
            string path,
            string json,
            string expectedVersion,
            bool shouldPass)
        {
            var failed = false;
            try
            {
                ContainerSmoke.ValidateHealthVersion(path, json, expectedVersion);
            }
            catch (InvalidOperationException)
            {
                failed = true;
            }

            if (failed == shouldPass)
            {
                throw new InvalidOperationException($"{name}: unexpected validation result");
            }

            Console.WriteLine($"ok   - {name}");
        }

        private static void ExpectOptionFailure(string name, string[] args, string message)
        {
            try
            {
                ContainerSmokeOptions.Parse(args);
                throw new InvalidOperationException($"{name}: expected failure");
            }
            catch (ArgumentException exception) when (exception.Message.Contains(message, StringComparison.Ordinal))
            {
                Console.WriteLine($"ok   - {name}");
            }
        }
    }

    private static void ExpectOptionFailure(string name, string[] args, string message)
    {
        try
        {
            CalendarVersionOptions.Parse(args);
            Fail($"{name}: expected failure");
        }
        catch (ArgumentException exception) when (exception.Message.Contains(message, StringComparison.Ordinal))
        {
            Console.WriteLine($"ok   - {name}");
        }
    }

    private static void ExpectCliFailure(string name, string[] args)
    {
        using var output = new StringWriter(CultureInfo.InvariantCulture);
        using var error = new StringWriter(CultureInfo.InvariantCulture);
        var exitCode = CalendarVersionCli.Run(args, output, error);
        if (exitCode != 1 || output.ToString().Length != 0 ||
            !error.ToString().StartsWith("calendar-version: ", StringComparison.Ordinal))
        {
            Fail($"{name}: unexpected exit code or output");
            return;
        }

        Console.WriteLine($"ok   - {name}");
    }

    private static DateOnly ParseDate(string date)
    {
        if (!DateOnly.TryParseExact(date, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None,
                out var parsed))
        {
            throw new ArgumentException("invalid date");
        }

        return parsed;
    }

    private static void Fail(string message)
    {
        failures++;
        Console.Error.WriteLine($"FAIL - {message}");
    }
}
