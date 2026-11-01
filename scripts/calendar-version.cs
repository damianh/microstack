using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text.RegularExpressions;

if (args is ["--test"])
{
    CalendarVersionTests.Run();
    return;
}

try
{
    var options = CalendarVersionOptions.Parse(args);
    var releaseDate = options.Date ?? DateOnly.FromDateTime(DateTime.UtcNow);
    var tags = options.TagsFile is null
        ? CalendarVersion.ReadRemoteTags()
        : File.ReadAllText(options.TagsFile);
    Console.WriteLine(CalendarVersion.Next(releaseDate, tags));
}
catch (Exception exception) when (exception is ArgumentException or IOException or InvalidOperationException)
{
    Console.Error.WriteLine($"calendar-version: {exception.Message}");
    Environment.ExitCode = 1;
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
                            DateTimeStyles.None, out var parsedDate) ||
                        parsedDate.Year < 1000)
                    {
                        throw new ArgumentException(
                            $"invalid date '{args[index]}'; expected a valid date in YYYY-MM-DD format");
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

    [GeneratedRegex(@"^v(?<year>[0-9]{4})\.(?<monthDay>[1-9][0-9]{1,3})\.(?<counter>[1-9][0-9]*)$",
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

    public static string ReadRemoteTags()
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo("git", ["ls-remote", "--tags", "--refs", "origin"])
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
                throw new InvalidOperationException("failed to start git while listing tags from origin");
            }
        }
        catch (Exception exception) when (exception is InvalidOperationException or System.ComponentModel.Win32Exception)
        {
            throw new InvalidOperationException("failed to list tags from origin", exception);
        }

        var outputTask = process.StandardOutput.ReadToEndAsync();
        var errorTask = process.StandardError.ReadToEndAsync();
        process.WaitForExit();
        var output = outputTask.GetAwaiter().GetResult();
        var error = errorTask.GetAwaiter().GetResult();
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"failed to list tags from origin{(string.IsNullOrWhiteSpace(error) ? "" : $": {error.Trim()}")}");
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
            "v2026.1101.1-beta.1\nv2026.01101.5\nv2026.1101.01",
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
        ExpectFailure("remote lookup failure is reported",
            "2026-11-01",
            "",
            "failed to list tags from origin",
            () => throw new InvalidOperationException("failed to list tags from origin"));

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
