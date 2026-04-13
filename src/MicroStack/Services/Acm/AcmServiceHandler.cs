using System.Text.Json;
using MicroStack.Internal;

namespace MicroStack.Services.Acm;

/// <summary>
/// ACM (Certificate Manager) service handler -- supports JSON protocol via X-Amz-Target.
///
/// Port of ministack/services/acm.py.
///
/// Supports: RequestCertificate, DescribeCertificate, ListCertificates,
///           DeleteCertificate, GetCertificate, ImportCertificate,
///           AddTagsToCertificate, RemoveTagsFromCertificate, ListTagsForCertificate,
///           UpdateCertificateOptions, RenewCertificate, ResendValidationEmail.
/// </summary>
internal sealed class AcmServiceHandler : IServiceHandler
{
    private readonly AccountScopedDictionary<string, AcmCertificate> _certificates = new(); // keyed by ARN
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "acm";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var target = request.GetHeader("x-amz-target") ?? "";
        var action = target.Contains('.', StringComparison.Ordinal)
            ? target[(target.LastIndexOf('.') + 1)..]
            : "";

        JsonElement data;
        if (request.Body.Length > 0)
        {
            try
            {
                using var doc = JsonDocument.Parse(request.Body);
                data = doc.RootElement.Clone();
            }
            catch (JsonException)
            {
                return Task.FromResult(
                    AwsResponseHelpers.ErrorResponseJson("SerializationException", "Invalid JSON", 400));
            }
        }
        else
        {
            data = JsonDocument.Parse("{}").RootElement.Clone();
        }

        var response = action switch
        {
            "RequestCertificate" => ActRequestCertificate(data),
            "DescribeCertificate" => ActDescribeCertificate(data),
            "ListCertificates" => ActListCertificates(data),
            "DeleteCertificate" => ActDeleteCertificate(data),
            "GetCertificate" => ActGetCertificate(data),
            "ImportCertificate" => ActImportCertificate(data),
            "AddTagsToCertificate" => ActAddTags(data),
            "RemoveTagsFromCertificate" => ActRemoveTags(data),
            "ListTagsForCertificate" => ActListTags(data),
            "UpdateCertificateOptions" => ActUpdateOptions(data),
            "RenewCertificate" => ActRenewCertificate(data),
            "ResendValidationEmail" => ActResendValidationEmail(data),
            _ => AwsResponseHelpers.ErrorResponseJson("InvalidAction", $"Unknown action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _certificates.Clear();
        }
    }

    public JsonElement? GetState() => null;

    public void RestoreState(JsonElement state) { }

    // -- Helpers ---------------------------------------------------------------

    private static string? GetString(JsonElement el, string propertyName)
    {
        return el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
    }

    private static string CertArn()
    {
        return $"arn:aws:acm:{Region}:{AccountContext.GetAccountId()}:certificate/{HashHelpers.NewUuid()}";
    }

    private static string FutureIso(int seconds)
    {
        return DateTime.UtcNow.AddSeconds(seconds).ToString("yyyy-MM-ddTHH:mm:ssZ");
    }

    private static Dictionary<string, object?> ValidationOptions(string domain, string method)
    {
        return new Dictionary<string, object?>
        {
            ["DomainName"] = domain,
            ["ValidationMethod"] = method,
            ["ValidationStatus"] = "SUCCESS",
            ["ResourceRecord"] = new Dictionary<string, object?>
            {
                ["Name"] = $"_acme-challenge.{domain}.",
                ["Type"] = "CNAME",
                ["Value"] = $"fake-validation-{HashHelpers.NewUuid()[..8]}.acm.amazonaws.com.",
            },
        };
    }

    private static Dictionary<string, object?> CertShape(AcmCertificate cert)
    {
        return new Dictionary<string, object?>
        {
            ["CertificateArn"] = cert.Arn,
            ["DomainName"] = cert.DomainName,
            ["SubjectAlternativeNames"] = cert.SubjectAlternativeNames,
            ["Status"] = cert.Status,
            ["Type"] = cert.Type,
            ["KeyAlgorithm"] = "RSA_2048",
            ["SignatureAlgorithm"] = "SHA256WITHRSA",
            ["InUseBy"] = cert.InUseBy,
            ["CreatedAt"] = cert.CreatedAt,
            ["IssuedAt"] = cert.IssuedAt,
            ["NotBefore"] = cert.NotBefore,
            ["NotAfter"] = cert.NotAfter,
            ["DomainValidationOptions"] = cert.DomainValidationOptions,
            ["Options"] = cert.Options,
            ["Tags"] = cert.Tags,
        };
    }

    private static List<Dictionary<string, string>> ParseTags(JsonElement data)
    {
        var tags = new List<Dictionary<string, string>>();
        if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var tagEl in tagsEl.EnumerateArray())
            {
                var key = GetString(tagEl, "Key");
                var value = GetString(tagEl, "Value") ?? "";
                if (key is not null)
                {
                    tags.Add(new Dictionary<string, string> { ["Key"] = key, ["Value"] = value });
                }
            }
        }

        return tags;
    }

    // -- Actions ---------------------------------------------------------------

    private ServiceResponse ActRequestCertificate(JsonElement data)
    {
        var domain = GetString(data, "DomainName") ?? "";
        if (string.IsNullOrEmpty(domain))
        {
            return AwsResponseHelpers.ErrorResponseJson(
                "InvalidParameterException", "DomainName is required", 400);
        }

        var method = GetString(data, "ValidationMethod") ?? "DNS";

        var sans = new List<string>();
        if (data.TryGetProperty("SubjectAlternativeNames", out var sansEl) && sansEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in sansEl.EnumerateArray())
            {
                var s = item.GetString();
                if (s is not null)
                {
                    sans.Add(s);
                }
            }
        }

        if (sans.Count == 0)
        {
            sans.Add(domain);
        }

        if (!sans.Contains(domain))
        {
            sans.Insert(0, domain);
        }

        var arn = CertArn();
        var now = TimeHelpers.NowIso();

        var validationOptions = sans.ConvertAll(d => ValidationOptions(d, method));

        lock (_lock)
        {
            _certificates[arn] = new AcmCertificate
            {
                Arn = arn,
                DomainName = domain,
                SubjectAlternativeNames = sans,
                Status = "ISSUED",
                Type = "AMAZON_ISSUED",
                CreatedAt = now,
                IssuedAt = now,
                NotBefore = now,
                NotAfter = FutureIso(365 * 24 * 3600),
                DomainValidationOptions = validationOptions,
                ValidationMethod = method,
                Tags = ParseTags(data),
                Options = new Dictionary<string, object?>(),
                InUseBy = [],
            };
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["CertificateArn"] = arn,
        });
    }

    private ServiceResponse ActDescribeCertificate(JsonElement data)
    {
        var arn = GetString(data, "CertificateArn") ?? "";

        lock (_lock)
        {
            if (!_certificates.TryGetValue(arn, out var cert))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Certificate {arn} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Certificate"] = CertShape(cert),
            });
        }
    }

    private ServiceResponse ActListCertificates(JsonElement data)
    {
        var statuses = new List<string>();
        if (data.TryGetProperty("CertificateStatuses", out var statusesEl) && statusesEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in statusesEl.EnumerateArray())
            {
                var s = item.GetString();
                if (s is not null)
                {
                    statuses.Add(s);
                }
            }
        }

        lock (_lock)
        {
            var summaries = new List<Dictionary<string, object?>>();
            foreach (var (arn, cert) in _certificates.Items)
            {
                if (statuses.Count > 0 && !statuses.Contains(cert.Status))
                {
                    continue;
                }

                summaries.Add(new Dictionary<string, object?>
                {
                    ["CertificateArn"] = arn,
                    ["DomainName"] = cert.DomainName,
                    ["Status"] = cert.Status,
                });
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["CertificateSummaryList"] = summaries,
                ["NextToken"] = (string?)null,
            });
        }
    }

    private ServiceResponse ActDeleteCertificate(JsonElement data)
    {
        var arn = GetString(data, "CertificateArn") ?? "";

        lock (_lock)
        {
            if (!_certificates.TryRemove(arn, out _))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Certificate {arn} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActGetCertificate(JsonElement data)
    {
        var arn = GetString(data, "CertificateArn") ?? "";

        lock (_lock)
        {
            if (!_certificates.ContainsKey(arn))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Certificate {arn} not found", 400);
            }

            const string fakePem = "-----BEGIN CERTIFICATE-----\nMIIFakeCertificateDataHere\n-----END CERTIFICATE-----";
            const string fakeChain = "-----BEGIN CERTIFICATE-----\nMIIFakeChainDataHere\n-----END CERTIFICATE-----";

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Certificate"] = fakePem,
                ["CertificateChain"] = fakeChain,
            });
        }
    }

    private ServiceResponse ActImportCertificate(JsonElement data)
    {
        var arn = GetString(data, "CertificateArn") ?? CertArn();
        var now = TimeHelpers.NowIso();

        lock (_lock)
        {
            _certificates[arn] = new AcmCertificate
            {
                Arn = arn,
                DomainName = "imported.example.com",
                SubjectAlternativeNames = ["imported.example.com"],
                Status = "ISSUED",
                Type = "IMPORTED",
                CreatedAt = now,
                IssuedAt = now,
                NotBefore = now,
                NotAfter = FutureIso(365 * 24 * 3600),
                DomainValidationOptions = [],
                Tags = ParseTags(data),
                Options = new Dictionary<string, object?>(),
                InUseBy = [],
            };
        }

        return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
        {
            ["CertificateArn"] = arn,
        });
    }

    private ServiceResponse ActAddTags(JsonElement data)
    {
        var arn = GetString(data, "CertificateArn") ?? "";

        lock (_lock)
        {
            if (!_certificates.TryGetValue(arn, out var cert))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Certificate {arn} not found", 400);
            }

            var existing = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
            foreach (var tag in cert.Tags)
            {
                existing[tag["Key"]] = tag;
            }

            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var tagEl in tagsEl.EnumerateArray())
                {
                    var key = GetString(tagEl, "Key");
                    var value = GetString(tagEl, "Value") ?? "";
                    if (key is not null)
                    {
                        existing[key] = new Dictionary<string, string> { ["Key"] = key, ["Value"] = value };
                    }
                }
            }

            cert.Tags = [.. existing.Values];
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActRemoveTags(JsonElement data)
    {
        var arn = GetString(data, "CertificateArn") ?? "";

        lock (_lock)
        {
            if (!_certificates.TryGetValue(arn, out var cert))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Certificate {arn} not found", 400);
            }

            var removeKeys = new HashSet<string>(StringComparer.Ordinal);
            if (data.TryGetProperty("Tags", out var tagsEl) && tagsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var tagEl in tagsEl.EnumerateArray())
                {
                    var key = GetString(tagEl, "Key");
                    if (key is not null)
                    {
                        removeKeys.Add(key);
                    }
                }
            }

            cert.Tags = cert.Tags.FindAll(t => !removeKeys.Contains(t["Key"]));
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActListTags(JsonElement data)
    {
        var arn = GetString(data, "CertificateArn") ?? "";

        lock (_lock)
        {
            if (!_certificates.TryGetValue(arn, out var cert))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Certificate {arn} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Tags"] = cert.Tags,
            });
        }
    }

    private ServiceResponse ActUpdateOptions(JsonElement data)
    {
        var arn = GetString(data, "CertificateArn") ?? "";

        lock (_lock)
        {
            if (!_certificates.TryGetValue(arn, out var cert))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Certificate {arn} not found", 400);
            }

            // Just accept the options; we don't need to parse deeply for emulation
            cert.Options = new Dictionary<string, object?>();
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActRenewCertificate(JsonElement data)
    {
        var arn = GetString(data, "CertificateArn") ?? "";

        lock (_lock)
        {
            if (!_certificates.ContainsKey(arn))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Certificate {arn} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    private ServiceResponse ActResendValidationEmail(JsonElement data)
    {
        var arn = GetString(data, "CertificateArn") ?? "";

        lock (_lock)
        {
            if (!_certificates.ContainsKey(arn))
            {
                return AwsResponseHelpers.ErrorResponseJson(
                    "ResourceNotFoundException", $"Certificate {arn} not found", 400);
            }

            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>());
        }
    }

    // -- Internal model --------------------------------------------------------

    private sealed class AcmCertificate
    {
        internal string Arn { get; init; } = "";
        internal string DomainName { get; init; } = "";
        internal List<string> SubjectAlternativeNames { get; init; } = [];
        internal string Status { get; set; } = "ISSUED";
        internal string Type { get; init; } = "AMAZON_ISSUED";
        internal string CreatedAt { get; init; } = "";
        internal string IssuedAt { get; init; } = "";
        internal string NotBefore { get; init; } = "";
        internal string NotAfter { get; init; } = "";
        internal List<Dictionary<string, object?>> DomainValidationOptions { get; init; } = [];
        internal string ValidationMethod { get; init; } = "DNS";
        internal List<Dictionary<string, string>> Tags { get; set; } = [];
        internal Dictionary<string, object?> Options { get; set; } = new();
        internal List<string> InUseBy { get; init; } = [];
    }
}
