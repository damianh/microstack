using System.Text;
using System.Text.Json;
using System.Web;
using MicroStack.Internal;
using MicroStack.Services.Iam;

namespace MicroStack.Services.Sts;

/// <summary>
/// STS service handler — supports Query/XML protocol (Action= form params) and
/// JSON protocol (X-Amz-Target: AWSSecurityTokenServiceV20110615.ActionName).
///
/// Port of ministack/services/iam_sts.py (STS portion, lines 1345-1483).
///
/// Supports: GetCallerIdentity, AssumeRole, AssumeRoleWithWebIdentity,
///           GetSessionToken, GetAccessKeyInfo.
/// </summary>
internal sealed class StsServiceHandler : IServiceHandler
{
    private readonly IamServiceHandler _iam;
    private readonly Lock _lock = new();

    private const string StsXmlNs = "https://sts.amazonaws.com/doc/2011-06-15/";

    internal StsServiceHandler(IamServiceHandler iamHandler)
    {
        _iam = iamHandler;
    }

    // ── IServiceHandler ─────────────────────────────────────────────────────────

    public string ServiceName => "sts";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        var formParams = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (k, v) in request.QueryParams)
        {
            if (v.Length > 0)
            {
                formParams[k] = v[0];
            }
        }

        var contentType = request.GetHeader("content-type") ?? "";
        var target = request.GetHeader("x-amz-target") ?? "";

        var useJson = contentType.Contains("amz-json", StringComparison.Ordinal);

        // JSON protocol (newer SDKs): X-Amz-Target: AWSSecurityTokenServiceV20110615.ActionName
        if (useJson && target.StartsWith("AWSSecurityTokenServiceV20110615.", StringComparison.Ordinal))
        {
            var actionName = target[(target.LastIndexOf('.') + 1)..];
            formParams["Action"] = actionName;
            if (request.Body.Length > 0)
            {
                try
                {
                    var jsonDoc = JsonDocument.Parse(request.Body);
                    foreach (var prop in jsonDoc.RootElement.EnumerateObject())
                    {
                        formParams[prop.Name] = prop.Value.ValueKind == JsonValueKind.Array
                            ? prop.Value[0].GetString() ?? ""
                            : prop.Value.ToString();
                    }
                }
                catch (JsonException)
                {
                    // Ignore malformed JSON
                }
            }
        }
        else if (request.Method == "POST" && request.Body.Length > 0)
        {
            var formStr = Encoding.UTF8.GetString(request.Body);
            foreach (var pair in formStr.Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var eq = pair.IndexOf('=');
                if (eq < 0) continue;
                var key = HttpUtility.UrlDecode(pair[..eq]);
                var val = HttpUtility.UrlDecode(pair[(eq + 1)..]);
                formParams[key] = val;
            }
        }

        var action = P(formParams, "Action");
        var response = action switch
        {
            "GetCallerIdentity"          => ActGetCallerIdentity(useJson),
            "AssumeRole"                 => ActAssumeRole(formParams, useJson),
            "AssumeRoleWithWebIdentity"  => ActAssumeRoleWithWebIdentity(formParams, useJson),
            "GetSessionToken"            => ActGetSessionToken(formParams, useJson),
            "GetAccessKeyInfo"           => ActGetAccessKeyInfo(formParams, useJson),
            _                            => XmlError(400, "InvalidAction", $"Unknown STS action: {action}"),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        // STS is stateless — nothing to reset.
    }

    public JsonElement? GetState() => null;

    public void RestoreState(JsonElement state) { }

    // ── Actions ─────────────────────────────────────────────────────────────────

    private static ServiceResponse ActGetCallerIdentity(bool useJson)
    {
        var accountId = AccountContext.GetAccountId();
        if (useJson)
        {
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Account"] = accountId,
                ["Arn"] = $"arn:aws:iam::{accountId}:root",
                ["UserId"] = accountId,
            });
        }

        return XmlOk("GetCallerIdentityResponse",
            "<GetCallerIdentityResult>"
            + $"<Arn>arn:aws:iam::{accountId}:root</Arn>"
            + $"<UserId>{accountId}</UserId>"
            + $"<Account>{accountId}</Account>"
            + "</GetCallerIdentityResult>");
    }

    private static ServiceResponse ActAssumeRole(Dictionary<string, string> p, bool useJson)
    {
        var roleArn = P(p, "RoleArn");
        var sessionName = P(p, "RoleSessionName");
        var durationStr = P(p, "DurationSeconds");
        var duration = string.IsNullOrEmpty(durationStr) ? 3600 : int.Parse(durationStr);

        var accessKey = GenSessionAccessKey();
        var secretKey = GenSecret();
        var sessionToken = GenSessionToken();
        var roleId = "AROA" + Guid.NewGuid().ToString("N")[..17].ToUpperInvariant();

        var assumedArn = roleArn.Replace(":role/", ":assumed-role/", StringComparison.Ordinal);
        if (!assumedArn.EndsWith($"/{sessionName}", StringComparison.Ordinal))
        {
            assumedArn = $"{assumedArn}/{sessionName}";
        }

        if (useJson)
        {
            var expiration = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + duration;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Credentials"] = new Dictionary<string, object?>
                {
                    ["AccessKeyId"] = accessKey,
                    ["SecretAccessKey"] = secretKey,
                    ["SessionToken"] = sessionToken,
                    ["Expiration"] = expiration,
                },
                ["AssumedRoleUser"] = new Dictionary<string, object?>
                {
                    ["AssumedRoleId"] = $"{roleId}:{sessionName}",
                    ["Arn"] = assumedArn,
                },
                ["PackedPolicySize"] = (object?)0L,
            });
        }

        var xmlExpiration = Future(duration);
        return XmlOk("AssumeRoleResponse",
            "<AssumeRoleResult>"
            + "<Credentials>"
            + $"<AccessKeyId>{accessKey}</AccessKeyId>"
            + $"<SecretAccessKey>{secretKey}</SecretAccessKey>"
            + $"<SessionToken>{sessionToken}</SessionToken>"
            + $"<Expiration>{xmlExpiration}</Expiration>"
            + "</Credentials>"
            + "<AssumedRoleUser>"
            + $"<AssumedRoleId>{roleId}:{sessionName}</AssumedRoleId>"
            + $"<Arn>{assumedArn}</Arn>"
            + "</AssumedRoleUser>"
            + "<PackedPolicySize>0</PackedPolicySize>"
            + "</AssumeRoleResult>");
    }

    private static ServiceResponse ActAssumeRoleWithWebIdentity(Dictionary<string, string> p, bool useJson)
    {
        var roleArn = P(p, "RoleArn");
        var session = P(p, "RoleSessionName");
        if (string.IsNullOrEmpty(session))
        {
            session = "session";
        }

        var durationStr = P(p, "DurationSeconds");
        var duration = string.IsNullOrEmpty(durationStr) ? 3600 : int.Parse(durationStr);

        var accessKey = GenSessionAccessKey();
        var secretKey = GenSecret();
        var sessionToken = GenSessionToken();

        var assumedArn = roleArn.Replace(":role/", ":assumed-role/", StringComparison.Ordinal);
        if (!assumedArn.EndsWith($"/{session}", StringComparison.Ordinal))
        {
            assumedArn = $"{assumedArn}/{session}";
        }

        var roleId = "AROA" + Guid.NewGuid().ToString("N")[..17].ToUpperInvariant();

        if (useJson)
        {
            var expiration = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + duration;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Credentials"] = new Dictionary<string, object?>
                {
                    ["AccessKeyId"] = accessKey,
                    ["SecretAccessKey"] = secretKey,
                    ["SessionToken"] = sessionToken,
                    ["Expiration"] = expiration,
                },
                ["AssumedRoleUser"] = new Dictionary<string, object?>
                {
                    ["AssumedRoleId"] = $"{roleId}:{session}",
                    ["Arn"] = assumedArn,
                },
                ["SubjectFromWebIdentityToken"] = "test-subject",
                ["Audience"] = "sts.amazonaws.com",
                ["Provider"] = "accounts.google.com",
            });
        }

        var xmlExpiration = Future(duration);
        return XmlOk("AssumeRoleWithWebIdentityResponse",
            "<AssumeRoleWithWebIdentityResult>"
            + "<Credentials>"
            + $"<AccessKeyId>{accessKey}</AccessKeyId>"
            + $"<SecretAccessKey>{secretKey}</SecretAccessKey>"
            + $"<SessionToken>{sessionToken}</SessionToken>"
            + $"<Expiration>{xmlExpiration}</Expiration>"
            + "</Credentials>"
            + "<AssumedRoleUser>"
            + $"<AssumedRoleId>{roleId}:{session}</AssumedRoleId>"
            + $"<Arn>{assumedArn}</Arn>"
            + "</AssumedRoleUser>"
            + "<SubjectFromWebIdentityToken>test-subject</SubjectFromWebIdentityToken>"
            + "<Audience>sts.amazonaws.com</Audience>"
            + "<Provider>accounts.google.com</Provider>"
            + "</AssumeRoleWithWebIdentityResult>");
    }

    private static ServiceResponse ActGetSessionToken(Dictionary<string, string> p, bool useJson)
    {
        var durationStr = P(p, "DurationSeconds");
        var duration = string.IsNullOrEmpty(durationStr) ? 43200 : int.Parse(durationStr);

        var accessKey = GenSessionAccessKey();
        var secretKey = GenSecret();
        var sessionToken = GenSessionToken();

        if (useJson)
        {
            var expiration = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + duration;
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Credentials"] = new Dictionary<string, object?>
                {
                    ["AccessKeyId"] = accessKey,
                    ["SecretAccessKey"] = secretKey,
                    ["SessionToken"] = sessionToken,
                    ["Expiration"] = expiration,
                },
            });
        }

        var xmlExpiration = Future(duration);
        return XmlOk("GetSessionTokenResponse",
            "<GetSessionTokenResult>"
            + "<Credentials>"
            + $"<AccessKeyId>{accessKey}</AccessKeyId>"
            + $"<SecretAccessKey>{secretKey}</SecretAccessKey>"
            + $"<SessionToken>{sessionToken}</SessionToken>"
            + $"<Expiration>{xmlExpiration}</Expiration>"
            + "</Credentials>"
            + "</GetSessionTokenResult>");
    }

    private static ServiceResponse ActGetAccessKeyInfo(Dictionary<string, string> p, bool useJson)
    {
        var accountId = AccountContext.GetAccountId();
        if (useJson)
        {
            return AwsResponseHelpers.JsonResponse(new Dictionary<string, object?>
            {
                ["Account"] = accountId,
            });
        }

        return XmlOk("GetAccessKeyInfoResponse",
            "<GetAccessKeyInfoResult>"
            + $"<Account>{accountId}</Account>"
            + "</GetAccessKeyInfoResult>");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    private static string P(Dictionary<string, string> p, string key) =>
        p.GetValueOrDefault(key, "");

    private static string GenSessionAccessKey() =>
        "ASIA" + Guid.NewGuid().ToString("N")[..16].ToUpperInvariant();

    private static string GenSecret()
    {
        var raw = Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N");
        return raw[..40];
    }

    private static string GenSessionToken()
    {
        var parts = string.Concat(
            Guid.NewGuid().ToString("N"),
            Guid.NewGuid().ToString("N"),
            Guid.NewGuid().ToString("N"),
            Guid.NewGuid().ToString("N"));
        return "FwoGZX" + parts;
    }

    private static string Future(int seconds) =>
        DateTime.UtcNow.AddSeconds(seconds).ToString("yyyy-MM-ddTHH:mm:ssZ");

    // ── XML response helpers ────────────────────────────────────────────────────

    private static ServiceResponse XmlOk(string rootTag, string inner)
    {
        var body =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + $"<{rootTag} xmlns=\"{StsXmlNs}\">"
            + inner
            + $"<ResponseMetadata><RequestId>{HashHelpers.NewUuid()}</RequestId></ResponseMetadata>"
            + $"</{rootTag}>";

        return new ServiceResponse(200, XmlContentType, Encoding.UTF8.GetBytes(body));
    }

    private static ServiceResponse XmlError(int status, string code, string message)
    {
        var errorType = status < 500 ? "Sender" : "Receiver";
        var body =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + $"<ErrorResponse xmlns=\"{StsXmlNs}\">"
            + $"<Error><Type>{errorType}</Type><Code>{code}</Code><Message>{XmlEsc(message)}</Message></Error>"
            + $"<RequestId>{HashHelpers.NewUuid()}</RequestId>"
            + "</ErrorResponse>";

        return new ServiceResponse(status, XmlContentType, Encoding.UTF8.GetBytes(body));
    }

    private static readonly Dictionary<string, string> XmlContentType = new(StringComparer.Ordinal)
    {
        ["Content-Type"] = "application/xml",
    };

    private static string XmlEsc(string? s) =>
        s is null ? "" : s
            .Replace("&", "&amp;")
            .Replace("<", "&lt;")
            .Replace(">", "&gt;")
            .Replace("\"", "&quot;")
            .Replace("'", "&apos;");
}
