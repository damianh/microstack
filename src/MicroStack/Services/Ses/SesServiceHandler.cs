using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Web;
using MicroStack.Internal;

namespace MicroStack.Services.Ses;

/// <summary>
/// SES service handler — supports v1 Query/XML protocol and v2 REST/JSON protocol.
///
/// Port of ministack/services/ses.py (v1 + v2 inline) and ses_v2.py.
///
/// v1 Query API (Action=...) via POST form body.
/// v2 JSON API detected via path prefix /v2/email/.
///
/// v1 actions: SendEmail, SendRawEmail, SendTemplatedEmail, SendBulkTemplatedEmail,
///             VerifyEmailIdentity, VerifyEmailAddress, VerifyDomainIdentity,
///             VerifyDomainDkim, ListIdentities, GetIdentityVerificationAttributes,
///             DeleteIdentity, GetSendQuota, GetSendStatistics,
///             ListVerifiedEmailAddresses, CreateConfigurationSet,
///             DeleteConfigurationSet, DescribeConfigurationSet, ListConfigurationSets,
///             CreateTemplate, GetTemplate, DeleteTemplate, ListTemplates, UpdateTemplate,
///             GetIdentityDkimAttributes, SetIdentityNotificationTopic,
///             SetIdentityFeedbackForwardingEnabled.
///
/// v2 REST endpoints under /v2/email/:
///             outbound-emails, identities, configuration-sets, templates, account,
///             tags, suppression.
/// </summary>
internal sealed partial class SesServiceHandler : IServiceHandler
{
    private readonly Lock _lock = new();

    private static readonly string Region =
        Environment.GetEnvironmentVariable("MINISTACK_REGION") ?? "us-east-1";

    private const string SesXmlNs = "http://ses.amazonaws.com/doc/2010-12-01/";

    // ── v1 state ──────────────────────────────────────────────────────────────
    private readonly AccountScopedDictionary<string, SesIdentity> _identities = new();
    private readonly List<SesEmail> _sentEmails = [];
    private readonly AccountScopedDictionary<string, SesTemplate> _templates = new();
    private readonly AccountScopedDictionary<string, SesConfigurationSet> _configurationSets = new();

    // ── v2-only state ─────────────────────────────────────────────────────────
    private readonly AccountScopedDictionary<string, List<Dictionary<string, string>>> _sesTags = new();

    // -- IServiceHandler -------------------------------------------------------

    public string ServiceName => "ses";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        // v2 REST/JSON API detected via path prefix /v2/email/
        if (request.Path.StartsWith("/v2/email", StringComparison.OrdinalIgnoreCase))
        {
            return Task.FromResult(HandleV2(request));
        }

        // v1 Query/XML protocol
        var formParams = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (k, v) in request.QueryParams)
        {
            if (v.Length > 0) formParams[k] = v[0];
        }

        if (request.Method == "POST" && request.Body.Length > 0)
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
            "SendEmail"                           => ActSendEmail(formParams),
            "SendRawEmail"                        => ActSendRawEmail(formParams),
            "SendTemplatedEmail"                  => ActSendTemplatedEmail(formParams),
            "SendBulkTemplatedEmail"              => ActSendBulkTemplatedEmail(formParams),
            "VerifyEmailIdentity"                 => ActVerifyEmailIdentity(formParams),
            "VerifyEmailAddress"                  => ActVerifyEmailIdentity(formParams),
            "VerifyDomainIdentity"                => ActVerifyDomainIdentity(formParams),
            "VerifyDomainDkim"                    => ActVerifyDomainDkim(formParams),
            "ListIdentities"                      => ActListIdentities(formParams),
            "GetIdentityVerificationAttributes"   => ActGetIdentityVerificationAttributes(formParams),
            "DeleteIdentity"                      => ActDeleteIdentity(formParams),
            "GetSendQuota"                        => ActGetSendQuota(),
            "GetSendStatistics"                   => ActGetSendStatistics(),
            "ListVerifiedEmailAddresses"          => ActListVerifiedEmails(),
            "CreateConfigurationSet"              => ActCreateConfigurationSet(formParams),
            "DeleteConfigurationSet"              => ActDeleteConfigurationSet(formParams),
            "DescribeConfigurationSet"            => ActDescribeConfigurationSet(formParams),
            "ListConfigurationSets"               => ActListConfigurationSets(),
            "CreateTemplate"                      => ActCreateTemplate(formParams),
            "GetTemplate"                         => ActGetTemplate(formParams),
            "DeleteTemplate"                      => ActDeleteTemplate(formParams),
            "ListTemplates"                       => ActListTemplates(),
            "UpdateTemplate"                      => ActUpdateTemplate(formParams),
            "GetIdentityDkimAttributes"           => ActGetIdentityDkimAttributes(formParams),
            "SetIdentityNotificationTopic"        => ActSetIdentityNotificationTopic(formParams),
            "SetIdentityFeedbackForwardingEnabled" => ActSetIdentityFeedbackForwarding(formParams),
            _ => XmlError("InvalidAction", $"Unknown action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _identities.Clear();
            _sentEmails.Clear();
            _templates.Clear();
            _configurationSets.Clear();
            _sesTags.Clear();
        }
    }

    public object? GetState() => null;

    public void RestoreState(object state) { }

    // ═══════════════════════════════════════════════════════════════════════════
    // v1 — Send operations
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse ActSendEmail(Dictionary<string, string> p)
    {
        var source = P(p, "Source");
        var subject = P(p, "Message.Subject.Data");
        var bodyText = P(p, "Message.Body.Text.Data");
        var bodyHtml = P(p, "Message.Body.Html.Data");
        var configSet = P(p, "ConfigurationSetName");
        var toAddrs = CollectList(p, "Destination.ToAddresses.member");
        var ccAddrs = CollectList(p, "Destination.CcAddresses.member");
        var bccAddrs = CollectList(p, "Destination.BccAddresses.member");

        var msgId = $"{HashHelpers.NewUuid()}@email.amazonses.com";

        lock (_lock)
        {
            _sentEmails.Add(new SesEmail
            {
                MessageId = msgId,
                Source = source,
                To = toAddrs,
                Subject = subject,
                BodyText = bodyText,
                BodyHtml = bodyHtml,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                EmailType = "SendEmail",
                ConfigurationSetName = configSet,
            });
        }

        return XmlResponse("SendEmailResponse",
            $"<SendEmailResult><MessageId>{msgId}</MessageId></SendEmailResult>");
    }

    private ServiceResponse ActSendRawEmail(Dictionary<string, string> p)
    {
        var msgId = $"{HashHelpers.NewUuid()}@email.amazonses.com";

        lock (_lock)
        {
            _sentEmails.Add(new SesEmail
            {
                MessageId = msgId,
                Source = P(p, "Source"),
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                EmailType = "SendRawEmail",
            });
        }

        return XmlResponse("SendRawEmailResponse",
            $"<SendRawEmailResult><MessageId>{msgId}</MessageId></SendRawEmailResult>");
    }

    private ServiceResponse ActSendTemplatedEmail(Dictionary<string, string> p)
    {
        var source = P(p, "Source");
        var templateName = P(p, "Template");
        var templateData = P(p, "TemplateData");
        var configSet = P(p, "ConfigurationSetName");
        var toAddrs = CollectList(p, "Destination.ToAddresses.member");

        if (!_templates.ContainsKey(templateName))
        {
            return XmlError("TemplateDoesNotExist",
                $"Template {templateName} does not exist", 400);
        }

        var msgId = $"{HashHelpers.NewUuid()}@email.amazonses.com";

        lock (_lock)
        {
            _sentEmails.Add(new SesEmail
            {
                MessageId = msgId,
                Source = source,
                To = toAddrs,
                Template = templateName,
                TemplateData = templateData,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                EmailType = "SendTemplatedEmail",
                ConfigurationSetName = configSet,
            });
        }

        return XmlResponse("SendTemplatedEmailResponse",
            $"<SendTemplatedEmailResult><MessageId>{msgId}</MessageId></SendTemplatedEmailResult>");
    }

    private ServiceResponse ActSendBulkTemplatedEmail(Dictionary<string, string> p)
    {
        var source = P(p, "Source");
        var templateName = P(p, "Template");
        var defaultTemplateData = P(p, "DefaultTemplateData");

        if (!_templates.ContainsKey(templateName))
        {
            return XmlError("TemplateDoesNotExist",
                $"Template {templateName} does not exist", 400);
        }

        var statuses = new StringBuilder();
        var i = 1;
        while (P(p, $"Destinations.member.{i}.Destination.ToAddresses.member.1").Length > 0)
        {
            var toAddrs = CollectList(p, $"Destinations.member.{i}.Destination.ToAddresses.member");
            var replacement = P(p, $"Destinations.member.{i}.ReplacementTemplateData");
            if (string.IsNullOrEmpty(replacement)) replacement = defaultTemplateData;

            var msgId = $"{HashHelpers.NewUuid()}@email.amazonses.com";
            lock (_lock)
            {
                _sentEmails.Add(new SesEmail
                {
                    MessageId = msgId,
                    Source = source,
                    To = toAddrs,
                    Template = templateName,
                    TemplateData = replacement,
                    Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    EmailType = "SendBulkTemplatedEmail",
                });
            }
            statuses.Append($"<member><Status>Success</Status><MessageId>{msgId}</MessageId></member>");
            i++;
        }

        return XmlResponse("SendBulkTemplatedEmailResponse",
            $"<SendBulkTemplatedEmailResult><Status>{statuses}</Status></SendBulkTemplatedEmailResult>");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v1 — Identity operations
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse ActVerifyEmailIdentity(Dictionary<string, string> p)
    {
        var email = P(p, "EmailAddress");
        lock (_lock)
        {
            _identities[email] = MakeIdentity(email, "EmailAddress");
        }
        return XmlResponse("VerifyEmailIdentityResponse",
            "<VerifyEmailIdentityResult/>");
    }

    private ServiceResponse ActVerifyDomainIdentity(Dictionary<string, string> p)
    {
        var domain = P(p, "Domain");
        lock (_lock)
        {
            _identities[domain] = MakeIdentity(domain, "Domain");
        }
        var token = Convert.ToHexStringLower(MD5.HashData(Encoding.UTF8.GetBytes(domain)))[..32];
        return XmlResponse("VerifyDomainIdentityResponse",
            $"<VerifyDomainIdentityResult><VerificationToken>{token}</VerificationToken></VerifyDomainIdentityResult>");
    }

    private ServiceResponse ActVerifyDomainDkim(Dictionary<string, string> p)
    {
        var domain = P(p, "Domain");
        lock (_lock)
        {
            if (!_identities.ContainsKey(domain))
            {
                _identities[domain] = MakeIdentity(domain, "Domain");
            }

            var tokens = new List<string>();
            for (var i = 0; i < 3; i++)
            {
                tokens.Add(Convert.ToHexStringLower(
                    MD5.HashData(Encoding.UTF8.GetBytes($"{domain}-dkim-{i}")))[..32]);
            }

            _identities[domain].DkimEnabled = true;
            _identities[domain].DkimTokens = tokens;
            _identities[domain].DkimVerificationStatus = "Success";

            var members = string.Concat(tokens.Select(t => $"<member>{t}</member>"));
            return XmlResponse("VerifyDomainDkimResponse",
                $"<VerifyDomainDkimResult><DkimTokens>{members}</DkimTokens></VerifyDomainDkimResult>");
        }
    }

    private ServiceResponse ActListIdentities(Dictionary<string, string> p)
    {
        var identityType = P(p, "IdentityType");
        var sb = new StringBuilder();
        foreach (var (identity, info) in _identities)
        {
            if (identityType.Length == 0 || info.IdentityType == identityType)
            {
                sb.Append($"<member>{Esc(identity)}</member>");
            }
        }
        return XmlResponse("ListIdentitiesResponse",
            $"<ListIdentitiesResult><Identities>{sb}</Identities></ListIdentitiesResult>");
    }

    private ServiceResponse ActGetIdentityVerificationAttributes(Dictionary<string, string> p)
    {
        var identities = CollectList(p, "Identities.member");
        var sb = new StringBuilder();
        foreach (var identity in identities)
        {
            var status = _identities.TryGetValue(identity, out var info)
                ? info.VerificationStatus
                : "Pending";
            sb.Append($"<entry><key>{Esc(identity)}</key><value><VerificationStatus>{status}</VerificationStatus></value></entry>");
        }
        return XmlResponse("GetIdentityVerificationAttributesResponse",
            $"<GetIdentityVerificationAttributesResult><VerificationAttributes>{sb}</VerificationAttributes></GetIdentityVerificationAttributesResult>");
    }

    private ServiceResponse ActDeleteIdentity(Dictionary<string, string> p)
    {
        var identity = P(p, "Identity");
        lock (_lock)
        {
            _identities.TryRemove(identity, out _);
        }
        return XmlResponse("DeleteIdentityResponse", "");
    }

    private ServiceResponse ActListVerifiedEmails()
    {
        var sb = new StringBuilder();
        foreach (var (email, info) in _identities)
        {
            if (info.VerificationStatus == "Success" && info.IdentityType == "EmailAddress")
            {
                sb.Append($"<member>{Esc(email)}</member>");
            }
        }
        return XmlResponse("ListVerifiedEmailAddressesResponse",
            $"<ListVerifiedEmailAddressesResult><VerifiedEmailAddresses>{sb}</VerifiedEmailAddresses></ListVerifiedEmailAddressesResult>");
    }

    private ServiceResponse ActGetIdentityDkimAttributes(Dictionary<string, string> p)
    {
        var identities = CollectList(p, "Identities.member");
        var sb = new StringBuilder();
        foreach (var identity in identities)
        {
            _identities.TryGetValue(identity, out var info);
            var enabled = info?.DkimEnabled == true ? "true" : "false";
            var status = info?.DkimVerificationStatus ?? "NotStarted";
            var tokensXml = string.Concat(
                (info?.DkimTokens ?? []).Select(t => $"<member>{t}</member>"));
            sb.Append($"<entry><key>{Esc(identity)}</key><value>"
                    + $"<DkimEnabled>{enabled}</DkimEnabled>"
                    + $"<DkimVerificationStatus>{status}</DkimVerificationStatus>"
                    + $"<DkimTokens>{tokensXml}</DkimTokens>"
                    + $"</value></entry>");
        }
        return XmlResponse("GetIdentityDkimAttributesResponse",
            $"<GetIdentityDkimAttributesResult><DkimAttributes>{sb}</DkimAttributes></GetIdentityDkimAttributesResult>");
    }

    private ServiceResponse ActSetIdentityNotificationTopic(Dictionary<string, string> p)
    {
        var identity = P(p, "Identity");
        var notificationType = P(p, "NotificationType");
        var snsTopic = P(p, "SnsTopic");
        lock (_lock)
        {
            if (_identities.TryGetValue(identity, out var info))
            {
                info.NotificationTopics[notificationType] = snsTopic;
            }
        }
        return XmlResponse("SetIdentityNotificationTopicResponse", "");
    }

    private ServiceResponse ActSetIdentityFeedbackForwarding(Dictionary<string, string> p)
    {
        var identity = P(p, "Identity");
        var enabled = string.Equals(P(p, "ForwardingEnabled"), "true", StringComparison.OrdinalIgnoreCase);
        lock (_lock)
        {
            if (_identities.TryGetValue(identity, out var info))
            {
                info.FeedbackForwardingEnabled = enabled;
            }
        }
        return XmlResponse("SetIdentityFeedbackForwardingEnabledResponse", "");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v1 — Quota / Statistics
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse ActGetSendQuota()
    {
        double sent24H;
        lock (_lock)
        {
            var cutoff = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 86400;
            sent24H = _sentEmails.Count(e => e.Timestamp >= cutoff);
        }

        return XmlResponse("GetSendQuotaResponse",
            $"<GetSendQuotaResult>"
            + $"<Max24HourSend>50000.0</Max24HourSend>"
            + $"<MaxSendRate>14.0</MaxSendRate>"
            + $"<SentLast24Hours>{sent24H:F1}</SentLast24Hours>"
            + $"</GetSendQuotaResult>");
    }

    private ServiceResponse ActGetSendStatistics()
    {
        var sb = new StringBuilder();
        List<SesEmail> snapshot;
        lock (_lock)
        {
            snapshot = [.. _sentEmails];
        }

        var buckets = new Dictionary<long, (long Ts, int Attempts)>();
        foreach (var email in snapshot)
        {
            var bucketTs = email.Timestamp - (email.Timestamp % 900);
            if (!buckets.ContainsKey(bucketTs))
                buckets[bucketTs] = (bucketTs, 0);
            var b = buckets[bucketTs];
            buckets[bucketTs] = (b.Ts, b.Attempts + 1);
        }

        foreach (var b in buckets.Values.OrderBy(x => x.Ts))
        {
            var ts = DateTimeOffset.FromUnixTimeSeconds(b.Ts).UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ");
            sb.Append($"<member><Timestamp>{ts}</Timestamp>"
                    + $"<DeliveryAttempts>{b.Attempts}</DeliveryAttempts>"
                    + $"<Bounces>0</Bounces><Complaints>0</Complaints><Rejects>0</Rejects></member>");
        }

        return XmlResponse("GetSendStatisticsResponse",
            $"<GetSendStatisticsResult><SendDataPoints>{sb}</SendDataPoints></GetSendStatisticsResult>");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v1 — Configuration sets
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse ActCreateConfigurationSet(Dictionary<string, string> p)
    {
        var name = P(p, "ConfigurationSet.Name");
        if (name.Length == 0)
            return XmlError("ValidationError", "ConfigurationSet.Name is required", 400);
        lock (_lock)
        {
            if (_configurationSets.ContainsKey(name))
                return XmlError("ConfigurationSetAlreadyExists", $"Configuration set {name} already exists", 400);
            _configurationSets[name] = new SesConfigurationSet { Name = name, CreatedTimestamp = TimeHelpers.NowIso() };
        }
        return XmlResponse("CreateConfigurationSetResponse", "<CreateConfigurationSetResult/>");
    }

    private ServiceResponse ActDeleteConfigurationSet(Dictionary<string, string> p)
    {
        var name = P(p, "ConfigurationSetName");
        lock (_lock)
        {
            if (!_configurationSets.ContainsKey(name))
                return XmlError("ConfigurationSetDoesNotExist", $"Configuration set {name} does not exist", 400);
            _configurationSets.TryRemove(name, out _);
        }
        return XmlResponse("DeleteConfigurationSetResponse", "<DeleteConfigurationSetResult/>");
    }

    private ServiceResponse ActDescribeConfigurationSet(Dictionary<string, string> p)
    {
        var name = P(p, "ConfigurationSetName");
        if (!_configurationSets.TryGetValue(name, out var cs))
            return XmlError("ConfigurationSetDoesNotExist", $"Configuration set {name} does not exist", 400);
        return XmlResponse("DescribeConfigurationSetResponse",
            $"<DescribeConfigurationSetResult><ConfigurationSet><Name>{Esc(cs.Name)}</Name></ConfigurationSet></DescribeConfigurationSetResult>");
    }

    private ServiceResponse ActListConfigurationSets()
    {
        var sb = new StringBuilder();
        foreach (var cs in _configurationSets.Values)
        {
            sb.Append($"<member><Name>{Esc(cs.Name)}</Name></member>");
        }
        return XmlResponse("ListConfigurationSetsResponse",
            $"<ListConfigurationSetsResult><ConfigurationSets>{sb}</ConfigurationSets></ListConfigurationSetsResult>");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v1 — Templates
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse ActCreateTemplate(Dictionary<string, string> p)
    {
        var name = P(p, "Template.TemplateName");
        if (name.Length == 0)
            return XmlError("ValidationError", "Template.TemplateName is required", 400);
        lock (_lock)
        {
            if (_templates.ContainsKey(name))
                return XmlError("AlreadyExists", $"Template {name} already exists", 400);
            _templates[name] = new SesTemplate
            {
                TemplateName = name,
                SubjectPart = P(p, "Template.SubjectPart"),
                TextPart = P(p, "Template.TextPart"),
                HtmlPart = P(p, "Template.HtmlPart"),
                CreatedTimestamp = TimeHelpers.NowIso(),
            };
        }
        return XmlResponse("CreateTemplateResponse", "<CreateTemplateResult/>");
    }

    private ServiceResponse ActGetTemplate(Dictionary<string, string> p)
    {
        var name = P(p, "TemplateName");
        if (!_templates.TryGetValue(name, out var tpl))
            return XmlError("TemplateDoesNotExist", $"Template {name} does not exist", 400);
        return XmlResponse("GetTemplateResponse",
            $"<GetTemplateResult><Template>"
            + $"<TemplateName>{Esc(tpl.TemplateName)}</TemplateName>"
            + $"<SubjectPart>{Esc(tpl.SubjectPart)}</SubjectPart>"
            + $"<TextPart>{Esc(tpl.TextPart)}</TextPart>"
            + $"<HtmlPart>{Esc(tpl.HtmlPart)}</HtmlPart>"
            + $"</Template></GetTemplateResult>");
    }

    private ServiceResponse ActDeleteTemplate(Dictionary<string, string> p)
    {
        var name = P(p, "TemplateName");
        lock (_lock) { _templates.TryRemove(name, out _); }
        return XmlResponse("DeleteTemplateResponse", "<DeleteTemplateResult/>");
    }

    private ServiceResponse ActListTemplates()
    {
        var sb = new StringBuilder();
        foreach (var t in _templates.Values)
        {
            sb.Append($"<member><Name>{Esc(t.TemplateName)}</Name>"
                    + $"<CreatedTimestamp>{t.CreatedTimestamp}</CreatedTimestamp></member>");
        }
        return XmlResponse("ListTemplatesResponse",
            $"<ListTemplatesResult><TemplatesMetadata>{sb}</TemplatesMetadata></ListTemplatesResult>");
    }

    private ServiceResponse ActUpdateTemplate(Dictionary<string, string> p)
    {
        var name = P(p, "Template.TemplateName");
        if (!_templates.TryGetValue(name, out var tpl))
            return XmlError("TemplateDoesNotExist", $"Template {name} does not exist", 400);
        lock (_lock)
        {
            var subj = P(p, "Template.SubjectPart");
            if (subj.Length > 0) tpl.SubjectPart = subj;
            var text = P(p, "Template.TextPart");
            if (text.Length > 0) tpl.TextPart = text;
            var html = P(p, "Template.HtmlPart");
            if (html.Length > 0) tpl.HtmlPart = html;
        }
        return XmlResponse("UpdateTemplateResponse", "<UpdateTemplateResult/>");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v2 — REST/JSON dispatcher
    // ═══════════════════════════════════════════════════════════════════════════

    [GeneratedRegex(@"^/identities/(.+)$")]
    private static partial Regex V2IdentityIdRegex();

    [GeneratedRegex(@"^/configuration-sets/([^/]+)$")]
    private static partial Regex V2ConfigSetIdRegex();

    [GeneratedRegex(@"^/templates/(.+)$")]
    private static partial Regex V2TemplateIdRegex();

    private ServiceResponse HandleV2(ServiceRequest request)
    {
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
                data = JsonDocument.Parse("{}").RootElement.Clone();
            }
        }
        else
        {
            data = JsonDocument.Parse("{}").RootElement.Clone();
        }

        var method = request.Method;
        var route = request.Path.TrimEnd('/');
        if (route.StartsWith("/v2/email", StringComparison.Ordinal))
            route = route["/v2/email".Length..];

        // Account
        if (route == "/account" && method == "GET")
            return V2GetAccount();
        if (route == "/account/suppression" && method == "PUT")
            return JsonResponse(new { });
        if (route == "/suppression/addresses" && method == "GET")
            return JsonResponse(new { SuppressedDestinationSummaries = Array.Empty<object>(), NextToken = (string?)null });

        // Send
        if (route == "/outbound-emails" && method == "POST")
            return V2SendEmail(data);

        // Identities
        if (route == "/identities" && method == "POST")
            return V2CreateIdentity(data);
        if (route == "/identities" && method == "GET")
            return V2ListIdentities();

        // Configuration sets
        if (route == "/configuration-sets" && method == "POST")
            return V2CreateConfigurationSet(data);
        if (route == "/configuration-sets" && method == "GET")
            return V2ListConfigurationSets();

        // Templates
        if (route == "/templates" && method == "POST")
            return V2CreateTemplate(data);
        if (route == "/templates" && method == "GET")
            return V2ListTemplates();

        // Tags
        if (route == "/tags" && method == "GET")
        {
            var arn = request.GetQueryParam("ResourceArn") ?? "";
            return JsonResponse(new { Tags = _sesTags.TryGetValue(arn, out var tags) ? tags : new List<Dictionary<string, string>>() });
        }
        if (route == "/tags" && method == "POST")
        {
            return V2TagResource(data);
        }
        if (route == "/tags" && method == "DELETE")
        {
            return V2UntagResource(request);
        }

        // Identity by name
        var idMatch = V2IdentityIdRegex().Match(route);
        if (idMatch.Success)
        {
            var identity = Uri.UnescapeDataString(idMatch.Groups[1].Value);
            if (method == "GET") return V2GetIdentity(identity);
            if (method == "DELETE") return V2DeleteIdentity(identity);
        }

        // Configuration set by name
        var csMatch = V2ConfigSetIdRegex().Match(route);
        if (csMatch.Success)
        {
            var name = Uri.UnescapeDataString(csMatch.Groups[1].Value);
            if (method == "GET") return V2GetConfigurationSet(name);
            if (method == "DELETE") return V2DeleteConfigurationSet(name);
        }

        // Template by name
        var tplMatch = V2TemplateIdRegex().Match(route);
        if (tplMatch.Success)
        {
            var name = Uri.UnescapeDataString(tplMatch.Groups[1].Value);
            if (method == "GET") return V2GetTemplate(name);
            if (method == "PUT") return V2UpdateTemplate(name, data);
            if (method == "DELETE") return V2DeleteTemplate(name);
        }

        return JsonErrorResponse("NotFoundException", $"Unknown SES v2 path: {method} {request.Path}", 404);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v2 — Send
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse V2SendEmail(JsonElement data)
    {
        var msgId = $"ministack-{HashHelpers.NewUuid()}";
        var fromAddr = GetStr(data, "FromEmailAddress") ?? "";

        lock (_lock)
        {
            _sentEmails.Add(new SesEmail
            {
                MessageId = msgId,
                Source = fromAddr,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                EmailType = "v2.SendEmail",
            });
        }

        return JsonResponse(new { MessageId = msgId });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v2 — Identities
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse V2CreateIdentity(JsonElement data)
    {
        var identity = GetStr(data, "EmailIdentity") ?? "";
        if (identity.Length == 0)
            return JsonErrorResponse("BadRequestException", "EmailIdentity is required", 400);

        var identityType = identity.Contains('@') ? "EMAIL_ADDRESS" : "DOMAIN";
        lock (_lock)
        {
            _identities[identity] = MakeIdentity(identity,
                identityType == "DOMAIN" ? "Domain" : "EmailAddress");
        }

        return JsonResponse(new
        {
            IdentityType = identityType,
            VerifiedForSendingStatus = true,
            DkimAttributes = new { SigningEnabled = false, Status = "NOT_STARTED", Tokens = Array.Empty<string>() },
        });
    }

    private ServiceResponse V2ListIdentities()
    {
        var items = new List<object>();
        foreach (var (identity, info) in _identities)
        {
            items.Add(new
            {
                IdentityType = info.IdentityType == "Domain" ? "DOMAIN" : "EMAIL_ADDRESS",
                IdentityName = identity,
                SendingEnabled = true,
            });
        }
        return JsonResponse(new { EmailIdentities = items, NextToken = (string?)null });
    }

    private ServiceResponse V2GetIdentity(string identity)
    {
        if (!_identities.TryGetValue(identity, out var info))
            return JsonErrorResponse("NotFoundException", $"Identity {identity} not found", 404);

        return JsonResponse(new
        {
            EmailIdentity = identity,
            IdentityType = info.IdentityType == "Domain" ? "DOMAIN" : "EMAIL_ADDRESS",
            VerifiedForSendingStatus = info.VerificationStatus == "Success",
            FeedbackForwardingStatus = info.FeedbackForwardingEnabled,
            DkimAttributes = new
            {
                SigningEnabled = info.DkimEnabled,
                Status = info.DkimVerificationStatus ?? "NOT_STARTED",
                Tokens = info.DkimTokens ?? [],
            },
            MailFromAttributes = new { BehaviorOnMxFailure = "USE_DEFAULT_VALUE" },
        });
    }

    private ServiceResponse V2DeleteIdentity(string identity)
    {
        lock (_lock) { _identities.TryRemove(identity, out _); }
        return JsonResponse(new { });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v2 — Configuration sets
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse V2CreateConfigurationSet(JsonElement data)
    {
        var name = GetStr(data, "ConfigurationSetName") ?? "";
        if (name.Length == 0)
            return JsonErrorResponse("BadRequestException", "ConfigurationSetName is required", 400);
        lock (_lock)
        {
            if (_configurationSets.ContainsKey(name))
                return JsonErrorResponse("AlreadyExistsException", $"Configuration set {name} already exists", 409);
            _configurationSets[name] = new SesConfigurationSet { Name = name, CreatedTimestamp = TimeHelpers.NowIso() };
        }
        return JsonResponse(new { });
    }

    private ServiceResponse V2ListConfigurationSets()
    {
        var items = _configurationSets.Values.Select(cs => cs.Name).ToList();
        return JsonResponse(new { ConfigurationSets = items, NextToken = (string?)null });
    }

    private ServiceResponse V2GetConfigurationSet(string name)
    {
        if (!_configurationSets.TryGetValue(name, out var cs))
            return JsonErrorResponse("NotFoundException", $"Configuration set {name} not found", 404);
        return JsonResponse(new { ConfigurationSetName = cs.Name });
    }

    private ServiceResponse V2DeleteConfigurationSet(string name)
    {
        lock (_lock)
        {
            if (!_configurationSets.ContainsKey(name))
                return JsonErrorResponse("NotFoundException", $"Configuration set {name} not found", 404);
            _configurationSets.TryRemove(name, out _);
        }
        return JsonResponse(new { });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v2 — Templates
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse V2CreateTemplate(JsonElement data)
    {
        var name = GetStr(data, "TemplateName") ?? "";
        if (name.Length == 0)
            return JsonErrorResponse("BadRequestException", "TemplateName is required", 400);

        var content = data.TryGetProperty("TemplateContent", out var tc) ? tc : default;

        lock (_lock)
        {
            if (_templates.ContainsKey(name))
                return JsonErrorResponse("AlreadyExistsException", $"Template {name} already exists", 409);
            _templates[name] = new SesTemplate
            {
                TemplateName = name,
                SubjectPart = GetStr(content, "Subject") ?? "",
                TextPart = GetStr(content, "Text") ?? "",
                HtmlPart = GetStr(content, "Html") ?? "",
                CreatedTimestamp = TimeHelpers.NowIso(),
            };
        }
        return JsonResponse(new { });
    }

    private ServiceResponse V2ListTemplates()
    {
        var items = _templates.Values.Select(t => new
        {
            t.TemplateName,
            t.CreatedTimestamp,
        }).ToList();
        return JsonResponse(new { TemplatesMetadata = items });
    }

    private ServiceResponse V2GetTemplate(string name)
    {
        if (!_templates.TryGetValue(name, out var tpl))
            return JsonErrorResponse("NotFoundException", $"Template {name} not found", 404);
        return JsonResponse(new
        {
            tpl.TemplateName,
            TemplateContent = new
            {
                Subject = tpl.SubjectPart,
                Text = tpl.TextPart,
                Html = tpl.HtmlPart,
            },
        });
    }

    private ServiceResponse V2UpdateTemplate(string name, JsonElement data)
    {
        if (!_templates.TryGetValue(name, out var tpl))
            return JsonErrorResponse("NotFoundException", $"Template {name} not found", 404);

        var content = data.TryGetProperty("TemplateContent", out var tc) ? tc : default;
        lock (_lock)
        {
            var s = GetStr(content, "Subject");
            if (s is not null) tpl.SubjectPart = s;
            var t = GetStr(content, "Text");
            if (t is not null) tpl.TextPart = t;
            var h = GetStr(content, "Html");
            if (h is not null) tpl.HtmlPart = h;
        }
        return JsonResponse(new { });
    }

    private ServiceResponse V2DeleteTemplate(string name)
    {
        lock (_lock) { _templates.TryRemove(name, out _); }
        return JsonResponse(new { });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v2 — Tags
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse V2TagResource(JsonElement data)
    {
        var arn = GetStr(data, "ResourceArn") ?? "";
        var tagsEl = data.TryGetProperty("Tags", out var tagsArr) ? tagsArr : default;
        lock (_lock)
        {
            if (!_sesTags.TryGetValue(arn, out var existing))
            {
                existing = [];
                _sesTags[arn] = existing;
            }
            var existingMap = existing.ToDictionary(t => t["Key"]);
            if (tagsEl.ValueKind == JsonValueKind.Array)
            {
                foreach (var tag in tagsEl.EnumerateArray())
                {
                    var key = GetStr(tag, "Key") ?? "";
                    var val = GetStr(tag, "Value") ?? "";
                    existingMap[key] = new Dictionary<string, string> { ["Key"] = key, ["Value"] = val };
                }
            }
            _sesTags[arn] = existingMap.Values.ToList();
        }
        return JsonResponse(new { });
    }

    private ServiceResponse V2UntagResource(ServiceRequest request)
    {
        var arn = request.GetQueryParam("ResourceArn") ?? "";
        lock (_lock)
        {
            if (_sesTags.TryGetValue(arn, out var existing))
            {
                var removeKeys = new HashSet<string>(StringComparer.Ordinal);
                if (request.QueryParams.TryGetValue("TagKeys", out var keys))
                {
                    foreach (var k in keys) removeKeys.Add(k);
                }
                _sesTags[arn] = existing.Where(t => !removeKeys.Contains(t["Key"])).ToList();
            }
        }
        return JsonResponse(new { });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // v2 — Account
    // ═══════════════════════════════════════════════════════════════════════════

    private ServiceResponse V2GetAccount()
    {
        double sent24H;
        lock (_lock)
        {
            var cutoff = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 86400;
            sent24H = _sentEmails.Count(e => e.Timestamp >= cutoff);
        }

        return JsonResponse(new
        {
            DedicatedIpAutoWarmupEnabled = false,
            EnforcementStatus = "HEALTHY",
            ProductionAccessEnabled = true,
            SendQuota = new
            {
                Max24HourSend = 50000.0,
                MaxSendRate = 14.0,
                SentLast24Hours = sent24H,
            },
            SendingEnabled = true,
            SuppressionAttributes = new { SuppressedReasons = Array.Empty<string>() },
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Shared helpers
    // ═══════════════════════════════════════════════════════════════════════════

    private static SesIdentity MakeIdentity(string identity, string identityType) => new()
    {
        VerificationStatus = "Success",
        IdentityType = identityType,
        DkimEnabled = false,
        DkimTokens = [],
        DkimVerificationStatus = "NotStarted",
        NotificationTopics = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Bounce"] = "",
            ["Complaint"] = "",
            ["Delivery"] = "",
        },
        FeedbackForwardingEnabled = true,
    };

    private static List<string> CollectList(Dictionary<string, string> p, string prefix)
    {
        var result = new List<string>();
        var i = 1;
        while (true)
        {
            var val = P(p, $"{prefix}.{i}");
            if (val.Length == 0) break;
            result.Add(val);
            i++;
        }
        return result;
    }

    private static string P(Dictionary<string, string> p, string key)
        => p.TryGetValue(key, out var val) ? val : "";

    private static string Esc(string text)
    {
        if (string.IsNullOrEmpty(text)) return "";
        return text
            .Replace("&", "&amp;")
            .Replace("<", "&lt;")
            .Replace(">", "&gt;")
            .Replace("\"", "&quot;");
    }

    private static ServiceResponse XmlResponse(string rootTag, string inner)
    {
        var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + $"<{rootTag} xmlns=\"{SesXmlNs}\">"
                + inner
                + $"<ResponseMetadata><RequestId>{HashHelpers.NewUuid()}</RequestId></ResponseMetadata>"
                + $"</{rootTag}>";
        var body = Encoding.UTF8.GetBytes(xml);
        return new ServiceResponse(200, XmlHeaders, body);
    }

    private static ServiceResponse XmlError(string code, string message, int status)
    {
        var xml = $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + $"<ErrorResponse xmlns=\"{SesXmlNs}\">"
                + $"<Error><Code>{code}</Code><Message>{Esc(message)}</Message></Error>"
                + $"<RequestId>{HashHelpers.NewUuid()}</RequestId>"
                + $"</ErrorResponse>";
        var body = Encoding.UTF8.GetBytes(xml);
        return new ServiceResponse(status, XmlHeaders, body);
    }

    private static ServiceResponse JsonResponse(object data)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(data, JsonOpts);
        return new ServiceResponse(200, JsonResponseHeaders, json);
    }

    private static ServiceResponse JsonErrorResponse(string code, string message, int status)
    {
        var data = new { __type = code, message };
        var json = JsonSerializer.SerializeToUtf8Bytes(data, JsonOpts);
        return new ServiceResponse(status, JsonResponseHeaders, json);
    }

    private static string? GetStr(JsonElement el, string propertyName)
    {
        if (el.ValueKind == JsonValueKind.Undefined) return null;
        return el.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
    }

    private static readonly Dictionary<string, string> XmlHeaders = new()
        { ["Content-Type"] = "application/xml" };

    private static readonly Dictionary<string, string> JsonResponseHeaders = new()
        { ["Content-Type"] = "application/json" };

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
    };

    // ── Internal record types ────────────────────────────────────────────────

    private sealed class SesIdentity
    {
        internal string VerificationStatus { get; set; } = "Success";
        internal string IdentityType { get; set; } = "EmailAddress";
        internal bool DkimEnabled { get; set; }
        internal List<string> DkimTokens { get; set; } = [];
        internal string? DkimVerificationStatus { get; set; } = "NotStarted";
        internal Dictionary<string, string> NotificationTopics { get; set; } = new(StringComparer.Ordinal);
        internal bool FeedbackForwardingEnabled { get; set; } = true;
    }

    private sealed class SesEmail
    {
        internal string MessageId { get; set; } = "";
        internal string Source { get; set; } = "";
        internal List<string> To { get; set; } = [];
        internal string Subject { get; set; } = "";
        internal string BodyText { get; set; } = "";
        internal string BodyHtml { get; set; } = "";
        internal long Timestamp { get; set; }
        internal string EmailType { get; set; } = "";
        internal string? Template { get; set; }
        internal string? TemplateData { get; set; }
        internal string? ConfigurationSetName { get; set; }
    }

    private sealed class SesTemplate
    {
        internal string TemplateName { get; set; } = "";
        internal string SubjectPart { get; set; } = "";
        internal string TextPart { get; set; } = "";
        internal string HtmlPart { get; set; } = "";
        internal string CreatedTimestamp { get; set; } = "";
    }

    private sealed class SesConfigurationSet
    {
        internal string Name { get; set; } = "";
        internal string CreatedTimestamp { get; set; } = "";
    }
}
