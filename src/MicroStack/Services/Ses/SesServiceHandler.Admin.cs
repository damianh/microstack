using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Ses;

internal sealed partial class SesServiceHandler : IAdminResourceSource
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("identities", "Identities"),
        new("templates", "Templates"),
        new("configuration-sets", "Configuration sets"),
        new("emails", "Sent emails")
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "ses" ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "ses")
            return [];

        var account = AccountContext.GetAccountId();
        IdentitySnapshot[] identities;
        TemplateSnapshot[] templates;
        ConfigurationSetSnapshot[] configurationSets;
        EmailSnapshot[] emails;
        lock (_lock)
        {
            identities = _identities.Items.Select(item => new IdentitySnapshot(
                item.Key,
                item.Value.VerificationStatus,
                item.Value.IdentityType,
                item.Value.DkimEnabled,
                item.Value.DkimTokens.ToArray(),
                item.Value.DkimVerificationStatus,
                new(item.Value.NotificationTopics, StringComparer.Ordinal),
                item.Value.FeedbackForwardingEnabled)).ToArray();
            templates = _templates.Values.Select(template => new TemplateSnapshot(
                template.TemplateName, template.SubjectPart, template.TextPart,
                template.HtmlPart, template.CreatedTimestamp)).ToArray();
            configurationSets = _configurationSets.Values.Select(set =>
                new ConfigurationSetSnapshot(set.Name, set.CreatedTimestamp)).ToArray();
            emails = _sentEmails.Where(email => email.AccountId == account).Select(email =>
                new EmailSnapshot(email.MessageId, email.Source, email.To.ToArray(), email.Subject,
                    email.BodyText, email.BodyHtml, email.Timestamp, email.EmailType,
                    email.Template, email.TemplateData, email.ConfigurationSetName)).ToArray();
        }

        return identities.Select(IdentityNode)
            .Concat(templates.Select(TemplateNode))
            .Concat(configurationSets.Select(ConfigurationSetNode))
            .Concat(emails.Select(EmailNode))
            .ToArray();
    }

    private static AdminNode IdentityNode(IdentitySnapshot identity) =>
        AdminData.Node("identities", identity.Name, identity.Name, status: identity.VerificationStatus) with
        {
            ReadFields = () =>
            [
                AdminData.Field("Type", identity.Type),
                AdminData.Field("Verification status", identity.VerificationStatus),
                AdminData.Field("DKIM enabled", identity.DkimEnabled.ToString()),
                AdminData.Field("DKIM verification status", identity.DkimVerificationStatus),
                AdminData.Field("DKIM tokens", string.Join(", ", identity.DkimTokens), sensitive: identity.DkimTokens.Length > 0),
                AdminData.Field("Feedback forwarding", identity.FeedbackForwardingEnabled.ToString())
            ],
            ReadConnections = () => identity.NotificationTopics
                .OrderBy(item => item.Key, StringComparer.Ordinal)
                .Select(item => new AdminConnection(item.Key + " notifications", "publishes-to", "sns",
                    [new("topics", item.Value)], State: "configured")).ToArray()
        };

    private static AdminNode TemplateNode(TemplateSnapshot template) =>
        AdminData.Node("templates", template.Name, template.Name, status: "active") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Created", template.Created, format: "datetime"),
                AdminData.Field("Subject", template.Subject)
            ],
            ReadContent = () => AdminData.Json(new Dictionary<string, object?>
            {
                ["subject"] = template.Subject,
                ["text"] = template.Text,
                ["html"] = template.Html
            }, sensitive: true)
        };

    private static AdminNode ConfigurationSetNode(ConfigurationSetSnapshot set) =>
        AdminData.Node("configuration-sets", set.Name, set.Name, status: "active") with
        {
            ReadFields = () => [AdminData.Field("Created", set.Created, format: "datetime")]
        };

    private static AdminNode EmailNode(EmailSnapshot email) =>
        AdminData.Node("emails", email.MessageId, email.Subject.Length > 0 ? email.Subject : email.MessageId,
            status: "sent") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Message ID", email.MessageId),
                AdminData.Field("Type", email.Type),
                AdminData.Field("From", email.Source),
                AdminData.Field("To", string.Join(", ", email.To)),
                AdminData.Field("Sent", AdminData.IsoUtc(DateTimeOffset.FromUnixTimeSeconds(email.Timestamp)), format: "datetime"),
                AdminData.Field("Template", email.Template),
                AdminData.Field("Configuration set", email.ConfigurationSet)
            ],
            ReadContent = () => AdminData.Json(new Dictionary<string, object?>
            {
                ["subject"] = email.Subject,
                ["text"] = email.BodyText,
                ["html"] = email.BodyHtml,
                ["templateData"] = email.TemplateData
            }, sensitive: true),
            ReadConnections = () =>
            {
                var connections = new List<AdminConnection>();
                if (!string.IsNullOrEmpty(email.Template))
                    connections.Add(new("Template", "rendered-with", "ses",
                        [new("templates", email.Template)], State: "configured"));
                if (!string.IsNullOrEmpty(email.ConfigurationSet))
                    connections.Add(new("Configuration set", "uses", "ses",
                        [new("configuration-sets", email.ConfigurationSet)], State: "configured"));
                return connections;
            }
        };

    private sealed record IdentitySnapshot(
        string Name, string VerificationStatus, string Type, bool DkimEnabled,
        string[] DkimTokens, string? DkimVerificationStatus,
        Dictionary<string, string> NotificationTopics, bool FeedbackForwardingEnabled);
    private sealed record TemplateSnapshot(
        string Name, string Subject, string Text, string Html, string Created);
    private sealed record ConfigurationSetSnapshot(string Name, string Created);
    private sealed record EmailSnapshot(
        string MessageId, string Source, string[] To, string Subject,
        string BodyText, string BodyHtml, long Timestamp, string Type,
        string? Template, string? TemplateData, string? ConfigurationSet);
}
