using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Sns;

internal sealed partial class SnsServiceHandler : IAdminResourceSource
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("topics", "Topics"),
        new("subscriptions", "Subscriptions"),
        new("platform-applications", "Platform applications"),
        new("platform-endpoints", "Platform endpoints")
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "sns" ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "sns")
            return [];

        TopicSnapshot[] topics;
        AppSnapshot[] applications;
        lock (_lock)
        {
            topics = _topics.Values.Select(topic => new TopicSnapshot(
                topic.Name,
                topic.Arn,
                new(topic.Attributes, StringComparer.Ordinal),
                new(topic.Tags, StringComparer.Ordinal),
                topic.Subscriptions.Select(subscription => new SubscriptionSnapshot(
                    subscription.Arn,
                    subscription.Protocol,
                    subscription.Endpoint,
                    subscription.Confirmed,
                    subscription.TopicArn,
                    subscription.Owner,
                    new(subscription.Attributes, StringComparer.Ordinal))).ToArray())).ToArray();

            var endpoints = _platformEndpoints.Values.Select(endpoint => new EndpointSnapshot(
                endpoint.Arn, endpoint.ApplicationArn,
                new(endpoint.Attributes, StringComparer.Ordinal))).ToLookup(endpoint => endpoint.ApplicationArn);
            applications = _platformApps.Values.Select(application => new AppSnapshot(
                application.Arn,
                application.Name,
                application.Platform,
                new(application.Attributes, StringComparer.Ordinal),
                endpoints[application.Arn].ToArray())).ToArray();
        }

        return topics.Select(TopicNode).Concat(applications.Select(ApplicationNode)).ToArray();
    }

    private static AdminNode TopicNode(TopicSnapshot topic) =>
        AdminData.Node("topics", topic.Arn, topic.Name, topic.Arn, "active") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Display name", topic.Attributes.GetValueOrDefault("DisplayName")),
                AdminData.Field("Owner", topic.Attributes.GetValueOrDefault("Owner")),
                AdminData.Field("Confirmed subscriptions", topic.Subscriptions.Count(item => item.Confirmed).ToString()),
                AdminData.Field("Pending subscriptions", topic.Subscriptions.Count(item => !item.Confirmed).ToString()),
                AdminData.Field("Tags", topic.Tags.Count.ToString())
            ],
            ReadChildren = () => topic.Subscriptions.Select(SubscriptionNode).ToArray()
        };

    private static AdminNode SubscriptionNode(SubscriptionSnapshot subscription) =>
        AdminData.Node("subscriptions", subscription.Arn, subscription.Endpoint,
            subscription.Arn, subscription.Confirmed ? "confirmed" : "pending") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Protocol", subscription.Protocol),
                AdminData.Field("Endpoint", subscription.Endpoint),
                AdminData.Field("Owner", subscription.Owner),
                AdminData.Field("Raw message delivery", subscription.Attributes.GetValueOrDefault("RawMessageDelivery")),
                AdminData.Field("Filter policy", subscription.Attributes.GetValueOrDefault("FilterPolicy"), format: "json")
            ],
            ReadConnections = () => SubscriptionConnections(subscription)
        };

    private static IReadOnlyList<AdminConnection> SubscriptionConnections(SubscriptionSnapshot subscription)
    {
        if (subscription.Protocol == "sqs" && subscription.Endpoint.StartsWith("arn:", StringComparison.Ordinal))
            return [new("Subscribed queue", "delivers-to", "sqs",
                [new("queues", subscription.Endpoint)], State: "configured")];
        if (subscription.Protocol == "lambda" && subscription.Endpoint.StartsWith("arn:", StringComparison.Ordinal))
            return [new("Subscribed function", "invokes", "lambda",
                [new("functions", subscription.Endpoint)], State: "configured")];
        if (subscription.Protocol is "http" or "https"
            && Uri.TryCreate(subscription.Endpoint, UriKind.Absolute, out _))
            return [new("Subscribed endpoint", "delivers-to", ExternalUri: subscription.Endpoint, State: "configured")];
        return [new(subscription.Endpoint, "delivers-to", State: "configured")];
    }

    private static AdminNode ApplicationNode(AppSnapshot application) =>
        AdminData.Node("platform-applications", application.Arn, application.Name,
            application.Arn, "active") with
        {
            ReadFields = () => SafeAttributes(application.Attributes)
                .Prepend(AdminData.Field("Platform", application.Platform)).ToArray(),
            ReadChildren = () => application.Endpoints.Select(EndpointNode).ToArray()
        };

    private static AdminNode EndpointNode(EndpointSnapshot endpoint) =>
        AdminData.Node("platform-endpoints", endpoint.Arn, endpoint.Arn, endpoint.Arn,
            endpoint.Attributes.GetValueOrDefault("Enabled", "true") == "true" ? "enabled" : "disabled") with
        {
            ReadFields = () => SafeAttributes(endpoint.Attributes)
        };

    private static AdminField[] SafeAttributes(IReadOnlyDictionary<string, string> attributes) =>
        attributes.OrderBy(item => item.Key, StringComparer.Ordinal)
            .Select(item => IsCredential(item.Key)
                ? AdminData.Field(item.Key, item.Value, sensitive: true)
                : AdminData.Field(item.Key, item.Value))
            .ToArray();

    private static bool IsCredential(string name) =>
        name.Contains("credential", StringComparison.OrdinalIgnoreCase)
        || name.Contains("secret", StringComparison.OrdinalIgnoreCase)
        || name.Contains("token", StringComparison.OrdinalIgnoreCase)
        || name.Contains("key", StringComparison.OrdinalIgnoreCase);

    private sealed record TopicSnapshot(
        string Name, string Arn, Dictionary<string, string> Attributes,
        Dictionary<string, string> Tags, SubscriptionSnapshot[] Subscriptions);
    private sealed record SubscriptionSnapshot(
        string Arn, string Protocol, string Endpoint, bool Confirmed,
        string TopicArn, string Owner, Dictionary<string, string> Attributes);
    private sealed record AppSnapshot(
        string Arn, string Name, string Platform,
        Dictionary<string, string> Attributes, EndpointSnapshot[] Endpoints);
    private sealed record EndpointSnapshot(
        string Arn, string ApplicationArn, Dictionary<string, string> Attributes);
}
