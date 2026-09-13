using MicroStack.Admin.Contracts;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Sns;

internal sealed partial class SnsServiceHandler : IAdminResourceSource, IAdminRelationshipSource
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("topics", "Topics"),
        new("subscriptions", "Subscriptions") { IsRoot = false },
        new("platform-applications", "Platform applications"),
        new("platform-endpoints", "Platform endpoints") { IsRoot = false }
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
        AdminData.Node("topics", topic.Arn, topic.Name, topic.Arn, "active",
            type: topic.Attributes.GetValueOrDefault("FifoTopic") == "true" ? "FIFO" : "Standard",
            summary: [AdminData.Field("Subscriptions", topic.Subscriptions.Length.ToString())]) with
        {
            ReadSummary = () =>
            [
                AdminData.Field("Confirmed subscriptions", topic.Subscriptions.Count(item => item.Confirmed).ToString()),
                AdminData.Field("Pending subscriptions", topic.Subscriptions.Count(item => !item.Confirmed).ToString())
            ],
            ReadFields = () => topic.Attributes.OrderBy(item => item.Key, StringComparer.Ordinal)
                .Select(item => AdminData.Field(item.Key, item.Value,
                    format: item.Key is "Policy" or "DeliveryPolicy" or "EffectiveDeliveryPolicy" ? "json" : null,
                    secondary: item.Key is not ("DisplayName" or "Owner")))
                .Concat(topic.Tags.OrderBy(item => item.Key, StringComparer.Ordinal)
                    .Select(item => AdminData.Field($"Tag: {item.Key}", item.Value, secondary: true))).ToArray(),
            ChildKinds = [AdminKinds[1]],
            ReadChildren = () => topic.Subscriptions.Select(SubscriptionNode).ToArray(),
            ReadConnections = () => topic.Subscriptions.SelectMany(SubscriptionConnections).ToArray()
        };

    private static AdminNode SubscriptionNode(SubscriptionSnapshot subscription) =>
        AdminData.Node("subscriptions", subscription.Arn, subscription.Endpoint,
            subscription.Arn, subscription.Confirmed ? "confirmed" : "pending",
            type: subscription.Protocol,
            summary:
            [
                AdminData.Field("Protocol", subscription.Protocol),
                AdminData.Field("Endpoint", subscription.Endpoint),
                AdminData.Field("Confirmation", subscription.Confirmed ? "confirmed" : "pending"),
                AdminData.Field("Filter policy", subscription.Attributes.GetValueOrDefault("FilterPolicy"), format: "json")
            ]) with
        {
            ReadFields = () => new[]
            {
                AdminData.Field("Protocol", subscription.Protocol),
                AdminData.Field("Endpoint", subscription.Endpoint),
                AdminData.Field("Confirmation", subscription.Confirmed ? "confirmed" : "pending"),
                AdminData.Field("Topic ARN", subscription.TopicArn),
                AdminData.Field("Owner", subscription.Owner, secondary: true),
                AdminData.Field("Raw message delivery", subscription.Attributes.GetValueOrDefault("RawMessageDelivery")),
                AdminData.Field("Filter policy", subscription.Attributes.GetValueOrDefault("FilterPolicy"), format: "json")
            }.Concat(subscription.Attributes.OrderBy(item => item.Key, StringComparer.Ordinal)
                .Where(item => item.Key is not ("RawMessageDelivery" or "FilterPolicy" or "Protocol" or "Endpoint" or "Owner"))
                .Select(item => AdminData.Field(item.Key, item.Value, secondary: true))).ToArray(),
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
        return [new(subscription.Endpoint, "delivers-to", State: "external")];
    }

    private static AdminNode ApplicationNode(AppSnapshot application) =>
        AdminData.Node("platform-applications", application.Arn, application.Name,
            application.Arn, "active") with
        {
            ReadFields = () => SafeAttributes(application.Attributes)
                .Prepend(AdminData.Field("Platform", application.Platform)).ToArray(),
            ChildKinds = [AdminKinds[3]],
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

    public AdminRelationshipSnapshot GetAdminRelationshipSnapshot()
    {
        lock (_lock)
        {
            var resources = new List<AdminRelationshipResource>();
            var relationships = new List<AdminConfiguredRelationship>();
            foreach (var topic in _topics.Values)
            {
                AdminKey[] topicPath = [new("topics", topic.Arn)];
                resources.Add(new(topicPath, topic.Name));
                foreach (var subscription in topic.Subscriptions)
                {
                    AdminKey[] path = [.. topicPath, new("subscriptions", subscription.Arn)];
                    resources.Add(new(path, subscription.Endpoint));
                    var snapshot = new SubscriptionSnapshot(subscription.Arn, subscription.Protocol,
                        subscription.Endpoint, subscription.Confirmed, subscription.TopicArn,
                        subscription.Owner, new(subscription.Attributes, StringComparer.Ordinal));
                    relationships.AddRange(SubscriptionConnections(snapshot).Select(connection =>
                        new AdminConfiguredRelationship("sns", path, $"{topic.Name} / {subscription.Arn}", connection)));
                    relationships.Add(new("sns", path, topic.Name,
                        new("Parent topic", "belongs-to", "sns", [new("topics", subscription.TopicArn)])));
                }
            }
            foreach (var application in _platformApps.Values)
            {
                AdminKey[] path = [new("platform-applications", application.Arn)];
                resources.Add(new(path, application.Name));
                resources.AddRange(_platformEndpoints.Values.Where(endpoint => endpoint.ApplicationArn == application.Arn)
                    .Select(endpoint => new AdminRelationshipResource(
                        [.. path, new("platform-endpoints", endpoint.Arn)], endpoint.Arn,
                        endpoint.Attributes.GetValueOrDefault("Enabled", "true") == "true" ? "enabled" : "disabled")));
            }
            return new(resources.ToArray(), relationships.ToArray());
        }
    }

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
