using System.Text.Json;
using MicroStack.Admin.Contracts;
using MicroStack.Internal;
using MicroStack.Internal.Admin;

namespace MicroStack.Services.Sqs;

internal sealed partial class SqsServiceHandler : IAdminResourceSource
{
    private static readonly AdminResourceKind[] AdminKinds =
    [
        new("queues", "Queues"),
        new("messages", "Messages")
    ];

    public IReadOnlyList<AdminResourceKind> GetAdminResourceKinds(string serviceId) =>
        serviceId == "sqs" ? AdminKinds : [];

    public IEnumerable<AdminNode> GetAdminResources(string serviceId)
    {
        if (serviceId != "sqs")
            return [];

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        QueueSnapshot[] queues;
        lock (_lock)
        {
            queues = _queues.Values.Select(queue => new QueueSnapshot(
                queue.Name,
                queue.Url,
                queue.IsFifo,
                new(queue.Attributes, StringComparer.Ordinal),
                new(queue.Tags, StringComparer.Ordinal),
                queue.Messages.Select(message => new MessageSnapshot(
                    message.Id,
                    message.Body,
                    message.Md5Body,
                    message.Md5Attrs,
                    message.SentAtMs,
                    message.VisibleAtMs,
                    message.ReceiveCount,
                    message.FirstReceiveAtMs,
                    message.MessageAttributes.ToJsonString(),
                    new(message.SystemAttributes, StringComparer.Ordinal),
                    message.GroupId,
                    message.DedupId,
                    message.SequenceNumber)).ToArray())).ToArray();
        }

        return queues.Select(queue => QueueNode(queue, now)).ToArray();
    }

    private static AdminNode QueueNode(QueueSnapshot queue, long capturedAt)
    {
        var arn = queue.Attributes.GetValueOrDefault("QueueArn")
            ?? $"arn:aws:sqs:{_region}:{AccountContext.GetAccountId()}:{queue.Name}";
        var visible = queue.Messages.Count(message => MessageState(message, capturedAt) == "visible");
        var delayed = queue.Messages.Count(message => MessageState(message, capturedAt) == "delayed");
        var inFlight = queue.Messages.Length - visible - delayed;
        return AdminData.Node("queues", arn, queue.Name, arn, "available") with
        {
            ReadFields = () =>
            [
                AdminData.Field("Queue URL", queue.Url, format: "uri"),
                AdminData.Field("Type", queue.IsFifo ? "FIFO" : "Standard"),
                AdminData.Field("Visible messages", visible.ToString()),
                AdminData.Field("Delayed messages", delayed.ToString()),
                AdminData.Field("In-flight messages", inFlight.ToString()),
                AdminData.Field("Tags", queue.Tags.Count.ToString())
            ],
            ReadChildren = () => queue.Messages.Select(message => MessageNode(message, capturedAt)).ToArray(),
            ReadConnections = () => QueueConnections(queue.Attributes)
        };
    }

    private static AdminNode MessageNode(MessageSnapshot message, long capturedAt)
    {
        var state = MessageState(message, capturedAt);
        return AdminData.Node("messages", message.Id, message.Id, status: state) with
        {
            ReadFields = () =>
            [
                AdminData.Field("State", state),
                AdminData.Field("Sent", AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds(message.SentAtMs)), format: "datetime"),
                AdminData.Field("Visible at", AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds(message.VisibleAtMs)), format: "datetime"),
                AdminData.Field("Receive count", message.ReceiveCount.ToString()),
                AdminData.Field("First received", message.FirstReceiveAtMs is long first
                    ? AdminData.IsoUtc(DateTimeOffset.FromUnixTimeMilliseconds(first)) : null, format: "datetime"),
                AdminData.Field("Body MD5", message.Md5Body),
                AdminData.Field("Message attributes", message.MessageAttributes),
                AdminData.Field("System attributes", string.Join(", ",
                    message.SystemAttributes.OrderBy(item => item.Key, StringComparer.Ordinal)
                        .Select(item => $"{item.Key}={item.Value}"))),
                AdminData.Field("Message group ID", message.GroupId),
                AdminData.Field("Deduplication ID", message.DedupId),
                AdminData.Field("Sequence number", message.SequenceNumber)
            ],
            ReadContent = () => AdminData.Text(message.Body)
        };
    }

    private static string MessageState(MessageSnapshot message, long now) =>
        message.VisibleAtMs <= now ? "visible"
        : message.ReceiveCount > 0 || message.FirstReceiveAtMs is not null ? "in-flight"
        : "delayed";

    private static IReadOnlyList<AdminConnection> QueueConnections(IReadOnlyDictionary<string, string> attributes)
    {
        if (!attributes.TryGetValue("RedrivePolicy", out var policy) || string.IsNullOrWhiteSpace(policy))
            return [];
        try
        {
            using var json = JsonDocument.Parse(policy);
            if (!json.RootElement.TryGetProperty("deadLetterTargetArn", out var target))
                return [];
            var arn = target.GetString();
            return string.IsNullOrEmpty(arn)
                ? []
                : [new("Dead-letter queue", "redrive", "sqs", [new("queues", arn)], State: "configured")];
        }
        catch (JsonException)
        {
            return [];
        }
    }

    private sealed record QueueSnapshot(
        string Name, string Url, bool IsFifo,
        Dictionary<string, string> Attributes,
        Dictionary<string, string> Tags,
        MessageSnapshot[] Messages);

    private sealed record MessageSnapshot(
        string Id, string Body, string Md5Body, string? Md5Attrs,
        long SentAtMs, long VisibleAtMs, int ReceiveCount, long? FirstReceiveAtMs,
        string MessageAttributes, Dictionary<string, string> SystemAttributes,
        string? GroupId, string? DedupId, string? SequenceNumber);
}
