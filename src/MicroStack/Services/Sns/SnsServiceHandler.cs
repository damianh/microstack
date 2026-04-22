using System.Text;
using System.Text.Json;
using System.Web;
using MicroStack.Internal;
using MicroStack.Services.Sqs;

namespace MicroStack.Services.Sns;

/// <summary>
/// SNS service handler — supports Query/XML protocol (Action= form params).
///
/// Port of ministack/services/sns.py.
///
/// Supports: CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes,
///           Subscribe, Unsubscribe, ConfirmSubscription,
///           ListSubscriptions, ListSubscriptionsByTopic,
///           GetSubscriptionAttributes, SetSubscriptionAttributes,
///           Publish, PublishBatch,
///           ListTagsForResource, TagResource, UntagResource,
///           CreatePlatformApplication, CreatePlatformEndpoint.
/// SNS -> SQS fanout delivers synchronously into the SQS handler's queues.
/// </summary>
internal sealed class SnsServiceHandler : IServiceHandler
{
    private readonly SqsServiceHandler _sqs;

    private readonly AccountScopedDictionary<string, SnsTopic> _topics = new();
    private readonly AccountScopedDictionary<string, string> _subArnToTopic = new();
    private readonly AccountScopedDictionary<string, SnsPlatformApp> _platformApps = new();
    private readonly AccountScopedDictionary<string, SnsPlatformEndpoint> _platformEndpoints = new();
    private readonly Lock _lock = new();

    private static string Region => MicroStackOptions.Instance.Region;

    private const string SnsXmlNs = "http://sns.amazonaws.com/doc/2010-03-31/";

    internal SnsServiceHandler(SqsServiceHandler sqsHandler)
    {
        _sqs = sqsHandler;
    }

    // ── IServiceHandler ─────────────────────────────────────────────────────────

    public string ServiceName => "sns";

    public Task<ServiceResponse> HandleAsync(ServiceRequest request)
    {
        // SNS uses Query (form-encoded) protocol — parse form params from body and query string.
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
            "CreateTopic"               => ActCreateTopic(formParams),
            "DeleteTopic"               => ActDeleteTopic(formParams),
            "ListTopics"                => ActListTopics(formParams),
            "GetTopicAttributes"        => ActGetTopicAttributes(formParams),
            "SetTopicAttributes"        => ActSetTopicAttributes(formParams),
            "Subscribe"                 => ActSubscribe(formParams),
            "ConfirmSubscription"       => ActConfirmSubscription(formParams),
            "Unsubscribe"               => ActUnsubscribe(formParams),
            "ListSubscriptions"         => ActListSubscriptions(formParams),
            "ListSubscriptionsByTopic"  => ActListSubscriptionsByTopic(formParams),
            "GetSubscriptionAttributes" => ActGetSubscriptionAttributes(formParams),
            "SetSubscriptionAttributes" => ActSetSubscriptionAttributes(formParams),
            "Publish"                   => ActPublish(formParams),
            "PublishBatch"              => ActPublishBatch(formParams),
            "ListTagsForResource"       => ActListTagsForResource(formParams),
            "TagResource"               => ActTagResource(formParams),
            "UntagResource"             => ActUntagResource(formParams),
            "CreatePlatformApplication" => ActCreatePlatformApplication(formParams),
            "CreatePlatformEndpoint"    => ActCreatePlatformEndpoint(formParams),
            _                           => XmlError("InvalidAction", $"Unknown action: {action}", 400),
        };

        return Task.FromResult(response);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _topics.Clear();
            _subArnToTopic.Clear();
            _platformApps.Clear();
            _platformEndpoints.Clear();
        }
    }

    public JsonElement? GetState()
    {
        lock (_lock)
        {
            var topics = _topics.ToRaw()
                .Select(kv => new SnsTopicEntry(kv.Key.AccountId, kv.Key.Key, kv.Value))
                .ToList();
            var subs = _subArnToTopic.ToRaw()
                .Select(kv => new SnsSubEntry(kv.Key.AccountId, kv.Key.Key, kv.Value))
                .ToList();
            var apps = _platformApps.ToRaw()
                .Select(kv => new SnsAppEntry(kv.Key.AccountId, kv.Key.Key, kv.Value))
                .ToList();
            var endpoints = _platformEndpoints.ToRaw()
                .Select(kv => new SnsEndpointEntry(kv.Key.AccountId, kv.Key.Key, kv.Value))
                .ToList();
            var state = new SnsState(topics, subs, apps, endpoints);
            return JsonSerializer.SerializeToElement(state, MicroStackJsonContext.Default.SnsState);
        }
    }

    public void RestoreState(JsonElement state)
    {
        var restored = JsonSerializer.Deserialize(state, MicroStackJsonContext.Default.SnsState);
        if (restored is null) return;
        lock (_lock)
        {
            _topics.FromRaw(restored.Topics.Select(e =>
                new KeyValuePair<(string, string), SnsTopic>((e.AccountId, e.Key), e.Value)));
            _subArnToTopic.FromRaw(restored.Subs.Select(e =>
                new KeyValuePair<(string, string), string>((e.AccountId, e.Key), e.Value)));
            _platformApps.FromRaw(restored.Apps.Select(e =>
                new KeyValuePair<(string, string), SnsPlatformApp>((e.AccountId, e.Key), e.Value)));
            _platformEndpoints.FromRaw(restored.Endpoints.Select(e =>
                new KeyValuePair<(string, string), SnsPlatformEndpoint>((e.AccountId, e.Key), e.Value)));
        }
    }

    // ── Topic management ────────────────────────────────────────────────────────

    private ServiceResponse ActCreateTopic(Dictionary<string, string> p)
    {
        var name = P(p, "Name");
        if (string.IsNullOrEmpty(name))
            return XmlError("InvalidParameterException", "Name is required", 400);

        var accountId = AccountContext.GetAccountId();
        var arn = $"arn:aws:sns:{Region}:{accountId}:{name}";

        lock (_lock)
        {
            if (!_topics.ContainsKey(arn))
            {
                var defaultPolicy =
                    $"{{\"Version\":\"2008-10-17\",\"Id\":\"__default_policy_ID\",\"Statement\":[{{" +
                    $"\"Sid\":\"__default_statement_ID\",\"Effect\":\"Allow\"," +
                    $"\"Principal\":{{\"AWS\":\"*\"}}," +
                    $"\"Action\":[\"SNS:Publish\",\"SNS:Subscribe\",\"SNS:Receive\"]," +
                    $"\"Resource\":\"{arn}\"," +
                    $"\"Condition\":{{\"StringEquals\":{{\"AWS:SourceOwner\":\"{accountId}\"}}}}}}]}}";

                const string effectivePolicy =
                    "{\"http\":{\"defaultHealthyRetryPolicy\":{" +
                    "\"minDelayTarget\":20,\"maxDelayTarget\":20,\"numRetries\":3}}}";

                var topic = new SnsTopic
                {
                    Name = name,
                    Arn = arn,
                    Attributes = new Dictionary<string, string>(StringComparer.Ordinal)
                    {
                        ["TopicArn"] = arn,
                        ["DisplayName"] = name,
                        ["Owner"] = accountId,
                        ["Policy"] = defaultPolicy,
                        ["SubscriptionsConfirmed"] = "0",
                        ["SubscriptionsPending"] = "0",
                        ["SubscriptionsDeleted"] = "0",
                        ["EffectiveDeliveryPolicy"] = effectivePolicy,
                    },
                };

                // Parse Attributes.entry.N.key/value
                for (var i = 1; ; i++)
                {
                    var key = P(p, $"Attributes.entry.{i}.key");
                    if (string.IsNullOrEmpty(key)) break;
                    var val = P(p, $"Attributes.entry.{i}.value");
                    topic.Attributes[key] = val;
                }

                _topics[arn] = topic;
            }
        }

        return XmlOk("CreateTopicResponse",
            $"<CreateTopicResult><TopicArn>{arn}</TopicArn></CreateTopicResult>");
    }

    private ServiceResponse ActDeleteTopic(Dictionary<string, string> p)
    {
        var arn = P(p, "TopicArn");
        lock (_lock)
        {
            if (_topics.TryRemove(arn, out var topic))
            {
                foreach (var sub in topic.Subscriptions)
                {
                    _subArnToTopic.TryRemove(sub.Arn, out _);
                }
            }
        }
        return XmlOk("DeleteTopicResponse", "");
    }

    private ServiceResponse ActListTopics(Dictionary<string, string> p)
    {
        List<string> allArns;
        lock (_lock)
        {
            allArns = _topics.Keys.ToList();
        }

        var nextToken = P(p, "NextToken");
        var start = 0;
        if (!string.IsNullOrEmpty(nextToken) && int.TryParse(nextToken, out var parsed))
            start = parsed;

        var page = allArns.Skip(start).Take(100).ToList();
        var members = new StringBuilder();
        foreach (var arn in page)
        {
            members.Append($"<member><TopicArn>{arn}</TopicArn></member>");
        }

        var nextTokenXml = start + 100 < allArns.Count
            ? $"<NextToken>{start + 100}</NextToken>"
            : "";

        return XmlOk("ListTopicsResponse",
            $"<ListTopicsResult><Topics>{members}</Topics>{nextTokenXml}</ListTopicsResult>");
    }

    private ServiceResponse ActGetTopicAttributes(Dictionary<string, string> p)
    {
        var arn = P(p, "TopicArn");
        lock (_lock)
        {
            if (!_topics.TryGetValue(arn, out var topic))
                return XmlError("NotFoundException", $"Topic does not exist: {arn}", 404);

            RefreshSubscriptionCounts(topic);
            var attrs = new StringBuilder();
            foreach (var (k, v) in topic.Attributes)
            {
                attrs.Append($"<entry><key>{k}</key><value>{XmlEsc(v)}</value></entry>");
            }

            return XmlOk("GetTopicAttributesResponse",
                $"<GetTopicAttributesResult><Attributes>{attrs}</Attributes></GetTopicAttributesResult>");
        }
    }

    private ServiceResponse ActSetTopicAttributes(Dictionary<string, string> p)
    {
        var arn = P(p, "TopicArn");
        var attrName = P(p, "AttributeName");
        var attrVal = P(p, "AttributeValue");

        lock (_lock)
        {
            if (!_topics.TryGetValue(arn, out var topic))
                return XmlError("NotFoundException", $"Topic does not exist: {arn}", 404);

            if (!string.IsNullOrEmpty(attrName))
                topic.Attributes[attrName] = attrVal;
        }

        return XmlOk("SetTopicAttributesResponse", "");
    }

    // ── Subscriptions ───────────────────────────────────────────────────────────

    private ServiceResponse ActSubscribe(Dictionary<string, string> p)
    {
        var topicArn = P(p, "TopicArn");
        var protocol = P(p, "Protocol");
        var endpoint = P(p, "Endpoint");

        lock (_lock)
        {
            if (!_topics.TryGetValue(topicArn, out var topic))
                return XmlError("NotFoundException", $"Topic does not exist: {topicArn}", 404);

            if (string.IsNullOrEmpty(protocol))
                return XmlError("InvalidParameterException", "Protocol is required", 400);

            // Idempotent: return existing subscription
            foreach (var existing in topic.Subscriptions)
            {
                if (existing.Protocol == protocol && existing.Endpoint == endpoint)
                {
                    return XmlOk("SubscribeResponse",
                        $"<SubscribeResult><SubscriptionArn>{existing.Arn}</SubscriptionArn></SubscribeResult>");
                }
            }

            var subArn = $"{topicArn}:{HashHelpers.NewUuid()}";
            var needsConfirmation = protocol is "http" or "https";

            var sub = new SnsSubscription
            {
                Arn = subArn,
                Protocol = protocol,
                Endpoint = endpoint,
                Confirmed = !needsConfirmation,
                TopicArn = topicArn,
                Owner = AccountContext.GetAccountId(),
                Token = needsConfirmation ? HashHelpers.NewUuid() : null,
                Attributes = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["SubscriptionArn"] = subArn,
                    ["TopicArn"] = topicArn,
                    ["Protocol"] = protocol,
                    ["Endpoint"] = endpoint,
                    ["Owner"] = AccountContext.GetAccountId(),
                    ["ConfirmationWasAuthenticated"] = needsConfirmation ? "false" : "true",
                    ["PendingConfirmation"] = needsConfirmation ? "true" : "false",
                    ["RawMessageDelivery"] = "false",
                },
            };

            // Parse subscription attributes
            var allowedAttrs = new HashSet<string>(StringComparer.Ordinal)
            {
                "DeliveryPolicy", "FilterPolicy", "FilterPolicyScope",
                "RawMessageDelivery", "RedrivePolicy",
            };
            for (var i = 1; ; i++)
            {
                var key = P(p, $"Attributes.entry.{i}.key");
                if (string.IsNullOrEmpty(key)) break;
                var val = P(p, $"Attributes.entry.{i}.value");
                if (allowedAttrs.Contains(key))
                    sub.Attributes[key] = val;
            }

            topic.Subscriptions.Add(sub);
            _subArnToTopic[subArn] = topicArn;
            RefreshSubscriptionCounts(topic);

            var resultArn = needsConfirmation ? "PendingConfirmation" : subArn;
            return XmlOk("SubscribeResponse",
                $"<SubscribeResult><SubscriptionArn>{resultArn}</SubscriptionArn></SubscribeResult>");
        }
    }

    private ServiceResponse ActConfirmSubscription(Dictionary<string, string> p)
    {
        var topicArn = P(p, "TopicArn");
        var token = P(p, "Token");

        lock (_lock)
        {
            if (!_topics.TryGetValue(topicArn, out var topic))
                return XmlError("NotFoundException", $"Topic does not exist: {topicArn}", 404);

            if (string.IsNullOrEmpty(token))
                return XmlError("InvalidParameterException", "Token is required", 400);

            foreach (var sub in topic.Subscriptions)
            {
                if (sub.Token == token)
                {
                    sub.Confirmed = true;
                    sub.Token = null;
                    sub.Attributes["PendingConfirmation"] = "false";
                    sub.Attributes["ConfirmationWasAuthenticated"] = "true";
                    RefreshSubscriptionCounts(topic);
                    return XmlOk("ConfirmSubscriptionResponse",
                        $"<ConfirmSubscriptionResult><SubscriptionArn>{sub.Arn}</SubscriptionArn></ConfirmSubscriptionResult>");
                }
            }
        }

        return XmlError("InvalidParameterException", "Invalid token", 400);
    }

    private ServiceResponse ActUnsubscribe(Dictionary<string, string> p)
    {
        var subArn = P(p, "SubscriptionArn");
        lock (_lock)
        {
            if (_subArnToTopic.TryGetValue(subArn, out var topicArn)
                && _topics.TryGetValue(topicArn, out var topic))
            {
                topic.Subscriptions.RemoveAll(s => s.Arn == subArn);
                RefreshSubscriptionCounts(topic);
            }
            _subArnToTopic.TryRemove(subArn, out _);
        }
        return XmlOk("UnsubscribeResponse", "");
    }

    private ServiceResponse ActListSubscriptions(Dictionary<string, string> p)
    {
        List<SnsSubscription> allSubs;
        lock (_lock)
        {
            allSubs = _topics.Values.SelectMany(t => t.Subscriptions).ToList();
        }

        var nextToken = P(p, "NextToken");
        var start = 0;
        if (!string.IsNullOrEmpty(nextToken) && int.TryParse(nextToken, out var parsed))
            start = parsed;

        var page = allSubs.Skip(start).Take(100).ToList();
        var members = BuildSubscriptionMembers(page);

        var nextTokenXml = start + 100 < allSubs.Count
            ? $"<NextToken>{start + 100}</NextToken>"
            : "";

        return XmlOk("ListSubscriptionsResponse",
            $"<ListSubscriptionsResult><Subscriptions>{members}</Subscriptions>{nextTokenXml}</ListSubscriptionsResult>");
    }

    private ServiceResponse ActListSubscriptionsByTopic(Dictionary<string, string> p)
    {
        var topicArn = P(p, "TopicArn");
        lock (_lock)
        {
            if (!_topics.TryGetValue(topicArn, out var topic))
                return XmlError("NotFoundException", $"Topic does not exist: {topicArn}", 404);

            var members = BuildSubscriptionMembers(topic.Subscriptions);
            return XmlOk("ListSubscriptionsByTopicResponse",
                $"<ListSubscriptionsByTopicResult><Subscriptions>{members}</Subscriptions></ListSubscriptionsByTopicResult>");
        }
    }

    private ServiceResponse ActGetSubscriptionAttributes(Dictionary<string, string> p)
    {
        var subArn = P(p, "SubscriptionArn");
        lock (_lock)
        {
            if (!_subArnToTopic.TryGetValue(subArn, out var topicArn)
                || !_topics.TryGetValue(topicArn, out var topic))
                return XmlError("NotFoundException", $"Subscription does not exist: {subArn}", 404);

            var sub = FindSubscription(topic, subArn);
            if (sub is null)
                return XmlError("NotFoundException", $"Subscription does not exist: {subArn}", 404);

            var attrs = new StringBuilder();
            foreach (var (k, v) in sub.Attributes)
            {
                attrs.Append($"<entry><key>{k}</key><value>{XmlEsc(v)}</value></entry>");
            }

            return XmlOk("GetSubscriptionAttributesResponse",
                $"<GetSubscriptionAttributesResult><Attributes>{attrs}</Attributes></GetSubscriptionAttributesResult>");
        }
    }

    private ServiceResponse ActSetSubscriptionAttributes(Dictionary<string, string> p)
    {
        var subArn = P(p, "SubscriptionArn");
        var attrName = P(p, "AttributeName");
        var attrVal = P(p, "AttributeValue");

        lock (_lock)
        {
            if (!_subArnToTopic.TryGetValue(subArn, out var topicArn)
                || !_topics.TryGetValue(topicArn, out var topic))
                return XmlError("NotFoundException", $"Subscription does not exist: {subArn}", 404);

            var sub = FindSubscription(topic, subArn);
            if (sub is null)
                return XmlError("NotFoundException", $"Subscription does not exist: {subArn}", 404);

            var allowed = new HashSet<string>(StringComparer.Ordinal)
            {
                "DeliveryPolicy", "FilterPolicy", "FilterPolicyScope",
                "RawMessageDelivery", "RedrivePolicy",
            };
            if (!allowed.Contains(attrName))
                return XmlError("InvalidParameterException", $"Invalid attribute name: {attrName}", 400);

            if (attrName == "FilterPolicy" && !string.IsNullOrEmpty(attrVal))
            {
                try
                {
                    JsonDocument.Parse(attrVal).Dispose();
                }
                catch (JsonException)
                {
                    return XmlError("InvalidParameterException", "Invalid JSON in FilterPolicy", 400);
                }
            }

            sub.Attributes[attrName] = attrVal;
        }

        return XmlOk("SetSubscriptionAttributesResponse", "");
    }

    // ── Publish ─────────────────────────────────────────────────────────────────

    private ServiceResponse ActPublish(Dictionary<string, string> p)
    {
        var topicArn = P(p, "TopicArn");
        if (string.IsNullOrEmpty(topicArn))
            topicArn = P(p, "TargetArn");
        var phoneNumber = P(p, "PhoneNumber");
        var message = P(p, "Message");
        var subject = P(p, "Subject");
        var messageStructure = P(p, "MessageStructure");

        if (!string.IsNullOrEmpty(phoneNumber) && string.IsNullOrEmpty(topicArn))
        {
            // SMS stub
            var smsId = HashHelpers.NewUuid();
            return XmlOk("PublishResponse",
                $"<PublishResult><MessageId>{smsId}</MessageId></PublishResult>");
        }

        if (string.IsNullOrEmpty(topicArn))
            return XmlError("InvalidParameterException",
                "TopicArn, TargetArn, or PhoneNumber is required", 400);

        lock (_lock)
        {
            if (!_topics.TryGetValue(topicArn, out var topic))
                return XmlError("NotFoundException", $"Topic does not exist: {topicArn}", 404);

            var msgAttrs = ParseMessageAttributes(p);
            var msgId = HashHelpers.NewUuid();

            topic.Messages.Add(new SnsMessage
            {
                Id = msgId,
                Message = message,
                Subject = subject,
                MessageStructure = messageStructure,
                MessageAttributes = msgAttrs,
                Timestamp = TimeHelpers.NowEpoch(),
            });

            var groupId = P(p, "MessageGroupId");
            var dedupId = P(p, "MessageDeduplicationId");

            Fanout(topic, msgId, message, subject, messageStructure, msgAttrs,
                groupId, dedupId);

            return XmlOk("PublishResponse",
                $"<PublishResult><MessageId>{msgId}</MessageId></PublishResult>");
        }
    }

    private ServiceResponse ActPublishBatch(Dictionary<string, string> p)
    {
        var topicArn = P(p, "TopicArn");
        if (string.IsNullOrEmpty(topicArn))
            return XmlError("InvalidParameterException", "TopicArn is required", 400);

        lock (_lock)
        {
            if (!_topics.TryGetValue(topicArn, out var topic))
                return XmlError("NotFoundException", $"Topic does not exist: {topicArn}", 404);

            var entries = ParseBatchEntries(p);
            if (entries.Count == 0)
                return XmlError("InvalidParameterException",
                    "PublishBatchRequestEntries is required", 400);
            if (entries.Count > 10)
                return XmlError("TooManyEntriesInBatchRequest",
                    "The batch request contains more entries than permissible", 400);

            var idsSeen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var entry in entries)
            {
                if (!idsSeen.Add(entry.Id))
                    return XmlError("BatchEntryIdsNotDistinct",
                        "Batch entry ids must be distinct", 400);
            }

            var successful = new StringBuilder();
            foreach (var entry in entries)
            {
                var msgId = HashHelpers.NewUuid();
                topic.Messages.Add(new SnsMessage
                {
                    Id = msgId,
                    Message = entry.Message,
                    Subject = entry.Subject,
                    MessageStructure = entry.MessageStructure,
                    MessageAttributes = entry.MessageAttributes,
                    Timestamp = TimeHelpers.NowEpoch(),
                });

                Fanout(topic, msgId, entry.Message, entry.Subject, entry.MessageStructure,
                    entry.MessageAttributes, "", "");

                successful.Append(
                    "<member>"
                    + $"<Id>{XmlEsc(entry.Id)}</Id>"
                    + $"<MessageId>{msgId}</MessageId>"
                    + "</member>");
            }

            return XmlOk("PublishBatchResponse",
                $"<PublishBatchResult>"
                + $"<Successful>{successful}</Successful>"
                + "<Failed></Failed>"
                + "</PublishBatchResult>");
        }
    }

    // ── Fanout ──────────────────────────────────────────────────────────────────

    /// <summary>Deliver published message to all confirmed subscribers that match filters.</summary>
    /// <remarks>Must be called while holding <see cref="_lock"/>.</remarks>
    private void Fanout(
        SnsTopic topic,
        string msgId,
        string message,
        string subject,
        string messageStructure,
        Dictionary<string, SnsMessageAttribute> messageAttributes,
        string messageGroupId,
        string messageDeduplicationId)
    {
        foreach (var sub in topic.Subscriptions)
        {
            if (!sub.Confirmed) continue;

            if (!MatchesFilterPolicy(sub, messageAttributes))
                continue;

            var effectiveMessage = ResolveMessageForProtocol(message, messageStructure, sub.Protocol);
            var raw = sub.Attributes.GetValueOrDefault("RawMessageDelivery", "false") == "true";
            var envelope = BuildEnvelope(topic.Arn, msgId, effectiveMessage, subject,
                messageAttributes, raw);

            switch (sub.Protocol)
            {
                case "sqs":
                    DeliverToSqs(sub.Endpoint, envelope, raw, effectiveMessage,
                        messageGroupId, messageDeduplicationId);
                    break;
                case "lambda":
                    // Lambda fanout will be implemented when Lambda handler is ported.
                    break;
                case "http" or "https":
                    // HTTP delivery is a stub — real AWS would POST asynchronously.
                    break;
                case "email" or "email-json" or "sms" or "application":
                    // Stubs — no delivery in the emulator.
                    break;
            }
        }
    }

    private void DeliverToSqs(
        string sqsEndpoint,
        string envelope,
        bool raw,
        string rawMessage,
        string messageGroupId,
        string messageDeduplicationId)
    {
        // endpoint is an SQS ARN: arn:aws:sqs:region:account:queue-name
        var queueName = sqsEndpoint.Split(':')[^1];
        var body = raw ? rawMessage : envelope;

        _sqs.InjectMessage(
            queueName,
            body,
            string.IsNullOrEmpty(messageGroupId) ? null : messageGroupId,
            string.IsNullOrEmpty(messageDeduplicationId) ? null : messageDeduplicationId);
    }

    // ── Tags ────────────────────────────────────────────────────────────────────

    private ServiceResponse ActListTagsForResource(Dictionary<string, string> p)
    {
        var arn = P(p, "ResourceArn");
        var tagsXml = new StringBuilder();
        lock (_lock)
        {
            if (_topics.TryGetValue(arn, out var topic))
            {
                foreach (var (k, v) in topic.Tags)
                {
                    tagsXml.Append($"<member><Key>{k}</Key><Value>{v}</Value></member>");
                }
            }
        }

        return XmlOk("ListTagsForResourceResponse",
            $"<ListTagsForResourceResult><Tags>{tagsXml}</Tags></ListTagsForResourceResult>");
    }

    private ServiceResponse ActTagResource(Dictionary<string, string> p)
    {
        var arn = P(p, "ResourceArn");
        lock (_lock)
        {
            if (!_topics.TryGetValue(arn, out var topic))
                return XmlError("ResourceNotFoundException", "Resource not found", 404);

            for (var i = 1; ; i++)
            {
                var key = P(p, $"Tags.member.{i}.Key");
                if (string.IsNullOrEmpty(key)) break;
                var val = P(p, $"Tags.member.{i}.Value");
                topic.Tags[key] = val;
            }
        }

        return XmlOk("TagResourceResponse", "<TagResourceResult/>");
    }

    private ServiceResponse ActUntagResource(Dictionary<string, string> p)
    {
        var arn = P(p, "ResourceArn");
        lock (_lock)
        {
            if (_topics.TryGetValue(arn, out var topic))
            {
                for (var i = 1; ; i++)
                {
                    var key = P(p, $"TagKeys.member.{i}");
                    if (string.IsNullOrEmpty(key)) break;
                    topic.Tags.Remove(key);
                }
            }
        }

        return XmlOk("UntagResourceResponse", "<UntagResourceResult/>");
    }

    // ── Platform application stubs ──────────────────────────────────────────────

    private ServiceResponse ActCreatePlatformApplication(Dictionary<string, string> p)
    {
        var name = P(p, "Name");
        var platform = P(p, "Platform");
        if (string.IsNullOrEmpty(name) || string.IsNullOrEmpty(platform))
            return XmlError("InvalidParameterException", "Name and Platform are required", 400);

        var arn = $"arn:aws:sns:{Region}:{AccountContext.GetAccountId()}:app/{platform}/{name}";
        var attrs = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 1; ; i++)
        {
            var key = P(p, $"Attributes.entry.{i}.key");
            if (string.IsNullOrEmpty(key)) break;
            var val = P(p, $"Attributes.entry.{i}.value");
            attrs[key] = val;
        }

        lock (_lock)
        {
            _platformApps[arn] = new SnsPlatformApp
            {
                Arn = arn,
                Name = name,
                Platform = platform,
                Attributes = attrs,
            };
        }

        return XmlOk("CreatePlatformApplicationResponse",
            $"<CreatePlatformApplicationResult>"
            + $"<PlatformApplicationArn>{arn}</PlatformApplicationArn>"
            + "</CreatePlatformApplicationResult>");
    }

    private ServiceResponse ActCreatePlatformEndpoint(Dictionary<string, string> p)
    {
        var appArn = P(p, "PlatformApplicationArn");
        var token = P(p, "Token");

        lock (_lock)
        {
            if (!_platformApps.ContainsKey(appArn))
                return XmlError("NotFoundException", $"PlatformApplication does not exist: {appArn}", 404);
        }

        if (string.IsNullOrEmpty(token))
            return XmlError("InvalidParameterException", "Token is required", 400);

        var endpointArn = $"{appArn}/{HashHelpers.NewUuid()}";
        var attrs = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Enabled"] = "true",
            ["Token"] = token,
        };
        for (var i = 1; ; i++)
        {
            var key = P(p, $"Attributes.entry.{i}.key");
            if (string.IsNullOrEmpty(key)) break;
            var val = P(p, $"Attributes.entry.{i}.value");
            attrs[key] = val;
        }

        lock (_lock)
        {
            _platformEndpoints[endpointArn] = new SnsPlatformEndpoint
            {
                Arn = endpointArn,
                ApplicationArn = appArn,
                Attributes = attrs,
            };
        }

        return XmlOk("CreatePlatformEndpointResponse",
            $"<CreatePlatformEndpointResult>"
            + $"<EndpointArn>{endpointArn}</EndpointArn>"
            + "</CreatePlatformEndpointResult>");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────────

    private static string P(Dictionary<string, string> p, string key) =>
        p.GetValueOrDefault(key, "");

    private static SnsSubscription? FindSubscription(SnsTopic topic, string subArn) =>
        topic.Subscriptions.Find(s => s.Arn == subArn);

    private static void RefreshSubscriptionCounts(SnsTopic topic)
    {
        var confirmed = topic.Subscriptions.Count(s => s.Confirmed);
        var pending = topic.Subscriptions.Count(s => !s.Confirmed);
        topic.Attributes["SubscriptionsConfirmed"] = confirmed.ToString();
        topic.Attributes["SubscriptionsPending"] = pending.ToString();
    }

    private static string BuildSubscriptionMembers(IReadOnlyList<SnsSubscription> subs)
    {
        var sb = new StringBuilder();
        foreach (var sub in subs)
        {
            sb.Append("<member>")
              .Append($"<SubscriptionArn>{sub.Arn}</SubscriptionArn>")
              .Append($"<Owner>{sub.Owner}</Owner>")
              .Append($"<TopicArn>{sub.TopicArn}</TopicArn>")
              .Append($"<Protocol>{sub.Protocol}</Protocol>")
              .Append($"<Endpoint>{sub.Endpoint}</Endpoint>")
              .Append("</member>");
        }
        return sb.ToString();
    }

    private static Dictionary<string, SnsMessageAttribute> ParseMessageAttributes(Dictionary<string, string> p)
    {
        var attrs = new Dictionary<string, SnsMessageAttribute>(StringComparer.Ordinal);
        for (var i = 1; ; i++)
        {
            var name = P(p, $"MessageAttributes.entry.{i}.Name");
            if (string.IsNullOrEmpty(name)) break;
            var dataType = P(p, $"MessageAttributes.entry.{i}.Value.DataType");
            var stringVal = P(p, $"MessageAttributes.entry.{i}.Value.StringValue");
            var binaryVal = P(p, $"MessageAttributes.entry.{i}.Value.BinaryValue");
            attrs[name] = new SnsMessageAttribute
            {
                DataType = dataType,
                StringValue = string.IsNullOrEmpty(stringVal) ? null : stringVal,
                BinaryValue = string.IsNullOrEmpty(binaryVal) ? null : binaryVal,
            };
        }
        return attrs;
    }

    private static List<SnsBatchEntry> ParseBatchEntries(Dictionary<string, string> p)
    {
        var entries = new List<SnsBatchEntry>();
        for (var i = 1; ; i++)
        {
            var eid = P(p, $"PublishBatchRequestEntries.member.{i}.Id");
            if (string.IsNullOrEmpty(eid)) break;
            var entry = new SnsBatchEntry
            {
                Id = eid,
                Message = P(p, $"PublishBatchRequestEntries.member.{i}.Message"),
                Subject = P(p, $"PublishBatchRequestEntries.member.{i}.Subject"),
                MessageStructure = P(p, $"PublishBatchRequestEntries.member.{i}.MessageStructure"),
            };

            for (var j = 1; ; j++)
            {
                var attrName = P(p, $"PublishBatchRequestEntries.member.{i}.MessageAttributes.entry.{j}.Name");
                if (string.IsNullOrEmpty(attrName)) break;
                var dataType = P(p, $"PublishBatchRequestEntries.member.{i}.MessageAttributes.entry.{j}.Value.DataType");
                var stringVal = P(p, $"PublishBatchRequestEntries.member.{i}.MessageAttributes.entry.{j}.Value.StringValue");
                entry.MessageAttributes[attrName] = new SnsMessageAttribute
                {
                    DataType = dataType,
                    StringValue = string.IsNullOrEmpty(stringVal) ? null : stringVal,
                };
            }

            entries.Add(entry);
        }
        return entries;
    }

    private static string ResolveMessageForProtocol(string message, string messageStructure, string protocol)
    {
        if (messageStructure != "json") return message;
        try
        {
            using var doc = JsonDocument.Parse(message);
            if (doc.RootElement.ValueKind != JsonValueKind.Object) return message;
            if (doc.RootElement.TryGetProperty(protocol, out var protocolVal))
                return protocolVal.GetString() ?? message;
            if (doc.RootElement.TryGetProperty("default", out var defaultVal))
                return defaultVal.GetString() ?? message;
            return message;
        }
        catch (JsonException)
        {
            return message;
        }
    }

    private static bool MatchesFilterPolicy(SnsSubscription sub, Dictionary<string, SnsMessageAttribute> messageAttributes)
    {
        var policyJson = sub.Attributes.GetValueOrDefault("FilterPolicy", "");
        if (string.IsNullOrEmpty(policyJson)) return true;

        Dictionary<string, JsonElement> policy;
        try
        {
            policy = JsonSerializer.Deserialize(policyJson, MicroStackJsonContext.Default.DictionaryStringJsonElement)
                     ?? new Dictionary<string, JsonElement>();
        }
        catch (JsonException)
        {
            return true;
        }

        var scope = sub.Attributes.GetValueOrDefault("FilterPolicyScope", "MessageAttributes");
        if (scope == "MessageBody") return true;

        foreach (var (key, allowedValues) in policy)
        {
            if (!messageAttributes.TryGetValue(key, out var attr))
                return false;

            var attrValue = attr.StringValue ?? "";
            if (!AttrMatchesAny(attrValue, allowedValues))
                return false;
        }

        return true;
    }

    private static bool AttrMatchesAny(string attrValue, JsonElement rules)
    {
        // Normalise to array
        var items = rules.ValueKind == JsonValueKind.Array
            ? rules.EnumerateArray().ToList()
            : [rules];

        foreach (var rule in items)
        {
            switch (rule.ValueKind)
            {
                case JsonValueKind.String:
                    if (attrValue == rule.GetString()) return true;
                    break;
                case JsonValueKind.Number:
                    if (double.TryParse(attrValue, out var numVal) && numVal == rule.GetDouble())
                        return true;
                    break;
                case JsonValueKind.Object:
                    if (rule.TryGetProperty("exists", out var existsProp))
                    {
                        if (existsProp.ValueKind == JsonValueKind.True) return true;
                        continue;
                    }
                    if (rule.TryGetProperty("prefix", out var prefixProp))
                    {
                        if (attrValue.StartsWith(prefixProp.GetString() ?? "", StringComparison.Ordinal))
                            return true;
                    }
                    if (rule.TryGetProperty("anything-but", out var anythingButProp))
                    {
                        if (anythingButProp.ValueKind == JsonValueKind.Array)
                        {
                            var excluded = anythingButProp.EnumerateArray()
                                .Select(e => e.GetString() ?? "").ToHashSet(StringComparer.Ordinal);
                            if (!excluded.Contains(attrValue)) return true;
                        }
                        else
                        {
                            if (attrValue != (anythingButProp.GetString() ?? anythingButProp.ToString()))
                                return true;
                        }
                    }
                    if (rule.TryGetProperty("numeric", out var numericProp))
                    {
                        if (double.TryParse(attrValue, out var nv))
                        {
                            if (CheckNumeric(nv, numericProp)) return true;
                        }
                    }
                    break;
            }
        }

        return false;
    }

    private static bool CheckNumeric(double value, JsonElement conditions)
    {
        if (conditions.ValueKind != JsonValueKind.Array) return false;
        var arr = conditions.EnumerateArray().ToList();
        for (var i = 0; i < arr.Count - 1; i += 2)
        {
            var op = arr[i].GetString() ?? "";
            var threshold = arr[i + 1].GetDouble();
            var ok = op switch
            {
                "=" => value == threshold,
                ">" => value > threshold,
                ">=" => value >= threshold,
                "<" => value < threshold,
                "<=" => value <= threshold,
                _ => true,
            };
            if (!ok) return false;
        }
        return true;
    }

    private static string BuildEnvelope(
        string topicArn,
        string msgId,
        string message,
        string subject,
        Dictionary<string, SnsMessageAttribute> messageAttributes,
        bool raw)
    {
        if (raw) return message;

        var envelope = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Type"] = "Notification",
            ["MessageId"] = msgId,
            ["TopicArn"] = topicArn,
            ["Subject"] = string.IsNullOrEmpty(subject) ? null : subject,
            ["Message"] = message,
            ["Timestamp"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.000") + "Z",
            ["SignatureVersion"] = "1",
            ["Signature"] = "FAKE",
            ["SigningCertURL"] = "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-fake.pem",
            ["UnsubscribeURL"] = $"http://localhost:4566/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:{Region}:{AccountContext.GetAccountId()}:example",
        };

        if (messageAttributes.Count > 0)
        {
            var formatted = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var (name, attr) in messageAttributes)
            {
                formatted[name] = new Dictionary<string, string>
                {
                    ["Type"] = attr.DataType ?? "String",
                    ["Value"] = attr.StringValue ?? "",
                };
            }
            envelope["MessageAttributes"] = formatted;
        }

        // Remove null values and serialize
        var clean = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (k, v) in envelope)
        {
            if (v is not null) clean[k] = v;
        }

        return DictionaryObjectJsonConverter.SerializeValue(clean);
    }

    // ── XML response helpers ────────────────────────────────────────────────────

    private static ServiceResponse XmlOk(string rootTag, string inner)
    {
        var body =
            $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + $"<{rootTag} xmlns=\"{SnsXmlNs}\">"
            + inner
            + $"<ResponseMetadata><RequestId>{HashHelpers.NewUuid()}</RequestId></ResponseMetadata>"
            + $"</{rootTag}>";

        return new ServiceResponse(200, XmlContentType, Encoding.UTF8.GetBytes(body));
    }

    private static ServiceResponse XmlError(string code, string message, int status)
    {
        var errorType = status < 500 ? "Sender" : "Receiver";
        var body =
            $"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + $"<ErrorResponse xmlns=\"{SnsXmlNs}\">"
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

// ── Data models ─────────────────────────────────────────────────────────────────

internal sealed class SnsTopic
{
    public required string Name { get; set; }
    public required string Arn { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new(StringComparer.Ordinal);
    public List<SnsSubscription> Subscriptions { get; set; } = [];
    public List<SnsMessage> Messages { get; set; } = [];
    public Dictionary<string, string> Tags { get; set; } = new(StringComparer.Ordinal);
}

internal sealed class SnsSubscription
{
    public required string Arn { get; set; }
    public required string Protocol { get; set; }
    public required string Endpoint { get; set; }
    public bool Confirmed { get; set; }
    public required string TopicArn { get; set; }
    public required string Owner { get; set; }
    public string? Token { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new(StringComparer.Ordinal);
}

internal sealed class SnsMessage
{
    public required string Id { get; set; }
    public required string Message { get; set; }
    public string Subject { get; set; } = "";
    public string MessageStructure { get; set; } = "";
    public Dictionary<string, SnsMessageAttribute> MessageAttributes { get; set; } = new(StringComparer.Ordinal);
    public double Timestamp { get; set; }
}

internal sealed class SnsMessageAttribute
{
    public string? DataType { get; set; }
    public string? StringValue { get; set; }
    public string? BinaryValue { get; set; }
}

internal sealed class SnsBatchEntry
{
    public required string Id { get; set; }
    public string Message { get; set; } = "";
    public string Subject { get; set; } = "";
    public string MessageStructure { get; set; } = "";
    public Dictionary<string, SnsMessageAttribute> MessageAttributes { get; set; } = new(StringComparer.Ordinal);
}

internal sealed class SnsPlatformApp
{
    public required string Arn { get; set; }
    public required string Name { get; set; }
    public required string Platform { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new(StringComparer.Ordinal);
}

internal sealed class SnsPlatformEndpoint
{
    public required string Arn { get; set; }
    public required string ApplicationArn { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new(StringComparer.Ordinal);
}

// Persistence state records for SnsServiceHandler
internal sealed record SnsTopicEntry(string AccountId, string Key, SnsTopic Value);
internal sealed record SnsSubEntry(string AccountId, string Key, string Value);
internal sealed record SnsAppEntry(string AccountId, string Key, SnsPlatformApp Value);
internal sealed record SnsEndpointEntry(string AccountId, string Key, SnsPlatformEndpoint Value);
internal sealed record SnsState(
    List<SnsTopicEntry> Topics,
    List<SnsSubEntry> Subs,
    List<SnsAppEntry> Apps,
    List<SnsEndpointEntry> Endpoints);
