using MicroStack.Internal;
using MicroStack.Services.DynamoDb;
using MicroStack.Services.Sqs;

namespace MicroStack.Services.Lambda;

/// <summary>
/// Background polling loop for Lambda Event Source Mappings (ESMs).
/// Periodically polls SQS queues and DynamoDB Streams for records,
/// builds the appropriate event payload, and invokes the mapped Lambda function.
/// </summary>
internal sealed class EventSourceMappingPoller : IDisposable
{
    private readonly LambdaServiceHandler _lambdaHandler;
    private readonly SqsServiceHandler _sqsHandler;
    private readonly DynamoDbServiceHandler _ddbHandler;
    private Timer? _timer;
    private int _running; // 0 = idle, 1 = running (interlocked)

    internal EventSourceMappingPoller(
        LambdaServiceHandler lambdaHandler,
        SqsServiceHandler sqsHandler,
        DynamoDbServiceHandler ddbHandler)
    {
        _lambdaHandler = lambdaHandler;
        _sqsHandler = sqsHandler;
        _ddbHandler = ddbHandler;
    }

    internal void EnsureStarted()
    {
        _timer ??= new Timer(_ => Poll(), null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
    }

    internal void Stop()
    {
        _timer?.Dispose();
        _timer = null;
    }

    public void Dispose() => Stop();

    private void Poll()
    {
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0)
        {
            return; // Already running
        }

        try
        {
            PollOnce();
        }
        finally
        {
            Interlocked.Exchange(ref _running, 0);
        }
    }

    private void PollOnce()
    {
        var esmsByAccount = _lambdaHandler.GetEnabledEsmsByAccount();
        foreach (var (accountId, esms) in esmsByAccount)
        {
            // Set the account context so AccountScopedDictionary lookups
            // in the SQS and DynamoDB handlers resolve correctly.
            AccountContext.SetFromAccessKey(accountId);
            try
            {
                foreach (var esm in esms)
                {
                    try
                    {
                        var sourceArn = esm["EventSourceArn"]?.ToString() ?? "";
                        if (sourceArn.Contains(":sqs:", StringComparison.Ordinal))
                        {
                            PollSqs(esm, accountId);
                        }
                        else if (sourceArn.Contains(":dynamodb:", StringComparison.Ordinal)
                                 && sourceArn.Contains("/stream/", StringComparison.Ordinal))
                        {
                            PollDynamoDbStream(esm, accountId);
                        }
                    }
                    catch (Exception)
                    {
                        // Log but continue with other ESMs
                    }
                }
            }
            finally
            {
                AccountContext.Reset();
            }
        }
    }

    private void PollSqs(Dictionary<string, object?> esm, string accountId)
    {
        var sourceArn = esm["EventSourceArn"]?.ToString() ?? "";
        var batchSize = esm.TryGetValue("BatchSize", out var bs) && bs is int bsInt ? bsInt : 10;

        // Extract queue name and region from ARN: arn:aws:sqs:{region}:{account}:{queue-name}
        var parts = sourceArn.Split(':');
        if (parts.Length < 6)
        {
            return;
        }

        var region = parts[3];
        var queueName = parts[5];

        // Reconstruct the queue URL in the same format that SqsServiceHandler uses:
        // http://{host}:{port}/{accountId}/{queueName}
        var queueUrl = $"http://localhost:4566/{accountId}/{queueName}";

        var messages = _sqsHandler.ReceiveMessagesForEsm(queueUrl, batchSize);
        if (messages.Count == 0)
        {
            return;
        }

        // Build SQS event payload
        var records = new List<object?>();
        foreach (var m in messages)
        {
            var attributes = new Dictionary<string, string>
            {
                ["ApproximateReceiveCount"] = m.SystemAttributes.GetValueOrDefault("ApproximateReceiveCount", "1"),
                ["SentTimestamp"] = m.SystemAttributes.GetValueOrDefault("SentTimestamp", "0"),
                ["SenderId"] = m.SystemAttributes.GetValueOrDefault("SenderId", ""),
                ["ApproximateFirstReceiveTimestamp"] = m.SystemAttributes.GetValueOrDefault("ApproximateFirstReceiveTimestamp", "0"),
            };

            var record = new Dictionary<string, object?>
            {
                ["messageId"] = m.Id,
                ["receiptHandle"] = m.ReceiptHandle,
                ["body"] = m.Body,
                ["attributes"] = attributes,
                ["messageAttributes"] = new Dictionary<string, object>(),
                ["md5OfBody"] = m.Md5Body,
                ["eventSource"] = "aws:sqs",
                ["eventSourceARN"] = sourceArn,
                ["awsRegion"] = region,
            };
            records.Add(record);
        }

        var sqsEvent = new Dictionary<string, object?> { ["Records"] = records };

        // Resolve function ARN from ESM
        var funcArn = esm["FunctionArn"]?.ToString() ?? "";

        // Invoke Lambda
        var success = _lambdaHandler.InvokeForEsm(funcArn, sqsEvent);

        if (success)
        {
            // Delete consumed messages
            var handles = new List<string>();
            foreach (var m in messages)
            {
                if (m.ReceiptHandle is not null)
                {
                    handles.Add(m.ReceiptHandle);
                }
            }

            _sqsHandler.DeleteMessagesForEsm(queueUrl, handles);
            esm["LastProcessingResult"] = $"OK - {messages.Count} records";
        }
        else
        {
            esm["LastProcessingResult"] = "FAILED";
        }
    }

    private void PollDynamoDbStream(Dictionary<string, object?> esm, string accountId)
    {
        _ = accountId; // accountId already set via AccountContext before this call

        var sourceArn = esm["EventSourceArn"]?.ToString() ?? "";

        // Extract table name from stream ARN: arn:aws:dynamodb:{region}:{account}:table/{tableName}/stream/{date}
        var tableSegments = sourceArn.Split('/');
        if (tableSegments.Length < 2)
        {
            return;
        }

        var tableName = tableSegments[1]; // after "table/"
        var batchSize = esm.TryGetValue("BatchSize", out var bs) && bs is int bsInt ? bsInt : 100;

        var records = _ddbHandler.DrainStreamRecords(tableName, batchSize);
        if (records.Count == 0)
        {
            return;
        }

        // Build DynamoDB Streams event payload
        var ddbEvent = new Dictionary<string, object?>
        {
            ["Records"] = records.Select(r => (object?)r).ToList(),
        };

        var funcArn = esm["FunctionArn"]?.ToString() ?? "";
        var success = _lambdaHandler.InvokeForEsm(funcArn, ddbEvent);

        esm["LastProcessingResult"] = success ? $"OK - {records.Count} records" : "FAILED";
    }
}
