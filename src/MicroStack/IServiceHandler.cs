namespace MicroStack;

/// <summary>
/// Implemented by each AWS service handler (S3, SQS, DynamoDB, etc.).
/// </summary>
public interface IServiceHandler
{
    /// <summary>Canonical service name, e.g. "s3", "sqs", "dynamodb".</summary>
    string ServiceName { get; }

    /// <summary>Handle an incoming AWS request and return the response.</summary>
    Task<ServiceResponse> HandleAsync(ServiceRequest request);

    /// <summary>Reset all in-memory state (called by /_ministack/reset).</summary>
    void Reset();

    /// <summary>Serialize current state for persistence.</summary>
    object? GetState();

    /// <summary>Restore state from a previously persisted snapshot.</summary>
    void RestoreState(object state);
}
