using System.Text.Json;
using System.Text.Json.Serialization;
using MicroStack.Services.SecretsManager;
using MicroStack.Services.Ssm;
using MicroStack.Services.Sns;
using MicroStack.Services.Sqs;

namespace MicroStack.Internal;

/// <summary>
/// Source-generated JSON serializer context for Native AOT compatibility.
/// Registers all types used in JsonSerializer.Serialize/Deserialize calls throughout MicroStack.
/// </summary>
[JsonSerializable(typeof(Dictionary<string, object?>))]
[JsonSerializable(typeof(Dictionary<string, string>))]
[JsonSerializable(typeof(Dictionary<string, string[]>))]
[JsonSerializable(typeof(Dictionary<string, JsonElement>))]
[JsonSerializable(typeof(Dictionary<string, bool>))]
[JsonSerializable(typeof(List<object?>))]
[JsonSerializable(typeof(List<string>))]
[JsonSerializable(typeof(List<Dictionary<string, string>>))]
[JsonSerializable(typeof(List<Dictionary<string, object?>>))]
// Program.cs endpoint response types
[JsonSerializable(typeof(HealthResponse))]
[JsonSerializable(typeof(ResetResponse))]
[JsonSerializable(typeof(ConfigResponse))]
// AwsResponseHelpers error type
[JsonSerializable(typeof(AwsJsonError))]
// SecretsManager persistence
[JsonSerializable(typeof(SecretsManagerState))]
[JsonSerializable(typeof(List<SmSecretsEntry>))]
[JsonSerializable(typeof(List<SmPolicyEntry>))]
[JsonSerializable(typeof(SmSecret))]
[JsonSerializable(typeof(SmSecretVersion))]
[JsonSerializable(typeof(SmTag))]
[JsonSerializable(typeof(SmRotationRules))]
[JsonSerializable(typeof(SmReplicationStatus))]
// SSM persistence
[JsonSerializable(typeof(SsmState))]
[JsonSerializable(typeof(List<SsmParameterEntry>))]
[JsonSerializable(typeof(List<SsmHistoryEntry2>))]
[JsonSerializable(typeof(List<SsmTagEntry>))]
[JsonSerializable(typeof(SsmParameter))]
[JsonSerializable(typeof(SsmHistoryEntry))]
[JsonSerializable(typeof(List<SsmHistoryEntry>))]
// SNS persistence
[JsonSerializable(typeof(SnsState))]
[JsonSerializable(typeof(List<SnsTopicEntry>))]
[JsonSerializable(typeof(List<SnsSubEntry>))]
[JsonSerializable(typeof(List<SnsAppEntry>))]
[JsonSerializable(typeof(List<SnsEndpointEntry>))]
[JsonSerializable(typeof(SnsTopic))]
[JsonSerializable(typeof(SnsSubscription))]
[JsonSerializable(typeof(SnsMessage))]
[JsonSerializable(typeof(SnsMessageAttribute))]
[JsonSerializable(typeof(SnsPlatformApp))]
[JsonSerializable(typeof(SnsPlatformEndpoint))]
// SQS persistence
[JsonSerializable(typeof(SqsState))]
[JsonSerializable(typeof(List<SqsQueueEntry>))]
[JsonSerializable(typeof(List<SqsNameEntry>))]
[JsonSerializable(typeof(SqsQueue))]
[JsonSerializable(typeof(SqsMessage))]
[JsonSerializable(typeof(SqsDedupEntry))]
[JsonSourceGenerationOptions(
    GenerationMode = JsonSourceGenerationMode.Default,
    PropertyNamingPolicy = JsonKnownNamingPolicy.Unspecified,
    WriteIndented = false)]
internal sealed partial class MicroStackJsonContext : JsonSerializerContext
{
}
