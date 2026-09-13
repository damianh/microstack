namespace MicroStack.Internal;

/// <summary>
/// Enumerates owners of retained resources, independently of the current request.
/// Implementations must not create defaults, inspect payloads, or infer ownership
/// for instance-global state. Secondary indexes and empty containers are not resources.
/// </summary>
internal interface IKnownAccountSource
{
    IEnumerable<string> GetKnownAccountIds() => [];
}
