using Amazon.Lambda.Core;

namespace MicroStack.LambdaBootstrap;

/// <summary>
/// Minimal implementation of <see cref="ILambdaContext"/> for use in MicroStack's .NET Lambda worker.
/// </summary>
internal sealed class LambdaContext : ILambdaContext
{
    private readonly string _functionName;
    private readonly int _memoryLimitInMB;
    private readonly string _invokedFunctionArn;
    private readonly string _awsRequestId;

    internal LambdaContext(string functionName, int memoryLimitInMB, string invokedFunctionArn, string awsRequestId)
    {
        _functionName = functionName;
        _memoryLimitInMB = memoryLimitInMB;
        _invokedFunctionArn = invokedFunctionArn;
        _awsRequestId = awsRequestId;
    }

    public string FunctionName => _functionName;
    public string FunctionVersion => "$LATEST";
    public string InvokedFunctionArn => _invokedFunctionArn;
    public int MemoryLimitInMB => _memoryLimitInMB;
    public string AwsRequestId => _awsRequestId;
    public string LogGroupName => $"/aws/lambda/{_functionName}";
    public string LogStreamName => "test-stream";
    public ILambdaLogger Logger => LambdaStderrLogger.Instance;
    public IClientContext? ClientContext => null;
    public ICognitoIdentity? Identity => null;
    public TimeSpan RemainingTime => TimeSpan.FromMinutes(5);
}

/// <summary>Logger that writes to stderr (captured by MicroStack as function logs).</summary>
internal sealed class LambdaStderrLogger : ILambdaLogger
{
    internal static readonly LambdaStderrLogger Instance = new();

    private LambdaStderrLogger()
    {
    }

    public void Log(string message) => Console.Error.Write(message);
    public void LogLine(string message) => Console.Error.WriteLine(message);
}
