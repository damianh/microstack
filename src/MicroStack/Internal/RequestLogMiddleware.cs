using System.Diagnostics;

namespace MicroStack.Internal;

internal sealed class RequestLogMiddleware
{
    private readonly RequestDelegate _next;
    private readonly AwsServiceRouter _router;
    private readonly RequestLog _requestLog;

    public RequestLogMiddleware(RequestDelegate next, AwsServiceRouter router, RequestLog requestLog)
    {
        _next = next;
        _router = router;
        _requestLog = requestLog;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        if (!ShouldLog(context.Request))
        {
            await _next(context);
            return;
        }

        var timestamp = DateTimeOffset.UtcNow;
        var sw = Stopwatch.StartNew();
        await _next(context);
        sw.Stop();

        var request = context.Request;
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, values) in request.Headers)
            headers[key] = values.ToString();

        var queryParams = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, values) in request.Query)
            queryParams[key] = values.Select(v => v ?? string.Empty).ToArray();

        var service = _router.DetectService(new ServiceRequest(
            request.Method.ToUpperInvariant(),
            request.Path.Value ?? "/",
            headers,
            [],
            queryParams));

        _requestLog.Add(new RequestLogEntry(
            service,
            ExtractAction(request),
            AccountContext.GetAccountId(),
            timestamp,
            context.Response.StatusCode,
            sw.ElapsedMilliseconds));
    }

    private static bool ShouldLog(HttpRequest request)
    {
        var path = request.Path.Value ?? "/";
        if (request.Method.Equals("OPTIONS", StringComparison.OrdinalIgnoreCase))
            return false;

        return !path.StartsWith("/_microstack/", StringComparison.OrdinalIgnoreCase)
            && !path.Equals("/health", StringComparison.OrdinalIgnoreCase)
            && !path.Equals("/_localstack/health", StringComparison.OrdinalIgnoreCase);
    }

    private static string ExtractAction(HttpRequest request)
    {
        var target = request.Headers["x-amz-target"].ToString();
        if (!string.IsNullOrEmpty(target))
        {
            var dot = target.LastIndexOf('.');
            return dot >= 0 && dot < target.Length - 1
                ? target[(dot + 1)..]
                : target;
        }

        var action = request.Query["Action"].ToString();
        return string.IsNullOrEmpty(action) ? "-" : action;
    }
}
