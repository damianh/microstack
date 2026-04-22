using System.Web;

namespace MicroStack.Internal;

/// <summary>
/// Core ASP.NET Core middleware that:
///   1. Reads the raw HTTP request
///   2. Decodes AWS chunked transfer encoding (STREAMING-*)
///   3. Sets the per-request account ID from the Authorization header
///   4. Routes to the correct IServiceHandler via AwsServiceRouter
///   5. Writes the ServiceResponse back
///
/// Also handles special pre-router paths (virtual-hosted S3, execute-api,
/// ALB data plane, Cognito well-known, RDS Data API, SES v2, Lambda layers).
/// </summary>
internal sealed class AwsRequestMiddleware
{
    private readonly RequestDelegate _next;
    private readonly AwsServiceRouter _router;
    private readonly ServiceRegistry _registry;
    private readonly ILogger<AwsRequestMiddleware> _logger;

    private static readonly string _host =
        MicroStackOptions.Instance.Host;

    // Matches {apiId}.execute-api.<host>[:<port>]
    private static readonly System.Text.RegularExpressions.Regex _executeApiRe =
        new(@$"^([a-f0-9]{{8}})\.execute-api\.{System.Text.RegularExpressions.Regex.Escape(_host)}(:\d+)?$",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

    // Matches virtual-hosted S3: {bucket}[.s3].<host>[:<port>]
    // Excludes execute-api, alb, emr, efs, elasticache, s3-control subdomains
    private static readonly System.Text.RegularExpressions.Regex _s3VhostRe =
        new(@$"^([^.]+)(\.s3)?\.{System.Text.RegularExpressions.Regex.Escape(_host)}(:\d+)?$",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

    private static readonly System.Text.RegularExpressions.Regex _s3VhostExcludeRe =
        new(@"\.(execute-api|alb|emr|efs|elasticache|s3-control)\.",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

    // Must be public: ASP.NET Core UseMiddleware<T> convention requires a public constructor.
    public AwsRequestMiddleware(
        RequestDelegate next,
        AwsServiceRouter router,
        ServiceRegistry registry,
        ILogger<AwsRequestMiddleware> logger)
    {
        _next    = next;
        _router  = router;
        _registry = registry;
        _logger  = logger;
    }

    // Must be public: ASP.NET Core UseMiddleware<T> convention requires a public Invoke/InvokeAsync method.
    public async Task InvokeAsync(HttpContext context)
    {
        // If an endpoint has already been matched (e.g. admin routes registered via MapGet/MapPost),
        // skip AWS dispatch and let the endpoint middleware handle it.
        if (context.GetEndpoint() is not null)
        {
            await _next(context);
            return;
        }

        var req    = context.Request;
        var method = req.Method.ToUpperInvariant();
        var path   = req.Path.Value ?? "/";

        // ── Build headers dict ─────────────────────────────────────────────
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, values) in req.Headers)
            headers[key] = values.ToString();

        // ── Build query params dict ────────────────────────────────────────
        var queryParams = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, values) in req.Query)
            queryParams[key] = values.ToArray()!;

        // ── Read body ──────────────────────────────────────────────────────
        byte[] body;
        using (var ms = new MemoryStream())
        {
            await req.Body.CopyToAsync(ms);
            body = ms.ToArray();
        }

        // ── Decode AWS chunked transfer encoding ───────────────────────────
        var sha256Header     = headers.GetValueOrDefault("x-amz-content-sha256", "");
        var contentEncoding  = headers.GetValueOrDefault("content-encoding", "");
        var hasDecodedLength = headers.ContainsKey("x-amz-decoded-content-length");

        if (sha256Header.StartsWith("STREAMING-", StringComparison.OrdinalIgnoreCase)
            || contentEncoding.Contains("aws-chunked", StringComparison.OrdinalIgnoreCase)
            || hasDecodedLength)
        {
            body = DecodeAwsChunked(body);
            if (contentEncoding.Contains("aws-chunked", StringComparison.OrdinalIgnoreCase))
            {
                var parts = contentEncoding.Split(',')
                    .Select(p => p.Trim())
                    .Where(p => !string.Equals(p, "aws-chunked", StringComparison.OrdinalIgnoreCase))
                    .ToArray();
                if (parts.Length > 0)
                    headers["content-encoding"] = string.Join(", ", parts);
                else
                    headers.Remove("content-encoding");
            }
        }

        // ── Set per-request account ID ─────────────────────────────────────
        var accessKey = AwsServiceRouter.ExtractAccessKeyId(
            new ServiceRequest(method, path, headers, body, queryParams));
        AccountContext.SetFromAccessKey(accessKey);

        var serviceRequest = new ServiceRequest(method, path, headers, body, queryParams);

        // ── CORS pre-flight ────────────────────────────────────────────────
        if (method == "OPTIONS")
        {
            WriteCorsPreflight(context.Response);
            return;
        }

        // ── Pre-router special paths ───────────────────────────────────────
        var host = headers.GetValueOrDefault("host", "");

        // Execute-API data plane: {apiId}.execute-api.<host>
        if (_executeApiRe.IsMatch(host))
        {
            await DispatchToService(context, serviceRequest, "apigateway");
            return;
        }

        // ALB data plane via path prefix
        if (path.StartsWith("/_alb/", StringComparison.OrdinalIgnoreCase))
        {
            await DispatchToService(context, serviceRequest, "elasticloadbalancing");
            return;
        }

        // ALB data plane via host header {lb}.alb.<host>
        if (host.Contains(".alb.", StringComparison.OrdinalIgnoreCase))
        {
            await DispatchToService(context, serviceRequest, "elasticloadbalancing");
            return;
        }

        // Virtual-hosted S3: {bucket}[.s3].<host>
        if (!_s3VhostExcludeRe.IsMatch(host) && _s3VhostRe.IsMatch(host))
        {
            await DispatchToService(context, serviceRequest, "s3");
            return;
        }

        // Cognito well-known endpoints
        if (path.Contains("/.well-known/", StringComparison.OrdinalIgnoreCase) && method == "GET")
        {
            await DispatchToService(context, serviceRequest, "cognito-idp");
            return;
        }

        // Lambda layer content download
        if (path.StartsWith("/_microstack/lambda-layers/", StringComparison.OrdinalIgnoreCase) && method == "GET")
        {
            await DispatchToService(context, serviceRequest, "lambda");
            return;
        }

        // RDS Data API REST paths
        if (path.Equals("/Execute", StringComparison.OrdinalIgnoreCase)
            || path.Equals("/BeginTransaction", StringComparison.OrdinalIgnoreCase)
            || path.Equals("/CommitTransaction", StringComparison.OrdinalIgnoreCase)
            || path.Equals("/RollbackTransaction", StringComparison.OrdinalIgnoreCase)
            || path.Equals("/BatchExecute", StringComparison.OrdinalIgnoreCase)
            || path.StartsWith("/Execute/", StringComparison.OrdinalIgnoreCase))
        {
            await DispatchToService(context, serviceRequest, "rds-data");
            return;
        }

        // SES v2 REST paths
        if (path.StartsWith("/v2/email/", StringComparison.OrdinalIgnoreCase))
        {
            await DispatchToService(context, serviceRequest, "ses");
            return;
        }

        // S3 Control API
        if (path.StartsWith("/v20180820/", StringComparison.OrdinalIgnoreCase))
        {
            await DispatchToService(context, serviceRequest, "s3");
            return;
        }

        // ── Main service routing ───────────────────────────────────────────
        // For unsigned form-encoded requests (e.g. STS AssumeRoleWithWebIdentity),
        // Action is in the body not the query string — merge it in for routing only.
        var routingRequest = serviceRequest;
        if (!queryParams.ContainsKey("Action")
            && (headers.GetValueOrDefault("content-type", "")
                .StartsWith("application/x-www-form-urlencoded", StringComparison.OrdinalIgnoreCase))
            && body.Length > 0)
        {
            var bodyStr = System.Text.Encoding.UTF8.GetString(body);
            var bodyAction = HttpUtility.ParseQueryString(bodyStr)["Action"];
            if (!string.IsNullOrEmpty(bodyAction))
            {
                var routingParams = new Dictionary<string, string[]>(queryParams, StringComparer.OrdinalIgnoreCase)
                {
                    ["Action"] = [bodyAction],
                };
                routingRequest = new ServiceRequest(method, path, headers, body, routingParams);
            }
        }

        var serviceName = _router.DetectService(routingRequest);
        await DispatchToService(context, serviceRequest, serviceName);
    }

    private async Task DispatchToService(HttpContext context, ServiceRequest request, string serviceName)
    {
        var handler = _registry.Resolve(serviceName);

        ServiceResponse response;
        if (handler is null)
        {
            _logger.LogDebug("No handler for service '{Service}'", serviceName);
            response = ServiceResponse.Empty(400);
        }
        else
        {
            try
            {
                response = await handler.HandleAsync(request);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling request for service '{Service}'", serviceName);
                response = ServiceResponse.Empty(500);
            }
        }

        await WriteResponse(context.Response, response);
    }

    private static async Task WriteResponse(HttpResponse httpResponse, ServiceResponse response)
    {
        AddCorsHeaders(httpResponse);

        httpResponse.StatusCode = response.StatusCode;

        foreach (var (key, value) in response.Headers)
            httpResponse.Headers[key] = value;

        if (response.Body.Length > 0)
            await httpResponse.Body.WriteAsync(response.Body);
    }

    private static void AddCorsHeaders(HttpResponse response)
    {
        response.Headers["Access-Control-Allow-Origin"]  = "*";
        response.Headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, HEAD, OPTIONS, PATCH";
        response.Headers["Access-Control-Allow-Headers"] = "*";
        response.Headers["Access-Control-Expose-Headers"] = "*";
    }

    private static void WriteCorsPreflight(HttpResponse response)
    {
        AddCorsHeaders(response);
        response.StatusCode = 204;
    }

    /// <summary>
    /// Decode AWS chunked encoding.
    /// Format: &lt;hex-size&gt;[;chunk-signature=...]\r\n&lt;data&gt;\r\n ... 0\r\n
    /// </summary>
    private static byte[] DecodeAwsChunked(byte[] input)
    {
        var decoded = new List<byte>(input.Length);
        var remaining = input.AsSpan();

        while (remaining.Length > 0)
        {
            var crlf = remaining.IndexOf((byte)'\r');
            if (crlf < 0 || crlf + 1 >= remaining.Length || remaining[crlf + 1] != '\n')
                break;

            var headerSpan = remaining[..crlf];
            var headerStr  = System.Text.Encoding.ASCII.GetString(headerSpan);
            var sizeHex    = headerStr.Split(';')[0].Trim();

            if (!int.TryParse(sizeHex, System.Globalization.NumberStyles.HexNumber, null, out var chunkSize))
                break;

            if (chunkSize == 0)
                break;

            var dataStart = crlf + 2;
            if (dataStart + chunkSize > remaining.Length)
                break;

            decoded.AddRange(remaining.Slice(dataStart, chunkSize).ToArray());
            remaining = remaining[(dataStart + chunkSize + 2)..]; // skip trailing \r\n
        }

        return decoded.Count > 0 || input.Length == 0 ? decoded.ToArray() : input;
    }
}
