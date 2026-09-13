using Microsoft.Net.Http.Headers;

namespace MicroStack.Internal;

internal static class AdminUiHosting
{
    internal const string PathPrefix = "/ui";

    internal static bool IsGatewayHost(HttpRequest request, MicroStackOptions options)
    {
        var host = request.Host.Host;
        return host.Equals(options.Host, StringComparison.OrdinalIgnoreCase)
            || host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || host.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
            || host.Equals("::1", StringComparison.OrdinalIgnoreCase);
    }

    internal static bool IsUiRequest(HttpContext context, MicroStackOptions options) =>
        IsGatewayHost(context.Request, options)
        && context.Request.Path.StartsWithSegments(PathPrefix);

    internal static bool ShouldRedirectRoot(HttpRequest request, MicroStackOptions options)
    {
        if (!HttpMethods.IsGet(request.Method)
            || request.Path != "/"
            || request.QueryString.HasValue
            || !IsGatewayHost(request, options)
            || request.ContentLength is > 0
            || request.Headers.ContainsKey(HeaderNames.TransferEncoding)
            || request.Headers.ContainsKey(HeaderNames.Authorization)
            || request.Headers.ContainsKey("X-Amz-Target")
            || request.Headers.Keys.Any(key => key.StartsWith("X-Amz-", StringComparison.OrdinalIgnoreCase)))
        {
            return false;
        }

        var acceptsHtml = request.GetTypedHeaders().Accept?.Any(value =>
            value.MediaType.Equals("text/html", StringComparison.OrdinalIgnoreCase)
            && value.Quality.GetValueOrDefault(1) > 0) == true;
        if (!acceptsHtml)
            return false;

        var fetchMode = request.Headers["Sec-Fetch-Mode"].ToString();
        var fetchDestination = request.Headers["Sec-Fetch-Dest"].ToString();
        return (fetchMode.Length == 0 || fetchMode.Equals("navigate", StringComparison.OrdinalIgnoreCase))
            && (fetchDestination.Length == 0 || fetchDestination.Equals("document", StringComparison.OrdinalIgnoreCase));
    }

    internal static bool Map(WebApplication app, MicroStackOptions options)
    {
        if (!app.Environment.WebRootFileProvider.GetFileInfo("ui/index.html").Exists)
            return false;

        app.Use(async (context, next) =>
        {
            if (ShouldRedirectRoot(context.Request, options))
            {
                context.Response.StatusCode = StatusCodes.Status302Found;
                context.Response.Headers.Location = "/ui/";
                context.Response.Headers.CacheControl = "no-store";
                context.Response.Headers.Vary = $"{HeaderNames.Accept}, Sec-Fetch-Mode, Sec-Fetch-Dest";
                return;
            }

            await next(context);
        });

        const string originalPathKey = "MicroStack.AdminUi.OriginalPath";
        app.Use(async (context, next) =>
        {
            // Static-file middleware has no host predicate; hide service-host paths until AWS routing.
            if (context.Request.Path.StartsWithSegments(PathPrefix)
                && !IsGatewayHost(context.Request, options))
            {
                context.Items[originalPathKey] = context.Request.Path;
                context.Request.Path = "/__microstack_aws_passthrough" + context.Request.Path;
            }

            await next(context);
        });

        app.UseBlazorFrameworkFiles(PathPrefix);
        app.UseStaticFiles();

        app.Use(async (context, next) =>
        {
            if (context.Items.Remove(originalPathKey, out var originalPath))
                context.Request.Path = (PathString)originalPath!;

            if (IsUiRequest(context, options))
            {
                if (context.Request.Path == PathPrefix)
                {
                    context.Response.StatusCode = StatusCodes.Status308PermanentRedirect;
                    context.Response.Headers.Location = PathPrefix + "/" + context.Request.QueryString;
                    return;
                }

                if (!HttpMethods.IsGet(context.Request.Method) && !HttpMethods.IsHead(context.Request.Method))
                {
                    context.Response.StatusCode = StatusCodes.Status405MethodNotAllowed;
                    context.Response.Headers.Allow = "GET, HEAD";
                    return;
                }

                if (Path.HasExtension(context.Request.Path.Value))
                {
                    context.Response.StatusCode = StatusCodes.Status404NotFound;
                    return;
                }

                await ServeIndex(context);
                return;
            }

            await next(context);
        });

        return true;
    }

    private static async Task ServeIndex(HttpContext context)
    {
        var environment = context.RequestServices.GetRequiredService<IWebHostEnvironment>();
        var index = environment.WebRootFileProvider.GetFileInfo("ui/index.html");
        if (!index.Exists)
        {
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            await context.Response.WriteAsync("The MicroStack UI assets are unavailable.", context.RequestAborted);
            return;
        }

        context.Response.ContentType = "text/html; charset=utf-8";
        context.Response.Headers.CacheControl = "no-cache";
        await context.Response.SendFileAsync(index, context.RequestAborted);
    }
}
