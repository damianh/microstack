using System.Text.Json;
using MicroStack.Admin.Contracts;
using Microsoft.AspNetCore.Http.Features;

namespace MicroStack.Internal.Admin;

internal static class AdminEvents
{
    internal static async Task Stream(HttpContext context, AdminChangeHub hub,
        IHostApplicationLifetime lifetime)
    {
        string? account = null;
        if (context.Request.Query.TryGetValue("accountId", out var values))
        {
            account = values.ToString();
            if (values.Count != 1 || account.Length != 12 || account.Any(c => c is < '0' or > '9'))
            {
                context.Response.StatusCode = 400;
                await context.Response.WriteAsJsonAsync(
                    new AdminError("invalid_account", "Account IDs must contain exactly 12 digits."),
                    AdminJsonContext.Default.AdminError, cancellationToken: context.RequestAborted);
                return;
            }
        }

        using var subscription = hub.Subscribe(account);
        if (subscription is null)
        {
            context.Response.StatusCode = 503;
            context.Response.Headers.RetryAfter = "5";
            await context.Response.WriteAsJsonAsync(
                new AdminError("events_capacity", "Live updates are at capacity. Retry shortly."),
                AdminJsonContext.Default.AdminError, cancellationToken: context.RequestAborted);
            return;
        }

        context.Response.ContentType = "text/event-stream";
        context.Response.Headers.CacheControl = "no-store";
        context.Response.Headers["X-Accel-Buffering"] = "no";
        context.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();
        using var stopped = CancellationTokenSource.CreateLinkedTokenSource(
            context.RequestAborted, lifetime.ApplicationStopping);
        try
        {
            await WriteEvent(subscription.Take(resync: true));
            while (!stopped.IsCancellationRequested)
            {
                using var heartbeat = CancellationTokenSource.CreateLinkedTokenSource(stopped.Token);
                heartbeat.CancelAfter(TimeSpan.FromSeconds(15));
                bool ready;
                try
                {
                    ready = await subscription.Wakeup.Reader.WaitToReadAsync(heartbeat.Token);
                }
                catch (OperationCanceledException) when (!stopped.IsCancellationRequested)
                {
                    await Write(": heartbeat\n\n");
                    continue;
                }
                if (!ready)
                    break;
                await Task.Delay(TimeSpan.FromMilliseconds(500), stopped.Token);
                await WriteEvent(subscription.Take());
            }
        }
        catch (OperationCanceledException)
        {
            context.Abort();
        }
        catch (IOException)
        {
            context.Abort();
        }

        Task WriteEvent(AdminChangeEvent change) => Write(
            $"id: {change.Epoch}:{change.Sequence}\nevent: change\ndata: {JsonSerializer.Serialize(change, AdminJsonContext.Default.AdminChangeEvent)}\n\n");

        async Task Write(string text)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stopped.Token);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));
            await context.Response.WriteAsync(text, timeout.Token);
            await context.Response.Body.FlushAsync(timeout.Token);
        }
    }
}
