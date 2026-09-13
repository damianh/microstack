using Bunit;
using MicroStack.UI.Client.Services;
using Microsoft.Extensions.DependencyInjection;

namespace MicroStack.UI.Tests;

internal static class LiveTestServices
{
    internal static LiveUpdateCoordinator AddPaused(BunitContext context)
    {
        var live = new LiveUpdateCoordinator(context.JSInterop.JSRuntime);
        live.PauseAsync().GetAwaiter().GetResult();
        context.Services.AddSingleton(live);
        return live;
    }
}
