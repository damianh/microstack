using Atoll.Components;
using Atoll.Slots;
using SplashAddonLayout = Atoll.Lagoon.Layouts.SplashLayout;

namespace Docs.Layouts;

/// <summary>
/// Site-specific wrapper layout for the landing page.
/// </summary>
public sealed class SplashSiteLayout : AtollComponent
{
    [Parameter]
    public string PageTitle { get; set; } = "";

    [Parameter]
    public string? PageDescription { get; set; }

    protected override async Task RenderCoreAsync(RenderContext context)
    {
        var config = DocsSetup.Config;
        var pageSlot = context.Slots.GetSlotFragment(SlotCollection.DefaultSlotName);

        var addonProps = new Dictionary<string, object?>
        {
            ["Config"] = config,
            ["PageTitle"] = PageTitle,
            ["PageDescription"] = PageDescription,
        };

        var addonSlots = SlotCollection.FromDefault(pageSlot);
        await RenderAsync(ComponentRenderer.ToFragment<SplashAddonLayout>(addonProps, addonSlots));
    }
}
