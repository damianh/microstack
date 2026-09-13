using Bunit;
using MicroStack.UI.Client.Components;
using Xunit;

namespace MicroStack.UI.Tests;

public sealed class AccountScopeTests
{
    [Fact]
    public async Task Picker_accepts_known_accounts_only_and_reports_stale_selections()
    {
        using var context = new BunitContext();
        var selections = new List<string>();
        var view = context.Render<AccountScope>(parameters => parameters
            .Add(component => component.Account, "000000000000")
            .Add(component => component.DefaultAccount, "000000000000")
            .Add(component => component.Accounts, ["000000000000", "111111111111"])
            .Add(component => component.AccountChanged, value => selections.Add(value)));

        Assert.Equal(2, view.FindAll("option").Count);
        Assert.Equal("000000000000 (default)", view.Find("option").TextContent);
        Assert.Empty(view.FindAll("input,button"));
        await view.Find("select").ChangeAsync(new() { Value = "999999999999" });
        Assert.Empty(selections);
        Assert.Contains("no longer available", view.Find("[role=alert]").TextContent);
        await view.Find("select").ChangeAsync(new() { Value = "111111111111" });
        Assert.Equal(["111111111111"], selections);
        Assert.Empty(view.FindAll("[role=alert]"));
    }

    [Fact]
    public void One_known_account_is_read_only_but_unknown_current_account_can_recover()
    {
        using var context = new BunitContext();
        var view = context.Render<AccountScope>(parameters => parameters
            .Add(component => component.Account, "000000000000")
            .Add(component => component.DefaultAccount, "000000000000")
            .Add(component => component.Accounts, ["000000000000"]));

        Assert.Equal("000000000000", view.Find("code").TextContent);
        Assert.Empty(view.FindAll("select,input,button"));
        view.Render(parameters => parameters.Add(component => component.Account, "999999999999"));
        Assert.Single(view.FindAll("select"));
        Assert.Equal("Select a known account", view.Find("option[disabled]").TextContent);
        Assert.Single(view.FindAll("option:not([disabled])"));
        Assert.DoesNotContain("999999999999", view.Markup);
    }
}
