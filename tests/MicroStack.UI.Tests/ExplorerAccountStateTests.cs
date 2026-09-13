using Bunit;
using MicroStack.UI.Client.Services;
using Xunit;

namespace MicroStack.UI.Tests;

public sealed class ExplorerAccountStateTests
{
    [Fact]
    public async Task Late_storage_restore_does_not_overwrite_explicit_selection()
    {
        using var context = new BunitContext();
        context.JSInterop.Mode = JSRuntimeMode.Loose;
        var pending = context.JSInterop.Setup<string?>("sessionStorage.getItem", ExplorerAccountState.StorageKey);
        var state = new ExplorerAccountState(context.JSInterop.JSRuntime);
        var restore = state.RestoreAsync();
        await state.RememberAsync("111111111111");
        pending.SetResult("000000000000");
        await restore;
        Assert.Equal("111111111111", state.Account);
        context.JSInterop.VerifyInvoke("sessionStorage.setItem");
    }

    [Theory]
    [InlineData("not-an-account")]
    [InlineData("１２３４５６７８９０１２")]
    public async Task Invalid_saved_context_is_reported_and_never_used_as_account(string saved)
    {
        using var context = new BunitContext();
        context.JSInterop.Setup<string?>("sessionStorage.getItem", ExplorerAccountState.StorageKey).SetResult(saved);
        var state = new ExplorerAccountState(context.JSInterop.JSRuntime);
        await state.RestoreAsync();
        Assert.Null(state.Account);
        Assert.Contains("invalid", state.PersistenceError, StringComparison.Ordinal);
        await Assert.ThrowsAsync<ArgumentException>(() => state.RememberAsync(saved));
    }

    [Fact]
    public async Task Restore_is_once_per_tab_state_and_does_not_write_storage()
    {
        using var context = new BunitContext();
        context.JSInterop.Setup<string?>("sessionStorage.getItem", ExplorerAccountState.StorageKey).SetResult("111111111111");
        var state = new ExplorerAccountState(context.JSInterop.JSRuntime);
        await Task.WhenAll(state.RestoreAsync(), state.RestoreAsync());
        Assert.Equal("111111111111", state.Account);
        context.JSInterop.VerifyInvoke("sessionStorage.getItem", 1);
        Assert.DoesNotContain(context.JSInterop.Invocations, call => call.Identifier == "sessionStorage.setItem");
    }
}
