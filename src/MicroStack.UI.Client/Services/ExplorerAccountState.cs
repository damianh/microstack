using Microsoft.JSInterop;

namespace MicroStack.UI.Client.Services;

public sealed class ExplorerAccountState(IJSRuntime js)
{
    internal const string StorageKey = "microstack.explorer.account";
    private Task? _restore;
    public string? Account { get; private set; }
    public string? PersistenceError { get; private set; }
    public event Action? Changed;

    public Task RestoreAsync() => _restore ??= RestoreCoreAsync();

    private async Task RestoreCoreAsync()
    {
        try
        {
            var saved = await js.InvokeAsync<string?>("sessionStorage.getItem", StorageKey);
            // An explicit route selected while storage was loading takes precedence.
            if (Account is not null || saved is null) return;
            if (ExplorerLocation.ValidAccount(saved)) Account = saved;
            else PersistenceError = "The saved explorer account is invalid. Choose a known account in Services.";
        }
        catch (JSException)
        {
            PersistenceError = "The saved explorer account could not be restored. Choose an account in Services.";
        }
        Changed?.Invoke();
    }

    public async Task RememberAsync(string account)
    {
        if (!ExplorerLocation.ValidAccount(account))
            throw new ArgumentException("Account ID must contain exactly 12 ASCII digits.", nameof(account));
        if (Account == account) return;
        Account = account;
        Changed?.Invoke();
        try
        {
            await js.InvokeVoidAsync("sessionStorage.setItem", StorageKey, account);
            PersistenceError = null;
        }
        catch (JSException)
        {
            PersistenceError = "Account selection cannot be saved in this tab. It will be remembered only until the page is reloaded.";
        }
        Changed?.Invoke();
    }
}
