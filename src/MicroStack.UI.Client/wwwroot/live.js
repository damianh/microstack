export function create(receiver) {
    let source = null;
    let disposed = false;
    const notify = (method, ...args) => {
        if (!disposed) receiver.invokeMethodAsync(method, ...args).catch(() => {});
    };
    const stop = () => {
        if (source) {
            source.onopen = source.onerror = null;
            source.close();
            source = null;
        }
    };
    const visibility = () => {
        if (document.hidden) stop();
        notify("OnVisibilityChanged", !document.hidden);
    };
    document.addEventListener("visibilitychange", visibility);
    return {
        isVisible: () => !document.hidden,
        start(accountId, generation) {
            stop();
            if (disposed || document.hidden) return;
            const url = new URL("/_microstack/admin/v1/events", window.location.origin);
            if (accountId) url.searchParams.set("accountId", accountId);
            const current = new EventSource(url);
            source = current;
            current.onopen = () => {
                if (source === current) notify("OnConnectionChanged", generation, true);
            };
            current.onerror = () => {
                if (source === current) notify("OnConnectionChanged", generation, false);
            };
            current.addEventListener("change", event => {
                if (source === current) notify("OnChange", generation, event.data);
            });
        },
        stop,
        dispose() {
            disposed = true;
            stop();
            document.removeEventListener("visibilitychange", visibility);
        }
    };
}
