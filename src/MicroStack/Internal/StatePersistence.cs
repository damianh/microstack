using System.Text.Json;

namespace MicroStack.Internal;

/// <summary>
/// Optional JSON persistence for service state.
/// When PERSIST_STATE=1, saves each service's state to STATE_DIR on shutdown
/// and restores it on startup.
///
/// Port of ministack/core/persistence.py.
/// </summary>
internal sealed class StatePersistence
{
    private readonly bool _enabled;
    private readonly string _stateDir;
    private readonly ILogger<StatePersistence> _logger;
    private readonly ServiceRegistry _registry;

    internal StatePersistence(ILogger<StatePersistence> logger, ServiceRegistry registry, MicroStackOptions options)
    {
        _logger   = logger;
        _registry = registry;
        _enabled  = options.PersistState;
        _stateDir = options.StateDir;
    }

    internal bool IsEnabled => _enabled;

    /// <summary>Save all service states to disk.</summary>
    internal void SaveAll()
    {
        if (!_enabled) return;

        Directory.CreateDirectory(_stateDir);

        foreach (var handler in _registry.All)
        {
            try
            {
                var state = handler.GetState();
                if (state is null) continue;
                SaveState(handler.ServiceName, state);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Persistence: error getting state for {Service}", handler.ServiceName);
            }
        }
    }

    /// <summary>Restore all service states from disk.</summary>
    internal void RestoreAll()
    {
        if (!_enabled) return;

        foreach (var handler in _registry.All)
        {
            try
            {
                var data = LoadState(handler.ServiceName);
                if (data is null) continue;
                handler.RestoreState(data);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Persistence: error restoring state for {Service}", handler.ServiceName);
            }
        }
    }

    /// <summary>Delete all persisted state files (called by reset endpoint).</summary>
    internal void DeleteAll()
    {
        if (!_enabled) return;
        if (!Directory.Exists(_stateDir)) return;

        foreach (var file in Directory.GetFiles(_stateDir, "*.json"))
        {
            try { File.Delete(file); }
            catch (IOException) { /* best-effort */ }
        }
    }

    private void SaveState(string serviceName, object state)
    {
        try
        {
            Directory.CreateDirectory(_stateDir);
            var path = Path.Combine(_stateDir, $"{serviceName}.json");
            var tmp  = path + ".tmp";

            try
            {
                var json = JsonSerializer.Serialize(state, JsonOptions);
                File.WriteAllText(tmp, json);
                File.Move(tmp, path, overwrite: true);
                _logger.LogDebug("Persistence: saved {Service} state to {Path}", serviceName, path);
            }
            catch
            {
                try { File.Delete(tmp); } catch (IOException) { /* best-effort */ }
                throw;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Persistence: failed to save {Service}", serviceName);
        }
    }

    private object? LoadState(string serviceName)
    {
        var path = Path.Combine(_stateDir, $"{serviceName}.json");
        if (!File.Exists(path)) return null;

        try
        {
            var json = File.ReadAllText(path);
            var data = JsonSerializer.Deserialize<object>(json, JsonOptions);
            _logger.LogDebug("Persistence: loaded {Service} state from {Path}", serviceName, path);
            return data;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Persistence: failed to load {Service}", serviceName);
            return null;
        }
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = false,
        PropertyNamingPolicy = null,
    };
}
