using System.Text.Json;
using Serilog;

namespace VoicemeeterMQTT;

public sealed class RuntimeStateStore
{
    private readonly string _path;

    public RuntimeStateStore(string appDataPath)
    {
        _path = Path.Combine(appDataPath, "runtime-state.json");
    }

    public BridgeRuntimeState Load()
    {
        try
        {
            if (!File.Exists(_path))
                return new BridgeRuntimeState();

            var json = File.ReadAllText(_path);
            return JsonSerializer.Deserialize(json, AppJsonContext.Default.BridgeRuntimeState) ?? new BridgeRuntimeState();
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "Could not read runtime state from {Path}; rebuilding it", _path);
            return new BridgeRuntimeState();
        }
    }

    public bool Save(BridgeRuntimeState state)
    {
        string? temporaryPath = null;
        try
        {
            var directory = Path.GetDirectoryName(_path);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);

            var json = JsonSerializer.Serialize(state, AppJsonContext.Default.BridgeRuntimeState);
            temporaryPath = _path + ".tmp";
            File.WriteAllText(temporaryPath, json);
            File.Move(temporaryPath, _path, overwrite: true);
            return true;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Could not persist runtime state to {Path}", _path);
            if (!string.IsNullOrEmpty(temporaryPath))
            {
                try { File.Delete(temporaryPath); } catch { }
            }
            return false;
        }
    }
}
