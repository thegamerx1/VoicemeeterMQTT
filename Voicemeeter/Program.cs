using System.Text.Json;
using Serilog;
using VoicemeeterMQTT;

static void WriteAllTextAtomic(string path, string contents)
{
    var temp = path + ".tmp";
    File.WriteAllText(temp, contents);
    File.Move(temp, path, overwrite: true);
}

static FileStream AcquireSingleInstanceLock(string appDataPath)
{
    var lockPath = Path.Combine(appDataPath, "service.lock");
    try
    {
        return new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
    }
    catch (IOException ex)
    {
        throw new InvalidOperationException("Another VoicemeeterMQTT instance is already running for this user.", ex);
    }
}

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss}] {Level:u3} {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

var exitTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
var exitCode = 0;

try
{
    Log.Information("Starting Voicemeeter MQTT Service");
    var appDataPath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "voicemeetermqtt");
    var configPath = Path.Combine(appDataPath, "config.json");
    Directory.CreateDirectory(appDataPath);
    using var instanceLock = AcquireSingleInstanceLock(appDataPath);

    MqttConfig config;
    var configNeedsSave = false;
    if (File.Exists(configPath))
    {
        var json = File.ReadAllText(configPath);
        config = JsonSerializer.Deserialize(json, AppJsonContext.Default.MqttConfig) ?? new MqttConfig();
        Log.Information("Loaded config from {ConfigPath}", configPath);
    }
    else
    {
        config = new MqttConfig();
        configNeedsSave = true;
        Log.Warning("Config file not found at {ConfigPath}; creating a safe default configuration", configPath);
    }

    configNeedsSave |= config.NormalizeAndValidate();
    if (config.UseEncryption && config.SkipCertificateValidation)
        Log.Warning("MQTT certificate validation is DISABLED. Use this only in a controlled test environment.");

    if (configNeedsSave)
    {
        WriteAllTextAtomic(configPath, JsonSerializer.Serialize(config, AppJsonContext.Default.MqttConfig));
        Log.Information("Saved normalized config to {ConfigPath}", configPath);
    }

    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        exitTcs.TrySetResult(true);
    };
    AppDomain.CurrentDomain.ProcessExit += (_, _) => exitTcs.TrySetResult(true);

    await using (var service = new VoicemeeterMqttService(config, appDataPath))
    {
        await service.StartAsync();
        await exitTcs.Task;
    }
}
catch (Exception ex)
{
    exitCode = 1;
    Log.Fatal(ex, "Application terminated unexpectedly");
}
finally
{
    Environment.ExitCode = exitCode;
    await Log.CloseAndFlushAsync();
}
