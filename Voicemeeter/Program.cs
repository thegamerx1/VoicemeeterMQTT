using Serilog;
using VoicemeeterMQTT;
using System.Text.Json;

// Configure Serilog for minimal overhead
Log.Logger = new LoggerConfiguration()
	 .MinimumLevel.Debug()
	 .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss}] {Level:u3} {Message:lj}{NewLine}{Exception}")
	 .CreateLogger();


var exitTcs = new TaskCompletionSource();
try
{
	Log.Information("Starting Voicemeeter MQTT Service");
	var appDataPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "voicemeetermqtt");
	var configPath = Path.Combine(appDataPath, "config.json");

	MqttConfig config;
	if (File.Exists(configPath))
	{
		var json = File.ReadAllText(configPath);
		config = JsonSerializer.Deserialize(json, AppJsonContext.Default.MqttConfig) ?? new MqttConfig();
		Log.Information("Loaded config from {ConfigPath}", configPath);
	}
	else
	{
		Log.Warning("Config file not found at {ConfigPath}. Using defaults.", configPath);
		config = new MqttConfig();

		// Create directory and save default config
		Directory.CreateDirectory(appDataPath);
		var defaultJson = JsonSerializer.Serialize(
			config,
			AppJsonContext.Default.MqttConfig
		);
		File.WriteAllText(configPath, defaultJson);
		Log.Information("Created default config at {ConfigPath}", configPath);
	}

	if (!VoicemeeterInterop.LoadDll())
	{
		Log.Error("Failed to load VoicemeeterRemote.dll. Check Voicemeeter installation.");
		return;
	}

	await using var service = new VoicemeeterMqttService(config);

	Console.CancelKeyPress += (s, e) =>
	{
		e.Cancel = true;
		exitTcs.TrySetResult();
	};

	AppDomain.CurrentDomain.ProcessExit += (_, __) =>
	{
		exitTcs.TrySetResult();
	};

	await service.StartAsync();

	// Wait until exit signal
	await exitTcs.Task;
}
catch (Exception ex)
{
	Log.Fatal(ex, "Application terminated unexpectedly");
	Environment.Exit(1);
}
finally
{
	await Log.CloseAndFlushAsync();
}
