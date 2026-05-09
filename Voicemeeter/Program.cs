using Serilog;
using VoicemeeterMQTT;
using System.Text.Json;

// Configure Serilog for minimal overhead
Log.Logger = new LoggerConfiguration()
	 .MinimumLevel.Debug()
	 .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss}] {Level:u3} {Message:lj}{NewLine}{Exception}")
	 .CreateLogger();


var exitTcs = new TaskCompletionSource();
var cancellationSource = new CancellationTokenSource();

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

	// Try to load DLL, but don't fail startup if it's not available yet
	bool dllLoaded = VoicemeeterInterop.LoadDll();
	if (!dllLoaded)
	{
		Log.Warning("Failed to load VoicemeeterRemote.dll. Will retry in background.");
	}

	await using var service = new VoicemeeterMqttService(config);

	Console.CancelKeyPress += (s, e) =>
	{
		e.Cancel = true;
		cancellationSource.Cancel();
		exitTcs.TrySetResult();
	};

	AppDomain.CurrentDomain.ProcessExit += (_, __) =>
	{
		cancellationSource.Cancel();
		exitTcs.TrySetResult();
	};

	await service.StartAsync();

	// Start background DLL retry task if not loaded
	var retryTask = !dllLoaded ? RetryLoadDllAsync(cancellationSource.Token) : Task.CompletedTask;

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

/// <summary>
/// Retries loading the Voicemeeter DLL in the background every 5 seconds
/// </summary>
static async Task RetryLoadDllAsync(CancellationToken cancellationToken)
{
	var retryInterval = TimeSpan.FromSeconds(5);
	int retryCount = 0;

	while (!cancellationToken.IsCancellationRequested)
	{
		try
		{
			await Task.Delay(retryInterval, cancellationToken);

			if (VoicemeeterInterop.LoadDll())
			{
				Log.Information("Successfully loaded VoicemeeterRemote.dll on retry #{RetryCount}", retryCount);
				return;
			}

			retryCount++;
			if (retryCount % 12 == 0) // Log every 60 seconds (12 * 5 seconds)
			{
				Log.Debug("Still retrying to load VoicemeeterRemote.dll. Retry count: {RetryCount}", retryCount);
			}
		}
		catch (OperationCanceledException)
		{
			// Application is shutting down
			break;
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Unexpected error during DLL retry");
		}
	}
}
