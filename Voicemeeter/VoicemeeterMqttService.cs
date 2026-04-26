using Serilog;
using System.Text.Json;

namespace VoicemeeterMQTT;

/// <summary>
/// Main service coordinating Voicemeeter, Audio Devices, and MQTT
/// Efficiently monitors state and publishes changes
/// </summary>
public class VoicemeeterMqttService : IAsyncDisposable
{
	private readonly MqttConfig _config;
	private readonly VoicemeeterAPI _voicemeeter = new();
	private readonly AudioDeviceManager _audioManager = new();
	private readonly MqttPublisher _mqtt;

	private CancellationTokenSource? _cancellationSource;
	private Task? _updateTask;

	// Cache last known values to avoid redundant publishes
	private readonly Dictionary<string, object> _lastValues = new();
	private bool _disposed = false;

	public int stripCount = 7;
	public int busCount = 7;

	public VoicemeeterMqttService(MqttConfig config)
	{
		_config = config;
		_mqtt = new MqttPublisher(config);
	}

	public async Task StartAsync()
	{
		try
		{
			// Initialize components
			if (!_voicemeeter.Initialize())
			{
				Log.Warning("Voicemeeter not available - will run in audio-only mode");
			}

			_audioManager.Initialize();
			await _mqtt.ConnectAsync();

			// Publish Home Assistant discovery configs
			await PublishHomeAssistantDiscoveryAsync();
			await PublishHomeAssistantDiscoveryHardwareDeviceAsync();

			// Setup message handler for commands
			_mqtt.OnMessageReceived = HandleMqttMessageAsync;

			// Subscribe to command topics
			for (int i = 0; i <= stripCount; i++)
			{
				await _mqtt.SubscribeAsync($"{_config.BaseTopic}/strip{i}/+/set");
			}
			for (int i = 0; i <= busCount; i++)
			{
				await _mqtt.SubscribeAsync($"{_config.BaseTopic}/bus{i}/+/set");
			}

			_cancellationSource = new CancellationTokenSource();
			_updateTask = UpdateLoopAsync(_cancellationSource.Token);

			Log.Information("VoicemeeterMqttService started");
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Error starting service");
			throw;
		}
	}

	private async Task UpdateLoopAsync(CancellationToken cancellationToken)
	{
		var checkInterval = TimeSpan.FromMilliseconds(_config.UpdateIntervalMs);

		try
		{
			while (!cancellationToken.IsCancellationRequested)
			{
				bool hasChanges = false;

				var updated = _voicemeeter.Update();
				// Check for Voicemeeter changes
				if (updated)
				{
					hasChanges |= await CheckVoicemeeterStateAsync();

					// Check for audio device changes
					hasChanges |= await CheckAudioDeviceStateAsync();
				}
				// Only sleep if no changes found (still check frequently)
				if (!hasChanges)
				{
					await Task.Delay(checkInterval, cancellationToken);
				}
			}
		}
		catch (OperationCanceledException)
		{
			// Expected on shutdown
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Error in update loop");
		}
	}

	private async Task<bool> CheckVoicemeeterStateAsync()
	{
		bool changed = false;

		for (int i = 0; i <= stripCount; i++)
		{
			var paramName = $"Strip[{i}].Gain";

			var gain = _voicemeeter.GetParameter(paramName);
			if (gain.HasValue)
			{
				var topic = $"{_config.BaseTopic}/strip{i}/gain";

				if (await PublishIfChanged(topic, gain.Value))
				{
					var label = _voicemeeter.GetParameterString($"Strip[{i}].Label") ?? $"Strip {i}";
					Log.Debug("{Name} Gain: {Gain:F2}", label, gain.Value);
					changed = true;
				}
			}
			var muteParamName = $"Strip[{i}].Mute";

			var mute = _voicemeeter.GetParameter(muteParamName);
			if (mute.HasValue)
			{
				var topic = $"{_config.BaseTopic}/strip{i}/muted";

				if (await PublishIfChanged(topic, (int)mute.Value))
				{
					var label = $"Strip {i}";
					Log.Debug("{Name} Muted: {Muted}", label, (int)mute.Value);
					changed = true;
				}
			}
		}

		// Monitor output buses A1-A4 (use Gain, not Volume)
		for (int i = 0; i <= busCount; i++)
		{
			var paramName = $"Bus[{i}].Gain";
			var gain = _voicemeeter.GetParameter(paramName);
			if (gain.HasValue)
			{
				var topic = $"{_config.BaseTopic}/bus{i}/gain";

				if (await PublishIfChanged(topic, gain.Value))
				{
					Log.Debug("Output Bus {i} Gain: {Gain:F2}", i, gain.Value);
					changed = true;
				}
			}

			var muteParamName = $"Bus[{i}].Mute";
			var mute = _voicemeeter.GetParameter(muteParamName);
			if (mute.HasValue)
			{
				var topic = $"{_config.BaseTopic}/bus{i}/muted";

				if (await PublishIfChanged(topic, (int)mute.Value))
				{
					var label = $"Bus {i}";
					Log.Debug("{Name} Muted: {Muted}", label, (int)mute.Value);
					changed = true;
				}
			}
		}

		return changed;
	}

	private async Task PublishHomeAssistantDiscoveryHardwareDeviceAsync()
	{
		// foreach (var device in _audioManager.GetDevices())
		// {
		// print all the device info for testing
		// 	Log.Debug("Device: {Type} [{Name}] ID: {Id} Volume: {Volume} Muted: {Muted}", device.Type, device.Name, device.Id, device.Volume, device.IsMuted);
		// }
		// var discovery = $$"""
		// {
		// "name": "{{label}} Gain",
		// "unique_id": "voicemeeter_strip{{i}}_gain",
		// "state_topic": "{{_config.BaseTopic}}/strip{{i}}/gain",
		// "command_topic": "{{_config.BaseTopic}}/strip{{i}}/gain/set",
		// "min": -60.0,
		// "max": 0.0,
		// "step": 0.5,
		// "unit_of_measurement": "dB",
		// "icon": "mdi:import",

		// "availability": {
		// 	"topic": "{{_config.BaseTopic}}/status",
		// 	"payload_available": "online",
		// 	"payload_not_available": "offline"
		// },

		// "device": {
		// 	"identifiers": ["voicemeeter"],
		// 	"name": "Voicemeeter",
		// 	"manufacturer": "VB-Audio"
		// }
		// }
		// """;
		// await PublishDiscoveryAsync("number", $"strip{i}_gain", i, discovery);

		// var muteDiscovery = $$"""
		// {
		// "name": "{{label}} Mute",
		// "unique_id": "voicemeeter_strip{{i}}_mute",
		// "state_topic": "{{_config.BaseTopic}}/strip{{i}}/muted",
		// "command_topic": "{{_config.BaseTopic}}/strip{{i}}/muted/set",
		// "payload_on": "1",
		// "payload_off": "0",
		// "state_on": "1",
		// "state_off": "0",
		// "icon": "mdi:microphone-off",

		// "availability": {
		// 	"topic": "{{_config.BaseTopic}}/status",
		// 	"payload_available": "online",
		// 	"payload_not_available": "offline"
		// },

		// "device": {
		// 	"identifiers": ["voicemeeter"],
		// 	"name": "Voicemeeter",
		// 	"manufacturer": "VB-Audio"
		// }
		// }
		// """;
		// await PublishDiscoveryAsync("switch", $"strip{i}_mute", i, muteDiscovery);
	}

	private async Task<bool> CheckAudioDeviceStateAsync()
	{
		bool changed = false;
		_audioManager.RefreshDevices();

		foreach (var device in _audioManager.GetDevices())
		{
			var devicePath = $"voicemeeter/audio/{device.Type}/{Uri.EscapeDataString(device.Id)}";
			Log.Debug("Checking device: {Type} [{Name}] Path: {Path} Value: {Value}", device.Type, device.Name, devicePath, device.Volume);
			if (device.Volume.HasValue)
			{
				if (await PublishIfChanged($"{devicePath}/volume", (float)device.Volume.Value))
				{
					Log.Debug("{Type} [{Name}] Volume: {Volume:F2}", device.Type, device.Name, device.Volume.Value);
					changed = true;
				}
			}

			if (await PublishIfChanged($"{devicePath}/muted", device.IsMuted))
			{
				Log.Debug("{Type} [{Name}] Muted: {Muted}", device.Type, device.Name, device.IsMuted);
				changed = true;
			}

			if (await PublishIfChanged($"{devicePath}/peak", device.PeakLevel))
			{
				Log.Debug("{Type} [{Name}] Peak: {Peak:F2}", device.Type, device.Name, device.PeakLevel);
				changed = true;
			}

			if (device.IsDefault)
			{
				await _mqtt.PublishAsync($"{devicePath}/default", "true", retain: true);
			}
		}

		return changed;
	}

	private async Task<bool> PublishIfChanged(string topic, object value)
	{
		// Convert to string key for caching
		var key = topic;
		var valueStr = value switch
		{
			float f => f.ToString("F3"),
			double d => d.ToString("F3"),
			bool b => b ? "true" : "false",
			int i => i.ToString(),
			_ => value.ToString() ?? ""
		};

		if (!_lastValues.TryGetValue(key, out var lastValue) || !lastValue.Equals(valueStr))
		{
			_lastValues[key] = valueStr;
			Log.Debug("Publishing topic {Topic} value: {Value}", topic, valueStr);
			await _mqtt.PublishAsync(topic, valueStr);
			return true;
		}

		return false;
	}

	private async Task PublishDiscoveryAsync<T>(string component, string label, int index, T value)
	{
		string json;

		if (value is string s)
		{
			json = s;
		}
		else
		{
			// json = JsonSerializer.Serialize(value);
			throw new Exception("This should never happen");
		}

		string topic = $"homeassistant/{component}/voicemeeter/{label}/config";
		await _mqtt.PublishAsync(topic, json, retain: true);

		Log.Debug("Published discovery for {Name} at index {Index}", label, index);
	}



	private async Task PublishHomeAssistantDiscoveryAsync()
	{
		for (int i = 0; i <= stripCount; i++)
		{
			// Get label from config or fallback to index
			var label = _voicemeeter.GetParameterString($"Strip[{i}].Label") ?? $"Strip {i}";

			var discovery = $$"""
			{
			"name": "{{label}} Gain",
			"unique_id": "voicemeeter_strip{{i}}_gain",
			"state_topic": "{{_config.BaseTopic}}/strip{{i}}/gain",
			"command_topic": "{{_config.BaseTopic}}/strip{{i}}/gain/set",
			"min": -60.0,
			"max": 0.0,
			"step": 0.5,
			"unit_of_measurement": "dB",
			"icon": "mdi:import",

			"availability": {
				"topic": "{{_config.BaseTopic}}/status",
				"payload_available": "online",
				"payload_not_available": "offline"
			},

			"device": {
				"identifiers": ["voicemeeter"],
				"name": "Voicemeeter",
				"manufacturer": "VB-Audio"
			}
			}
			""";
			await PublishDiscoveryAsync("number", $"strip{i}_gain", i, discovery);

			var muteDiscovery = $$"""
			{
			"name": "{{label}} Mute",
			"unique_id": "voicemeeter_strip{{i}}_mute",
			"state_topic": "{{_config.BaseTopic}}/strip{{i}}/muted",
			"command_topic": "{{_config.BaseTopic}}/strip{{i}}/muted/set",
			"payload_on": "1",
			"payload_off": "0",
			"state_on": "1",
			"state_off": "0",
			"icon": "mdi:microphone-off",

			"availability": {
				"topic": "{{_config.BaseTopic}}/status",
				"payload_available": "online",
				"payload_not_available": "offline"
			},

			"device": {
				"identifiers": ["voicemeeter"],
				"name": "Voicemeeter",
				"manufacturer": "VB-Audio"
			}
			}
			""";
			await PublishDiscoveryAsync("switch", $"strip{i}_mute", i, muteDiscovery);
		}

		for (int i = 0; i <= busCount; i++)
		{
			var label = _voicemeeter.GetParameterString($"Bus[{i}].Label") ?? $"Bus {i}";
			var discovery = $$"""
			{
			"name": "Output {{label}} Gain",
			"unique_id": "voicemeeter_bus{{i}}_gain",
			"state_topic": "{{_config.BaseTopic}}/bus{{i}}/gain",
			"command_topic": "{{_config.BaseTopic}}/bus{{i}}/gain/set",
			"min": -60.0,
			"max": 0.0,
			"step": 0.5,
			"unit_of_measurement": "dB",
			"icon": "mdi:export",

			"availability": {
				"topic": "{{_config.BaseTopic}}/status",
				"payload_available": "online",
				"payload_not_available": "offline"
			},

			"device": {
				"identifiers": ["voicemeeter"],
				"name": "Voicemeeter",
				"manufacturer": "VB-Audio"
			}
			}
			""";

			await PublishDiscoveryAsync("number", $"bus{i}_gain", i, discovery);

			var muteDiscovery = $$"""
			{
			"name": "Output {{label}} Mute",
			"unique_id": "voicemeeter_bus{{i}}_mute",
			"state_topic": "{{_config.BaseTopic}}/bus{{i}}/muted",
			"command_topic": "{{_config.BaseTopic}}/bus{{i}}/muted/set",
			"payload_on": "1",
			"payload_off": "0",
			"state_on": "1",
			"state_off": "0",
			"icon": "mdi:volume-mute",

			"availability": {
				"topic": "{{_config.BaseTopic}}/status",
				"payload_available": "online",
				"payload_not_available": "offline"
			},

			"device": {
				"identifiers": ["voicemeeter"],
				"name": "Voicemeeter",
				"manufacturer": "VB-Audio"
			}
			}
			""";
			await PublishDiscoveryAsync("switch", $"bus{i}_mute", i, muteDiscovery);
		}

		Log.Information("Home Assistant discovery published");
	}

	private async Task HandleMqttMessageAsync(string topic, string payload)
	{
		Log.Debug("Received MQTT message on topic {Topic} with payload: {Payload}", topic, payload);
		try
		{
			if (topic.EndsWith("/gain/set"))
			{
				if (float.TryParse(payload, out var gainDb))
				{
					for (int i = 0; i <= stripCount; i++)
					{
						if (topic.Contains($"strip{i}"))
						{
							Log.Debug($"Set strip{i} gain from MQTT: {gainDb:F2}dB");
							_voicemeeter.SetParameter($"Strip[{i}].Gain", gainDb);
							await _mqtt.PublishAsync($"{_config.BaseTopic}/strip{i}/gain", gainDb, retain: true);
							break;
						}
					}
					for (int i = 0; i <= busCount; i++)
					{
						if (topic.Contains($"bus{i}"))
						{
							Log.Debug($"Set bus{i} gain from MQTT: {gainDb:F2}dB", gainDb);
							_voicemeeter.SetParameter($"Bus[{i}].Gain", gainDb);
							await _mqtt.PublishAsync($"{_config.BaseTopic}/bus{i}/gain", gainDb, retain: true);
							break;
						}
					}
				}
				else
				{
					Log.Warning("Received invalid gain value on topic {Topic}: {Payload}", topic, payload);
				}
			}
			else if (topic.EndsWith("/muted/set"))
			{
				float newPayload = (payload == "1") ? 1.0f : 0.0f;
				for (int i = 0; i <= stripCount; i++)
				{
					if (topic.Contains($"strip{i}"))
					{
						Log.Debug($"Set strip{i} mute from MQTT: {newPayload}", newPayload);
						_voicemeeter.SetParameter($"Strip[{i}].Mute", newPayload);
						await _mqtt.PublishAsync($"{_config.BaseTopic}/strip{i}/muted", newPayload, retain: true);
						break;
					}
				}
				for (int i = 0; i <= busCount; i++)
				{
					if (topic.Contains($"bus{i}"))
					{
						Log.Debug($"Set bus{i} mute from MQTT: {newPayload}", newPayload);
						_voicemeeter.SetParameter($"Bus[{i}].Mute", newPayload);
						await _mqtt.PublishAsync($"{_config.BaseTopic}/bus{i}/muted", newPayload, retain: true);
						break;
					}
				}
			}
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Error handling MQTT command on topic {Topic}", topic);
		}
	}

	public async ValueTask DisposeAsync()
	{
		if (!_disposed)
		{
			Log.Information("Stopping VoicemeeterMqttService...");
			_cancellationSource?.Cancel();

			if (_updateTask != null)
			{
				try
				{
					await _updateTask;
				}
				catch (OperationCanceledException) { }
			}

			_cancellationSource?.Dispose();
			_voicemeeter?.Dispose();
			_audioManager?.Dispose();
			await _mqtt.DisposeAsync();

			_disposed = true;
			Log.Information("VoicemeeterMqttService stopped");
		}
	}
}
