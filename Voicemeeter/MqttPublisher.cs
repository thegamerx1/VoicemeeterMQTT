using MQTTnet;
using MQTTnet.Client;
using Serilog;

namespace VoicemeeterMQTT;

/// <summary>
/// Lightweight MQTT client for publishing Voicemeeter state
/// Uses async/await for minimal blocking
/// </summary>
public class MqttPublisher : IAsyncDisposable
{
	private readonly MqttConfig _config;
	private IMqttClient? _client;
	private bool _connected = false;
	private readonly SemaphoreSlim _publishLock = new(1);
	public Func<string, string, Task>? OnMessageReceived { get; set; }

	public MqttPublisher(MqttConfig config)
	{
		_config = config;
	}

	public async Task ConnectAsync()
	{
		try
		{
			var factory = new MqttFactory();
			_client = factory.CreateMqttClient();

			var options = new MqttClientOptionsBuilder()
				.WithTcpServer(_config.BrokerHost, _config.BrokerPort)
				.WithClientId(_config.ClientId)
				.WithCleanSession(false)
				.WithKeepAlivePeriod(TimeSpan.FromSeconds(60));

			// Use TLS on port 8883
			if (_config.BrokerPort == 8883)
			{
				options.WithTlsOptions(tlsOptions =>
				{
					tlsOptions.WithAllowUntrustedCertificates(_config.SkipCertificateValidation);
					tlsOptions.WithIgnoreCertificateChainErrors(_config.SkipCertificateValidation);
					tlsOptions.WithIgnoreCertificateRevocationErrors(_config.SkipCertificateValidation);
				});
				Log.Debug("Using TLS for MQTT connection (SkipValidation={SkipCert})", _config.SkipCertificateValidation);
			}

			// Add credentials if provided
			if (!string.IsNullOrEmpty(_config.Username) && !string.IsNullOrEmpty(_config.Password))
			{
				options.WithCredentials(_config.Username, _config.Password);
				Log.Debug("Using MQTT credentials for user {Username}", _config.Username);
			}

			// Setup event handlers BEFORE connecting
			_client.ApplicationMessageReceivedAsync += HandleApplicationMessageReceivedAsync;
			_client.DisconnectedAsync += async e =>
			{
				Log.Warning("MQTT disconnected: {Reason}", e.Reason);
				_connected = false;

				// Attempt automatic reconnection after 5 seconds
				if (e.ClientWasConnected)
				{
					await Task.Delay(5000);
					try
					{
						await _client.ConnectAsync(options.Build());
						_connected = true;
						Log.Information("MQTT reconnected successfully");
						await PublishAsync($"{_config.BaseTopic}/status", "online", retain: true);
					}
					catch (Exception ex)
					{
						Log.Error(ex, "Failed to reconnect to MQTT broker");
					}
				}
			};

			await _client.ConnectAsync(options.Build());
			_connected = true;

			Log.Information("Connected to MQTT broker {Host}:{Port}", _config.BrokerHost, _config.BrokerPort);

			// Publish online status
			await PublishAsync($"{_config.BaseTopic}/status", "online", retain: true);
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Failed to connect to MQTT broker");
			throw;
		}
	}

	public async Task PublishAsync(string topic, string payload, bool retain = false)
	{
		if (!_connected || _client == null) return;

		try
		{
			await _publishLock.WaitAsync();
			try
			{
				var message = new MqttApplicationMessageBuilder()
					.WithTopic(topic)
					.WithPayload(payload)
					.WithRetainFlag(retain)
					.WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce)
					.Build();

				await _client.PublishAsync(message);
			}
			finally
			{
				_publishLock.Release();
			}
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Error publishing to topic {Topic}", topic);
		}
	}

	public async Task PublishAsync(string topic, float value, bool retain = false)
	{
		if (!_connected || _client == null) return;
		await PublishAsync(topic, value.ToString("F3"), retain);
	}

	public async Task PublishAsync(string topic, int value, bool retain = false)
	{
		if (!_connected || _client == null) return;
		await PublishAsync(topic, value.ToString(), retain);
	}

	public async Task PublishAsync(string topic, bool value, bool retain = false)
	{
		if (!_connected || _client == null) return;
		await PublishAsync(topic, value ? "true" : "false", retain);
	}

	public async Task SubscribeAsync(string topic)
	{
		if (!_connected || _client == null) return;

		try
		{
			await _client.SubscribeAsync(topic);
			Log.Debug("Subscribed to topic {Topic}", topic);
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Error subscribing to topic {Topic}", topic);
		}
	}

	private async Task HandleApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
	{
		try
		{
			var topic = e.ApplicationMessage.Topic;
			var payload = System.Text.Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
			Log.Debug("Received message on {Topic}: {Payload}", topic, payload);
			if (OnMessageReceived != null)
				await OnMessageReceived(topic, payload);
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Error handling MQTT message");
		}
	}

	public async Task DisconnectAsync()
	{
		if (_connected && _client != null)
		{
			try
			{
				await PublishAsync($"{_config.BaseTopic}/status", "offline", retain: true);
				await _client.DisconnectAsync();
				_connected = false;
				Log.Information("Disconnected from MQTT broker");
			}
			catch (Exception ex)
			{
				Log.Error(ex, "Error disconnecting from MQTT");
			}
		}
	}

	public async ValueTask DisposeAsync()
	{
		await DisconnectAsync();
		_client?.Dispose();
		_publishLock?.Dispose();
	}
}
