using System.Globalization;
using System.Security.Authentication;
using MQTTnet;
using MQTTnet.Adapter;
using MQTTnet.Client;
using MQTTnet.Protocol;
using Serilog;

namespace VoicemeeterMQTT;

/// <summary>
/// Owns the single MQTT connection and all reconnect lifecycle.
/// A CONNECT is not considered Ready until the caller's reconciliation callback succeeds.
/// </summary>
public sealed class MqttPublisher : IAsyncDisposable
{
    private readonly MqttConfig _config;
    private readonly MqttFactory _factory = new();
    private readonly SemaphoreSlim _publishLock = new(1, 1);
    private readonly SemaphoreSlim _disconnectSignal = new(0, 1);
    private IMqttClient? _client;
    private MqttClientOptions? _options;
    private CancellationTokenSource? _lifetimeCts;
    private Task? _supervisorTask;
    private volatile bool _connected;
    private volatile bool _ready;
    private int _stopping;
    private int _reconcileGeneration;
    private static readonly TimeSpan StableConnectionReset = TimeSpan.FromSeconds(30);

    public Func<string, string, bool, Task>? OnMessageReceived { get; set; }
    public Func<CancellationToken, Task<bool>>? OnConnectedReconcileAsync { get; set; }
    public Func<Task>? OnDisconnectedAsync { get; set; }

    public bool IsConnected => _connected && _client?.IsConnected == true;
    public bool IsReady => IsConnected && _ready;

    /// <summary>
    /// Marks the current MQTT connection as needing full application reconciliation again.
    /// The supervisor remains the only owner of reconciliation work.
    /// </summary>
    public void RequestReconciliation(string reason = "unspecified")
    {
        if (!IsConnected || Volatile.Read(ref _stopping) != 0)
            return;

        Log.Debug("MQTT reconciliation requested: {Reason}", reason);
        Interlocked.Increment(ref _reconcileGeneration);

        // If the supervisor is already reconciling, the generation change makes that pass run
        // again before Ready is exposed. Otherwise wake it from the connected wait state.
        if (!_ready)
            return;

        _ready = false;
        try
        {
            _disconnectSignal.Release();
        }
        catch (SemaphoreFullException)
        {
            // Wake-up already queued.
        }
    }

    public MqttPublisher(MqttConfig config)
    {
        _config = config;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_supervisorTask != null)
            return Task.CompletedTask;

        _lifetimeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _client = _factory.CreateMqttClient();
        _options = BuildOptions();

        _client.ApplicationMessageReceivedAsync += HandleApplicationMessageReceivedAsync;
        _client.DisconnectedAsync += HandleDisconnectedAsync;

        _supervisorTask = RunSupervisorAsync(_lifetimeCts.Token);
        return Task.CompletedTask;
    }

    private MqttClientOptions BuildOptions()
    {
        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(_config.BrokerHost, _config.BrokerPort)
            .WithClientId(_config.EffectiveClientId)
            .WithCleanSession(true)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(_config.KeepAliveSeconds))
            .WithTimeout(TimeSpan.FromSeconds(_config.ConnectTimeoutSeconds))
            .WithWillTopic($"{_config.EffectiveBaseTopic}/status")
            .WithWillPayload("offline")
            .WithWillQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .WithWillRetain(true);

        if (_config.UseEncryption)
        {
            options.WithTlsOptions(tlsOptions =>
            {
                tlsOptions.WithAllowUntrustedCertificates(_config.SkipCertificateValidation);
                tlsOptions.WithIgnoreCertificateChainErrors(_config.SkipCertificateValidation);
                tlsOptions.WithIgnoreCertificateRevocationErrors(_config.SkipCertificateValidation);
            });
            Log.Information("MQTT TLS enabled (certificate validation bypass={SkipValidation})", _config.SkipCertificateValidation);
        }

        if (!string.IsNullOrWhiteSpace(_config.Username))
        {
            options.WithCredentials(_config.Username, _config.Password);
            Log.Debug("Using MQTT credentials for user {Username}", _config.Username);
        }

        return options.Build();
    }

    private async Task RunSupervisorAsync(CancellationToken cancellationToken)
    {
        var attempt = 0;
        DateTimeOffset? readySince = null;

        while (!cancellationToken.IsCancellationRequested)
        {
            if (!IsConnected)
            {
                try
                {
                    Log.Information("Connecting to MQTT broker {Host}:{Port} as {ClientId}",
                        _config.BrokerHost, _config.BrokerPort, _config.EffectiveClientId);

                    await _client!.ConnectAsync(_options!, cancellationToken);
                    _connected = true;
                    _ready = false;
                    Log.Information("Connected to MQTT broker {Host}:{Port}", _config.BrokerHost, _config.BrokerPort);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _connected = false;
                    _ready = false;
                    attempt++;
                    var slowRetry = IsAuthenticationOrConfigurationFailure(ex);
                    var delay = ComputeReconnectDelay(attempt, slowRetry);
                    Log.Error(ex, "MQTT connect failed; retrying in {DelayMs} ms (slowRetry={SlowRetry})", delay.TotalMilliseconds, slowRetry);
                    try
                    {
                        await Task.Delay(delay, cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                    continue;
                }
            }

            // Reconcile subscriptions, discovery and state before exposing Ready.
            while (IsConnected && !cancellationToken.IsCancellationRequested && !_ready)
            {
                try
                {
                    var generationAtStart = Volatile.Read(ref _reconcileGeneration);
                    var reconciled = OnConnectedReconcileAsync == null ||
                                     await OnConnectedReconcileAsync(cancellationToken);
                    if (reconciled && IsConnected)
                    {
                        if (Volatile.Read(ref _reconcileGeneration) != generationAtStart)
                        {
                            Log.Debug("State changed during MQTT reconciliation; running another pass before Ready");
                            continue;
                        }

                        _ready = true;
                        readySince ??= DateTimeOffset.UtcNow;
                        Log.Information("MQTT connection reconciliation complete; bridge is ready");
                        break;
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "MQTT post-connect reconciliation failed");
                }

                if (IsConnected && !cancellationToken.IsCancellationRequested)
                {
                    attempt++;
                    var delay = ComputeReconnectDelay(attempt);
                    Log.Warning("Retrying MQTT reconciliation in {DelayMs} ms", delay.TotalMilliseconds);
                    await Task.Delay(delay, cancellationToken);
                }
            }

            if (cancellationToken.IsCancellationRequested)
                break;

            if (IsConnected)
            {
                try
                {
                    await _disconnectSignal.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            // A successful TCP/MQTT handshake is not enough to reset backoff. If a broker,
            // router or TLS middlebox accepts connections and immediately drops them, retrying
            // in a tight loop creates a reconnect storm. Reset only after a genuinely stable run.
            if (!IsConnected && !cancellationToken.IsCancellationRequested)
            {
                if (readySince.HasValue && DateTimeOffset.UtcNow - readySince.Value >= StableConnectionReset)
                    attempt = 0;

                readySince = null;
                attempt++;
                var delay = ComputeReconnectDelay(attempt);
                Log.Warning("MQTT connection lost; reconnecting in {DelayMs} ms (attempt {Attempt})",
                    delay.TotalMilliseconds, attempt);
                try
                {
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private TimeSpan ComputeReconnectDelay(int attempt, bool slowRetry = false)
    {
        var minMs = _config.ReconnectMinSeconds * 1000L;
        var maxMs = _config.ReconnectMaxSeconds * 1000L;
        if (slowRetry)
        {
            // Bad credentials/certificates/protocol do not usually heal in a few seconds.
            // Retry at the configured ceiling instead of hammering the broker.
            return TimeSpan.FromMilliseconds(maxMs);
        }

        var exponent = Math.Min(attempt - 1, 20);
        var cap = Math.Min(maxMs, minMs * (1L << exponent));
        var jittered = Random.Shared.NextInt64(0, Math.Max(1, cap) + 1);
        return TimeSpan.FromMilliseconds(jittered);
    }


    private static bool IsAuthenticationOrConfigurationFailure(Exception exception)
    {
        for (Exception? current = exception; current != null; current = current.InnerException)
        {
            if (current is AuthenticationException)
                return true;

            if (current is MqttConnectingFailedException mqttFailure)
            {
                return mqttFailure.ResultCode is
                    MqttClientConnectResultCode.MalformedPacket or
                    MqttClientConnectResultCode.ProtocolError or
                    MqttClientConnectResultCode.UnsupportedProtocolVersion or
                    MqttClientConnectResultCode.ClientIdentifierNotValid or
                    MqttClientConnectResultCode.BadUserNameOrPassword or
                    MqttClientConnectResultCode.NotAuthorized or
                    MqttClientConnectResultCode.Banned or
                    MqttClientConnectResultCode.BadAuthenticationMethod;
            }
        }

        return false;
    }

    private async Task HandleDisconnectedAsync(MqttClientDisconnectedEventArgs e)
    {
        _connected = false;
        _ready = false;

        if (Volatile.Read(ref _stopping) == 0)
            Log.Warning("MQTT disconnected: {Reason}", e.Reason);
        else
            Log.Debug("MQTT disconnected during shutdown");

        if (OnDisconnectedAsync != null)
        {
            try
            {
                await OnDisconnectedAsync();
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error in MQTT disconnect callback");
            }
        }

        if (Volatile.Read(ref _stopping) == 0)
        {
            try
            {
                _disconnectSignal.Release();
            }
            catch (SemaphoreFullException)
            {
                // Coalesce duplicate disconnect notifications.
            }
        }
    }

    public async Task<bool> PublishAsync(
        string topic,
        string payload,
        bool retain = false,
        MqttQualityOfServiceLevel qos = MqttQualityOfServiceLevel.AtMostOnce,
        CancellationToken cancellationToken = default)
    {
        if (!IsConnected || _client == null)
            return false;

        await _publishLock.WaitAsync(cancellationToken);
        try
        {
            if (!IsConnected)
                return false;

            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(payload)
                .WithRetainFlag(retain)
                .WithQualityOfServiceLevel(qos)
                .Build();

            var result = await _client.PublishAsync(message, cancellationToken);
            if (!result.IsSuccess)
            {
                Log.Error("Broker rejected publish to {Topic}: {ReasonCode} {ReasonString}", topic, result.ReasonCode, result.ReasonString);
                return false;
            }
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return false;
        }
        catch (MqttClientDisconnectedException)
        {
            // A publish that was in flight when the broker/network disappeared is an expected
            // consequence of the disconnect. The supervisor will reconnect and reconciliation
            // will republish authoritative discovery/state, so do not report this as an application
            // error with a scary stack trace.
            Log.Warning("Publish to {Topic} was interrupted because MQTT disconnected", topic);
            return false;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error publishing to topic {Topic}", topic);
            return false;
        }
        finally
        {
            _publishLock.Release();
        }
    }

    public Task<bool> PublishAsync(
        string topic,
        float value,
        bool retain = false,
        MqttQualityOfServiceLevel qos = MqttQualityOfServiceLevel.AtMostOnce,
        CancellationToken cancellationToken = default) =>
        PublishAsync(topic, value.ToString("0.###", CultureInfo.InvariantCulture), retain, qos, cancellationToken);

    public Task<bool> PublishAsync(
        string topic,
        int value,
        bool retain = false,
        MqttQualityOfServiceLevel qos = MqttQualityOfServiceLevel.AtMostOnce,
        CancellationToken cancellationToken = default) =>
        PublishAsync(topic, value.ToString(CultureInfo.InvariantCulture), retain, qos, cancellationToken);

    public Task<bool> PublishAsync(
        string topic,
        bool value,
        bool retain = false,
        MqttQualityOfServiceLevel qos = MqttQualityOfServiceLevel.AtMostOnce,
        CancellationToken cancellationToken = default) =>
        PublishAsync(topic, value ? "true" : "false", retain, qos, cancellationToken);

    public async Task<bool> SubscribeAsync(
        string topic,
        MqttQualityOfServiceLevel qos = MqttQualityOfServiceLevel.AtLeastOnce,
        CancellationToken cancellationToken = default)
    {
        if (!IsConnected || _client == null)
            return false;

        try
        {
            var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(filter => filter
                    .WithTopic(topic)
                    .WithQualityOfServiceLevel(qos))
                .Build();

            var result = await _client.SubscribeAsync(subscribeOptions, cancellationToken);
            var rejected = result.Items.FirstOrDefault(item => (int)item.ResultCode >= 0x80);
            if (rejected != null)
            {
                Log.Error("Broker rejected MQTT subscription to {Topic}: {Reason}", topic, rejected.ResultCode);
                return false;
            }

            Log.Debug("Subscribed to topic {Topic} with QoS {Qos}", topic, qos);
            return true;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error subscribing to topic {Topic}", topic);
            return false;
        }
    }

    private async Task HandleApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
    {
        try
        {
            var topic = e.ApplicationMessage.Topic;
            var payload = System.Text.Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment.ToArray());
            Log.Debug("Received message on {Topic}: {Payload}", topic, payload);
            if (OnMessageReceived != null)
                await OnMessageReceived(topic, payload, e.ApplicationMessage.Retain);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error handling MQTT message");
        }
    }

    public async Task StopAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) != 0)
            return;

        _ready = false;
        _lifetimeCts?.Cancel();

        if (_supervisorTask != null)
        {
            try
            {
                await _supervisorTask;
            }
            catch (OperationCanceledException)
            {
            }
        }

        if (_client?.IsConnected == true)
        {
            // Graceful shutdown suppresses the Will, so explicitly publish offline first.
            await PublishAsync(
                $"{_config.EffectiveBaseTopic}/status",
                "offline",
                retain: true,
                qos: MqttQualityOfServiceLevel.AtLeastOnce);

            try
            {
                await _client.DisconnectAsync(new MqttClientDisconnectOptionsBuilder().Build());
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Error during MQTT graceful disconnect");
            }
        }

        _connected = false;
        Log.Information("MQTT client stopped");
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _client?.Dispose();
        _lifetimeCts?.Dispose();
        _publishLock.Dispose();
        _disconnectSignal.Dispose();
    }
}
