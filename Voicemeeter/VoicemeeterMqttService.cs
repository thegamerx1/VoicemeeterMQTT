using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using MQTTnet.Protocol;
using Serilog;

namespace VoicemeeterMQTT;

/// <summary>
/// Coordinates Voicemeeter, Windows Core Audio, MQTT and Home Assistant discovery.
/// MQTT Connected and application Ready are deliberately separate states.
/// </summary>
public sealed class VoicemeeterMqttService : IAsyncDisposable
{
    private readonly MqttConfig _config;
    private readonly VoicemeeterAPI _voicemeeter = new();
    private readonly AudioDeviceManager _audioManager = new();
    private readonly MqttPublisher _mqtt;
    private readonly RuntimeStateStore _stateStore;
    private readonly BridgeRuntimeState _runtimeState;
    private readonly ConcurrentDictionary<string, string> _lastPublished = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _reconcileLock = new(1, 1);
    private readonly SemaphoreSlim _haBirthSignal = new(0, 1);
    private readonly Channel<InboundMqttMessage> _inboundMessages = Channel.CreateBounded<InboundMqttMessage>(
        new BoundedChannelOptions(128)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });
    private readonly object _retrySync = new();

    private CancellationTokenSource? _cancellationSource;
    private Task? _voicemeeterLoopTask;
    private Task? _audioLoopTask;
    private Task? _voicemeeterRetryTask;
    private Task? _audioRetryTask;
    private Task? _haBirthTask;
    private Task? _mqttMessageTask;
    private VoicemeeterTopology _topology;
    private HashSet<string> _audioDeviceSignatures = new(StringComparer.Ordinal);
    private string _voicemeeterDiscoverySignature = string.Empty;
    private volatile bool _voicemeeterReady;
    private volatile bool _audioReady;
    private bool _disposed;

    private string BaseTopic => _config.EffectiveBaseTopic;
    private string ServiceStatusTopic => $"{BaseTopic}/status";
    private string VoicemeeterStatusTopic => $"{BaseTopic}/voicemeeter/status";
    private string AudioStatusTopic => $"{BaseTopic}/audio/status";

    public VoicemeeterMqttService(MqttConfig config, string appDataPath)
    {
        _config = config;
        _mqtt = new MqttPublisher(config);
        _stateStore = new RuntimeStateStore(appDataPath);
        _runtimeState = _stateStore.Load();
    }

    public Task StartAsync()
    {
        if (_cancellationSource != null)
            return Task.CompletedTask;

        _cancellationSource = new CancellationTokenSource();
        var token = _cancellationSource.Token;

        _mqtt.OnMessageReceived = EnqueueMqttMessageAsync;
        _mqtt.OnConnectedReconcileAsync = ReconcileMqttAsync;
        _mqtt.OnDisconnectedAsync = () =>
        {
            _lastPublished.Clear();
            return Task.CompletedTask;
        };

        _voicemeeterReady = TryInitializeVoicemeeter();
        if (!_voicemeeterReady)
        {
            Log.Warning("Voicemeeter is unavailable; Windows audio monitoring will continue and Voicemeeter initialization will be retried");
            EnsureVoicemeeterRetryRunning(token);
        }

        _audioReady = _audioManager.Initialize();
        if (!_audioReady)
        {
            Log.Warning("Windows audio monitoring is unavailable; initialization will be retried");
            EnsureAudioRetryRunning(token);
        }
        else
        {
            _audioDeviceSignatures = CurrentAudioDeviceSignatures();
        }

        _voicemeeterLoopTask = VoicemeeterLoopAsync(token);
        _audioLoopTask = AudioLoopAsync(token);
        _haBirthTask = HomeAssistantBirthLoopAsync(token);
        _mqttMessageTask = MqttMessageLoopAsync(token);
        _ = _mqtt.StartAsync(token);

        Log.Information("VoicemeeterMqttService started for instance {InstanceId} using base topic {BaseTopic}",
            _config.InstanceId, BaseTopic);
        return Task.CompletedTask;
    }

    private bool TryInitializeVoicemeeter()
    {
        if (!_voicemeeter.Initialize())
            return false;

        var topology = _voicemeeter.GetTopology();
        if (topology is null || topology.Value.StripCount <= 0 || topology.Value.BusCount <= 0)
        {
            Log.Warning("Voicemeeter API initialized but topology could not be determined");
            _voicemeeter.Dispose();
            return false;
        }

        _topology = topology.Value;
        _voicemeeterDiscoverySignature = CurrentVoicemeeterDiscoverySignature();
        Log.Information("Detected Voicemeeter type {Type}: {Strips} strips, {Buses} buses",
            _topology.Type, _topology.StripCount, _topology.BusCount);
        return true;
    }

    private async Task<bool> ReconcileMqttAsync(CancellationToken cancellationToken)
    {
        await _reconcileLock.WaitAsync(cancellationToken);
        try
        {
            _lastPublished.Clear();

            // An old retained online value may still exist from a previous version or unclean
            // deployment. Keep the public service availability offline until subscriptions,
            // discovery and the complete authoritative snapshot have all reconciled successfully.
            if (!await _mqtt.PublishAsync(
                    ServiceStatusTopic,
                    "offline",
                    retain: true,
                    qos: MqttQualityOfServiceLevel.AtLeastOnce,
                    cancellationToken: cancellationToken))
                return false;

            if (!await _mqtt.SubscribeAsync($"{BaseTopic}/+/+/set", MqttQualityOfServiceLevel.AtLeastOnce, cancellationToken))
                return false;
            if (!await _mqtt.SubscribeAsync(_config.HomeAssistantStatusTopic, MqttQualityOfServiceLevel.AtLeastOnce, cancellationToken))
                return false;

            if (!await PublishBackendAvailabilityAsync(cancellationToken))
                return false;
            if (!await ReconcileDiscoveryAndRetainedTopicsAsync(cancellationToken))
                return false;
            if (!await PublishFullSnapshotAsync(cancellationToken))
                return false;

            // Service availability is published last. Until this succeeds HA should not consider
            // entities ready, even if discovery and backend-specific status already exist.
            return await _mqtt.PublishAsync(
                ServiceStatusTopic,
                "online",
                retain: true,
                qos: MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);
        }
        finally
        {
            _reconcileLock.Release();
        }
    }

    private async Task<bool> PublishBackendAvailabilityAsync(CancellationToken cancellationToken)
    {
        var vm = await _mqtt.PublishAsync(
            VoicemeeterStatusTopic,
            _voicemeeterReady ? "online" : "offline",
            retain: true,
            qos: MqttQualityOfServiceLevel.AtLeastOnce,
            cancellationToken: cancellationToken);
        var audio = await _mqtt.PublishAsync(
            AudioStatusTopic,
            _audioReady ? "online" : "offline",
            retain: true,
            qos: MqttQualityOfServiceLevel.AtLeastOnce,
            cancellationToken: cancellationToken);
        return vm && audio;
    }

    private async Task<bool> ReconcileDiscoveryAndRetainedTopicsAsync(CancellationToken cancellationToken)
    {
        var desiredDiscovery = new Dictionary<string, string>(StringComparer.Ordinal);
        var desiredRetained = new HashSet<string>(StringComparer.Ordinal)
        {
            ServiceStatusTopic,
            VoicemeeterStatusTopic,
            AudioStatusTopic
        };

        if (_voicemeeterReady)
        {
            var vmTopic = $"{_config.DiscoveryPrefix}/device/voicemeeter-{_config.InstanceId}/config";
            desiredDiscovery[vmTopic] = BuildVoicemeeterDiscoveryJson();

            for (var i = 0; i < _topology.StripCount; i++)
            {
                desiredRetained.Add($"{BaseTopic}/strip{i}/gain");
                desiredRetained.Add($"{BaseTopic}/strip{i}/muted");
            }
            for (var i = 0; i < _topology.BusCount; i++)
            {
                desiredRetained.Add($"{BaseTopic}/bus{i}/gain");
                desiredRetained.Add($"{BaseTopic}/bus{i}/muted");
            }
        }

        if (_audioReady)
        {
            foreach (var device in _audioManager.GetDevices())
            {
                var key = AudioDeviceKey(device);
                var discoveryTopic = $"{_config.DiscoveryPrefix}/device/windows-audio-{_config.InstanceId}-{key}/config";
                desiredDiscovery[discoveryTopic] = BuildAudioDiscoveryJson(device, key);

                var path = AudioDevicePath(device, key);
                if (device.Volume.HasValue)
                    desiredRetained.Add($"{path}/volume");
                if (device.IsMuted.HasValue)
                    desiredRetained.Add($"{path}/muted");
                desiredRetained.Add($"{path}/default");
            }
        }

        var allOk = true;

        // Topic/identity changes are authoritative even when a backend is currently offline.
        // Cleanup is best-effort: inability to delete an obsolete retained topic must not take the
        // live bridge offline. Failed tombstones stay in the persistent registry and are retried.
        var namespaceChanged =
            (!string.IsNullOrEmpty(_runtimeState.BaseTopic) && !string.Equals(_runtimeState.BaseTopic, BaseTopic, StringComparison.Ordinal)) ||
            (!string.IsNullOrEmpty(_runtimeState.DiscoveryPrefix) && !string.Equals(_runtimeState.DiscoveryPrefix, _config.DiscoveryPrefix, StringComparison.Ordinal)) ||
            (!string.IsNullOrEmpty(_runtimeState.InstanceId) && !string.Equals(_runtimeState.InstanceId, _config.InstanceId, StringComparison.Ordinal));

        if (!_runtimeState.LegacyDiscoveryCleaned)
        {
            var legacyCleanupOk = await CleanupLegacyDiscoveryAsync(cancellationToken);
            if (legacyCleanupOk)
                _runtimeState.LegacyDiscoveryCleaned = true;
            else
                Log.Warning("Some legacy Home Assistant discovery topics could not be tombstoned; cleanup will be retried");
        }

        if (!_runtimeState.LegacyRetainedCleaned)
        {
            var legacyRetainedCleanupOk = await CleanupLegacyRetainedStateAsync(cancellationToken);
            if (legacyRetainedCleanupOk)
                _runtimeState.LegacyRetainedCleaned = true;
            else
                Log.Warning("Some legacy retained state topics could not be tombstoned; cleanup will be retried");
        }

        if (namespaceChanged)
        {
            var failedDiscoveryCleanup = new List<string>();
            foreach (var oldTopic in _runtimeState.DiscoveryTopics)
            {
                if (!await ClearRetainedAsync(oldTopic, cancellationToken))
                    failedDiscoveryCleanup.Add(oldTopic);
            }

            var failedRetainedCleanup = new List<string>();
            foreach (var oldTopic in _runtimeState.RetainedTopics)
            {
                if (!await ClearRetainedAsync(oldTopic, cancellationToken))
                    failedRetainedCleanup.Add(oldTopic);
            }

            _runtimeState.DiscoveryTopics = failedDiscoveryCleanup;
            _runtimeState.RetainedTopics = failedRetainedCleanup;
        }
        else
        {
            // Clean stale entries independently for each backend that is currently authoritative.
            // Entries belonging to an unavailable backend stay in the registry until that backend can
            // tell us what should exist, avoiding accidental deletion during transient failures.
            foreach (var stale in _runtimeState.DiscoveryTopics.Except(desiredDiscovery.Keys, StringComparer.Ordinal).ToArray())
            {
                if (!CanCleanDiscoveryTopic(stale) || !await ClearRetainedAsync(stale, cancellationToken))
                    desiredDiscovery[stale] = string.Empty; // registry-only preservation/retry
            }

            foreach (var stale in _runtimeState.RetainedTopics.Except(desiredRetained, StringComparer.Ordinal).ToArray())
            {
                if (!CanCleanRetainedTopic(stale) || !await ClearRetainedAsync(stale, cancellationToken))
                    desiredRetained.Add(stale);
            }
        }

        // Preserve failed namespace-cleanup tombstones for a later retry without republishing them.
        foreach (var oldTopic in _runtimeState.DiscoveryTopics)
            if (!desiredDiscovery.ContainsKey(oldTopic))
                desiredDiscovery[oldTopic] = string.Empty;
        foreach (var oldTopic in _runtimeState.RetainedTopics)
            desiredRetained.Add(oldTopic);

        foreach (var (topic, payload) in desiredDiscovery)
        {
            if (string.IsNullOrEmpty(payload))
                continue;
            allOk &= await _mqtt.PublishAsync(
                topic,
                payload,
                retain: true,
                qos: MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);
        }

        if (allOk)
        {
            _runtimeState.SchemaVersion = 3;
            _runtimeState.InstanceId = _config.InstanceId;
            _runtimeState.BaseTopic = BaseTopic;
            _runtimeState.DiscoveryPrefix = _config.DiscoveryPrefix;
            _runtimeState.DiscoveryTopics = desiredDiscovery.Keys.OrderBy(x => x, StringComparer.Ordinal).ToList();
            _runtimeState.RetainedTopics = desiredRetained.OrderBy(x => x, StringComparer.Ordinal).ToList();
            if (!_stateStore.Save(_runtimeState))
                Log.Warning("Runtime migration registry could not be persisted; bridge will continue and rebuild it on next start");
        }

        return allOk;
    }

    private bool CanCleanDiscoveryTopic(string topic)
    {
        // Anything outside the current discovery namespace/instance is known historical state from
        // our persisted ownership registry and is always safe to tombstone.
        var currentPrefix = _config.DiscoveryPrefix + "/device/";
        if (!topic.StartsWith(currentPrefix, StringComparison.Ordinal) ||
            !topic.Contains($"-{_config.InstanceId}", StringComparison.Ordinal))
            return true;

        if (topic.Contains("/device/voicemeeter-", StringComparison.Ordinal))
            return _voicemeeterReady;
        if (topic.Contains("/device/windows-audio-", StringComparison.Ordinal))
            return _audioReady;
        return _voicemeeterReady && _audioReady;
    }

    private bool CanCleanRetainedTopic(string topic)
    {
        // Retained topics outside the current base namespace can only be leftovers from a previous
        // configuration recorded in our ownership registry; do not wait for a backend to recover.
        if (!topic.StartsWith(BaseTopic + "/", StringComparison.Ordinal))
            return true;

        if (topic.Contains("/audio/", StringComparison.Ordinal))
            return _audioReady;
        if (topic.Contains("/strip", StringComparison.Ordinal) || topic.Contains("/bus", StringComparison.Ordinal))
            return _voicemeeterReady;
        return true;
    }

    private async Task<bool> CleanupLegacyDiscoveryAsync(CancellationToken cancellationToken)
    {
        var prefixes = new HashSet<string>(StringComparer.Ordinal) { "homeassistant", _config.DiscoveryPrefix };
        var allOk = true;
        foreach (var prefix in prefixes)
        {
            for (var i = 0; i < 8; i++)
            {
                allOk &= await ClearRetainedAsync($"{prefix}/number/voicemeeter/strip{i}_gain/config", cancellationToken);
                allOk &= await ClearRetainedAsync($"{prefix}/switch/voicemeeter/strip{i}_mute/config", cancellationToken);
                allOk &= await ClearRetainedAsync($"{prefix}/number/voicemeeter/bus{i}_gain/config", cancellationToken);
                allOk &= await ClearRetainedAsync($"{prefix}/switch/voicemeeter/bus{i}_mute/config", cancellationToken);
            }
        }
        return allOk;
    }

    private async Task<bool> CleanupLegacyRetainedStateAsync(CancellationToken cancellationToken)
    {
        // The uploaded v1 application used BaseTopic directly, before this bridge introduced the
        // per-installation EffectiveBaseTopic. Tombstone the finite state namespace it could have
        // created so upgrades do not leave stale retained availability/gain/mute values behind.
        var legacyBase = _config.BaseTopic;
        var allOk = await ClearRetainedAsync($"{legacyBase}/status", cancellationToken);

        for (var i = 0; i < 8; i++)
        {
            allOk &= await ClearRetainedAsync($"{legacyBase}/strip{i}/gain", cancellationToken);
            allOk &= await ClearRetainedAsync($"{legacyBase}/strip{i}/muted", cancellationToken);
            allOk &= await ClearRetainedAsync($"{legacyBase}/bus{i}/gain", cancellationToken);
            allOk &= await ClearRetainedAsync($"{legacyBase}/bus{i}/muted", cancellationToken);
        }

        // v1 hard-coded the root 'voicemeeter/audio' for Windows audio state. We can safely clear
        // entries for endpoints that still exist. Removed historical endpoint IDs cannot be
        // enumerated from MQTT itself without a broker-wide retained scan, so they are intentionally
        // left alone rather than risk deleting another publisher's namespace.
        if (_audioReady)
        {
            foreach (var device in _audioManager.GetDevices())
            {
                var escapedId = Uri.EscapeDataString(device.Id);
                var legacyPath = $"voicemeeter/audio/{device.Type}/{escapedId}";
                allOk &= await ClearRetainedAsync($"{legacyPath}/volume", cancellationToken);
                allOk &= await ClearRetainedAsync($"{legacyPath}/muted", cancellationToken);
                allOk &= await ClearRetainedAsync($"{legacyPath}/peak", cancellationToken);
                allOk &= await ClearRetainedAsync($"{legacyPath}/default", cancellationToken);
            }
        }

        return allOk;
    }

    private Task<bool> ClearRetainedAsync(string topic, CancellationToken cancellationToken) =>
        _mqtt.PublishAsync(
            topic,
            string.Empty,
            retain: true,
            qos: MqttQualityOfServiceLevel.AtLeastOnce,
            cancellationToken: cancellationToken);

    private string BuildVoicemeeterDiscoveryJson()
    {
        var components = new Dictionary<string, object?>(StringComparer.Ordinal);
        for (var i = 0; i < _topology.StripCount; i++)
        {
            var label = _voicemeeter.GetParameterString($"Strip[{i}].Label") ?? $"Strip {i}";
            components[$"strip{i}_gain"] = NumberComponent(
                $"{label} Gain", $"{BaseTopic}/strip{i}/gain", $"{BaseTopic}/strip{i}/gain/set",
                $"{_config.InstanceId}_strip{i}_gain", "mdi:import");
            components[$"strip{i}_mute"] = SwitchComponent(
                $"{label} Mute", $"{BaseTopic}/strip{i}/muted", $"{BaseTopic}/strip{i}/muted/set",
                $"{_config.InstanceId}_strip{i}_mute", "mdi:microphone-off");
        }

        for (var i = 0; i < _topology.BusCount; i++)
        {
            var label = _voicemeeter.GetParameterString($"Bus[{i}].Label") ?? $"Bus {i}";
            components[$"bus{i}_gain"] = NumberComponent(
                $"Output {label} Gain", $"{BaseTopic}/bus{i}/gain", $"{BaseTopic}/bus{i}/gain/set",
                $"{_config.InstanceId}_bus{i}_gain", "mdi:export");
            components[$"bus{i}_mute"] = SwitchComponent(
                $"Output {label} Mute", $"{BaseTopic}/bus{i}/muted", $"{BaseTopic}/bus{i}/muted/set",
                $"{_config.InstanceId}_bus{i}_mute", "mdi:volume-mute");
        }

        var root = new Dictionary<string, object?>
        {
            ["device"] = new Dictionary<string, object?>
            {
                ["identifiers"] = new[] { $"voicemeeter-{_config.InstanceId}" },
                ["name"] = $"Voicemeeter ({Environment.MachineName})",
                ["manufacturer"] = "VB-Audio",
                ["model"] = _topology.Type switch { 1 => "Voicemeeter", 2 => "Voicemeeter Banana", 3 => "Voicemeeter Potato", _ => "Voicemeeter" }
            },
            ["origin"] = Origin(),
            ["components"] = components
        };
        return JsonSerializer.Serialize(root);
    }

    private Dictionary<string, object?> NumberComponent(string name, string stateTopic, string commandTopic, string uniqueId, string icon) =>
        new()
        {
            ["platform"] = "number",
            ["has_entity_name"] = true,
            ["name"] = name,
            ["unique_id"] = uniqueId,
            ["state_topic"] = stateTopic,
            ["command_topic"] = commandTopic,
            ["min"] = ProtocolValues.MinGainDb,
            ["max"] = ProtocolValues.MaxGainDb,
            ["step"] = ProtocolValues.GainStepDb,
            ["mode"] = "slider",
            ["unit_of_measurement"] = "dB",
            ["icon"] = icon,
            ["qos"] = 1,
            ["availability"] = BackendAvailability(VoicemeeterStatusTopic),
            ["availability_mode"] = "all"
        };

    private Dictionary<string, object?> SwitchComponent(string name, string stateTopic, string commandTopic, string uniqueId, string icon) =>
        new()
        {
            ["platform"] = "switch",
            ["has_entity_name"] = true,
            ["name"] = name,
            ["unique_id"] = uniqueId,
            ["state_topic"] = stateTopic,
            ["command_topic"] = commandTopic,
            ["payload_on"] = "1",
            ["payload_off"] = "0",
            ["state_on"] = "1",
            ["state_off"] = "0",
            ["icon"] = icon,
            ["qos"] = 1,
            ["availability"] = BackendAvailability(VoicemeeterStatusTopic),
            ["availability_mode"] = "all"
        };

    private string BuildAudioDiscoveryJson(AudioDevice device, string key)
    {
        var path = AudioDevicePath(device, key);
        var availability = BackendAvailability(AudioStatusTopic);
        var components = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (device.Volume.HasValue)
        {
            components["volume"] = new Dictionary<string, object?>
            {
                ["platform"] = "sensor", ["has_entity_name"] = true, ["name"] = "Volume", ["unique_id"] = $"{_config.InstanceId}_{key}_volume",
                ["state_topic"] = $"{path}/volume", ["unit_of_measurement"] = "%", ["state_class"] = "measurement",
                ["qos"] = 1, ["availability"] = availability, ["availability_mode"] = "all"
            };
        }
        if (device.IsMuted.HasValue)
        {
            components["muted"] = new Dictionary<string, object?>
            {
                ["platform"] = "binary_sensor", ["has_entity_name"] = true, ["name"] = "Muted", ["unique_id"] = $"{_config.InstanceId}_{key}_muted",
                ["state_topic"] = $"{path}/muted", ["payload_on"] = "true", ["payload_off"] = "false",
                ["qos"] = 1, ["availability"] = availability, ["availability_mode"] = "all"
            };
        }
        if (device.PeakLevel.HasValue)
        {
            components["peak"] = new Dictionary<string, object?>
            {
                ["platform"] = "sensor", ["has_entity_name"] = true, ["name"] = "Peak level", ["unique_id"] = $"{_config.InstanceId}_{key}_peak",
                ["state_topic"] = $"{path}/peak", ["unit_of_measurement"] = "%", ["state_class"] = "measurement",
                ["qos"] = 0, ["availability"] = availability, ["availability_mode"] = "all"
            };
        }
        components["default"] = new Dictionary<string, object?>
        {
            ["platform"] = "binary_sensor", ["has_entity_name"] = true, ["name"] = "Default device", ["unique_id"] = $"{_config.InstanceId}_{key}_default",
            ["state_topic"] = $"{path}/default", ["payload_on"] = "true", ["payload_off"] = "false",
            ["entity_category"] = "diagnostic", ["qos"] = 1, ["availability"] = availability, ["availability_mode"] = "all"
        };

        var root = new Dictionary<string, object?>
        {
            ["device"] = new Dictionary<string, object?>
            {
                ["identifiers"] = new[] { $"windows-audio-{_config.InstanceId}-{key}" },
                ["name"] = device.Name,
                ["manufacturer"] = "Microsoft Windows",
                ["model"] = device.Type == "playback" ? "Audio Playback Endpoint" : "Audio Capture Endpoint"
            },
            ["origin"] = Origin(),
            ["components"] = components
        };
        return JsonSerializer.Serialize(root);
    }

    private Dictionary<string, object?> Origin() => new()
    {
        ["name"] = "VoicemeeterMQTT",
        ["sw_version"] = typeof(VoicemeeterMqttService).Assembly.GetName().Version?.ToString() ?? "unknown"
    };

    private object[] BackendAvailability(string backendTopic) =>
        new object[]
        {
            new Dictionary<string, object?> { ["topic"] = ServiceStatusTopic, ["payload_available"] = "online", ["payload_not_available"] = "offline" },
            new Dictionary<string, object?> { ["topic"] = backendTopic, ["payload_available"] = "online", ["payload_not_available"] = "offline" }
        };

    private async Task<bool> PublishFullSnapshotAsync(CancellationToken cancellationToken)
    {
        var allOk = true;
        if (_voicemeeterReady)
        {
            var vmSnapshotOk = await CheckVoicemeeterStateAsync(force: true, cancellationToken);
            allOk &= vmSnapshotOk;
            if (!vmSnapshotOk)
                await MarkVoicemeeterUnavailableAsync("authoritative state snapshot failed", cancellationToken);
        }
        if (_audioReady)
            allOk &= await PublishAudioStateAsync(force: true, cancellationToken);
        return allOk;
    }

    private async Task VoicemeeterLoopAsync(CancellationToken cancellationToken)
    {
        var interval = TimeSpan.FromMilliseconds(_config.UpdateIntervalMs);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var poll = _voicemeeter.Poll();
                if (poll == VoicemeeterPollResult.Unavailable)
                {
                    if (_voicemeeterReady)
                    {
                        _voicemeeterReady = false;
                        if (_mqtt.IsConnected)
                            await _mqtt.PublishAsync(VoicemeeterStatusTopic, "offline", true, MqttQualityOfServiceLevel.AtLeastOnce, cancellationToken);
                    }
                    EnsureVoicemeeterRetryRunning(cancellationToken);
                }
                else if (poll == VoicemeeterPollResult.Changed)
                {
                    var discoverySignature = CurrentVoicemeeterDiscoverySignature();
                    var discoveryChanged = !string.Equals(
                        discoverySignature, _voicemeeterDiscoverySignature, StringComparison.Ordinal);
                    if (discoveryChanged)
                    {
                        _voicemeeterDiscoverySignature = discoverySignature;
                        if (_mqtt.IsReady)
                            _mqtt.RequestReconciliation("Voicemeeter discovery metadata changed");
                    }
                    else if (_mqtt.IsReady &&
                             !await CheckVoicemeeterStateAsync(force: false, cancellationToken))
                    {
                        await MarkVoicemeeterUnavailableAsync(
                            "parameter read failed after a change notification", cancellationToken);
                    }
                }

                await Task.Delay(interval, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error in Voicemeeter update loop");
                await Task.Delay(interval, cancellationToken);
            }
        }
    }

    private async Task AudioLoopAsync(CancellationToken cancellationToken)
    {
        var interval = TimeSpan.FromMilliseconds(_config.AudioUpdateIntervalMs);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (!_audioManager.RefreshDevices())
                {
                    if (_audioReady)
                    {
                        _audioReady = false;
                        if (_mqtt.IsConnected)
                            await _mqtt.PublishAsync(AudioStatusTopic, "offline", true, MqttQualityOfServiceLevel.AtLeastOnce, cancellationToken);
                    }
                    EnsureAudioRetryRunning(cancellationToken);
                }
                else
                {
                    var currentSignatures = CurrentAudioDeviceSignatures();
                    var topologyChanged = !_audioDeviceSignatures.SetEquals(currentSignatures);
                    _audioDeviceSignatures = currentSignatures;
                    _audioReady = true;

                    if (_mqtt.IsReady)
                    {
                        if (topologyChanged)
                            await ReconcileBackendChangeAsync(cancellationToken);
                        else
                            await PublishAudioStateAsync(force: false, cancellationToken);
                    }
                }

                await Task.Delay(interval, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error in Windows audio update loop");
                await Task.Delay(interval, cancellationToken);
            }
        }
    }

    private async Task<bool> CheckVoicemeeterStateAsync(bool force, CancellationToken cancellationToken)
    {
        var allOk = true;
        for (var i = 0; i < _topology.StripCount; i++)
        {
            var gain = _voicemeeter.GetParameter($"Strip[{i}].Gain");
            if (gain.HasValue)
                allOk &= await PublishStateAsync($"{BaseTopic}/strip{i}/gain", ProtocolValues.FormatFloat(gain.Value), true, MqttQualityOfServiceLevel.AtLeastOnce, force, cancellationToken);
            else
                allOk = false;

            var mute = _voicemeeter.GetParameter($"Strip[{i}].Mute");
            if (mute.HasValue)
                allOk &= await PublishStateAsync($"{BaseTopic}/strip{i}/muted", mute.Value >= 0.5f ? "1" : "0", true, MqttQualityOfServiceLevel.AtLeastOnce, force, cancellationToken);
            else
                allOk = false;
        }

        for (var i = 0; i < _topology.BusCount; i++)
        {
            var gain = _voicemeeter.GetParameter($"Bus[{i}].Gain");
            if (gain.HasValue)
                allOk &= await PublishStateAsync($"{BaseTopic}/bus{i}/gain", ProtocolValues.FormatFloat(gain.Value), true, MqttQualityOfServiceLevel.AtLeastOnce, force, cancellationToken);
            else
                allOk = false;

            var mute = _voicemeeter.GetParameter($"Bus[{i}].Mute");
            if (mute.HasValue)
                allOk &= await PublishStateAsync($"{BaseTopic}/bus{i}/muted", mute.Value >= 0.5f ? "1" : "0", true, MqttQualityOfServiceLevel.AtLeastOnce, force, cancellationToken);
            else
                allOk = false;
        }
        return allOk;
    }

    private async Task<bool> PublishAudioStateAsync(bool force, CancellationToken cancellationToken)
    {
        var allOk = true;
        foreach (var device in _audioManager.GetDevices())
        {
            var key = AudioDeviceKey(device);
            var path = AudioDevicePath(device, key);

            if (device.Volume.HasValue)
                allOk &= await PublishStateAsync($"{path}/volume", ProtocolValues.FormatFloat((float)device.Volume.Value), true, MqttQualityOfServiceLevel.AtLeastOnce, force, cancellationToken);
            if (device.IsMuted.HasValue)
                allOk &= await PublishStateAsync($"{path}/muted", device.IsMuted.Value ? "true" : "false", true, MqttQualityOfServiceLevel.AtLeastOnce, force, cancellationToken);
            if (device.PeakLevel.HasValue)
                allOk &= await PublishStateAsync($"{path}/peak", ProtocolValues.FormatFloat(device.PeakLevel.Value * 100.0f), false, MqttQualityOfServiceLevel.AtMostOnce, force, cancellationToken);

            // Publish both true and false so the previous default endpoint cannot stay retained=true.
            allOk &= await PublishStateAsync($"{path}/default", device.IsDefault ? "true" : "false", true, MqttQualityOfServiceLevel.AtLeastOnce, force, cancellationToken);
        }
        return allOk;
    }

    private async Task<bool> PublishStateAsync(
        string topic,
        string payload,
        bool retain,
        MqttQualityOfServiceLevel qos,
        bool force,
        CancellationToken cancellationToken)
    {
        if (!force && _lastPublished.TryGetValue(topic, out var previous) && string.Equals(previous, payload, StringComparison.Ordinal))
            return true;
        if (!force && !_mqtt.IsReady)
            return false;

        var published = await _mqtt.PublishAsync(topic, payload, retain, qos, cancellationToken);
        if (published)
        {
            _lastPublished[topic] = payload;
        }
        else if (_mqtt.IsConnected)
        {
            // A live connection can still reject/drop a publish. Do not wait for the source value
            // to change again: force a complete reconciliation of authoritative state.
            _mqtt.RequestReconciliation($"state publish failed while connected: {topic}");
        }
        return published;
    }

    private async Task EnqueueMqttMessageAsync(string topic, string payload, bool retained)
    {
        // MQTTnet awaits receive callbacks. A bounded queue provides backpressure instead of silently
        // discarding QoS 1 control messages when command processing temporarily falls behind.
        var cancellationToken = _cancellationSource?.Token ?? CancellationToken.None;
        try
        {
            await _inboundMessages.Writer.WriteAsync(
                new InboundMqttMessage(topic, payload, retained), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Service is stopping.
        }
        catch (ChannelClosedException)
        {
            // Service is stopping.
        }
    }

    private async Task MqttMessageLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in _inboundMessages.Reader.ReadAllAsync(cancellationToken))
            {
                try
                {
                    await HandleMqttMessageAsync(message.Topic, message.Payload, message.Retained);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error processing MQTT message on {Topic}", message.Topic);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task HandleMqttMessageAsync(string topic, string payload, bool retained)
    {
        if (string.Equals(topic, _config.HomeAssistantStatusTopic, StringComparison.Ordinal))
        {
            if (string.Equals(payload.Trim(), "online", StringComparison.OrdinalIgnoreCase))
                SignalHomeAssistantBirth();
            return;
        }

        if (!MqttCommandParser.TryParse(BaseTopic, topic, out var target))
            return;

        // Subscriptions become active during post-connect reconciliation. Do not execute commands
        // until discovery/state have converged and service availability has been published online.
        if (!_mqtt.IsReady)
        {
            Log.Debug("Ignoring command on {Topic} while MQTT reconciliation is still in progress", topic);
            return;
        }

        if (retained)
        {
            Log.Warning("Ignoring retained command on {Topic}; command topics must never be retained", topic);
            return;
        }

        if (!_voicemeeterReady)
        {
            Log.Warning("Ignoring command on {Topic} because Voicemeeter is unavailable", topic);
            return;
        }

        var max = target.Kind == "strip" ? _topology.StripCount : _topology.BusCount;
        if (target.Index >= max)
        {
            Log.Warning("Ignoring command for nonexistent {Kind} index {Index}", target.Kind, target.Index);
            return;
        }

        if (target.Property == "gain")
        {
            if (!ProtocolValues.TryParseGain(payload, out var gain))
            {
                Log.Warning("Rejected invalid gain payload '{Payload}' on {Topic}", payload, topic);
                return;
            }

            await ApplyAndPublishActualAsync(target, gain);
        }
        else if (target.Property == "muted")
        {
            if (!ProtocolValues.TryParseMute(payload, out var muteValue))
            {
                Log.Warning("Rejected invalid mute payload '{Payload}' on {Topic}", payload, topic);
                return;
            }

            await ApplyAndPublishActualAsync(target, muteValue);
        }
    }

    private async Task ApplyAndPublishActualAsync(MqttCommandTarget target, float requested)
    {
        var prefix = target.Kind == "strip" ? "Strip" : "Bus";
        var property = target.Property == "gain" ? "Gain" : "Mute";
        var parameter = $"{prefix}[{target.Index}].{property}";
        if (!_voicemeeter.SetParameter(parameter, requested))
        {
            Log.Warning("Voicemeeter rejected command {Parameter}={Value}", parameter, requested);
            await MarkVoicemeeterUnavailableAsync("command write failed", _cancellationSource?.Token ?? CancellationToken.None);
            return;
        }

        // Voicemeeter applies parameter writes asynchronously. An immediate GetParameter() can
        // still return the previous value, which makes Home Assistant briefly snap back to the
        // first slider position when several commands arrive quickly. A successful SetParameter
        // means the command was accepted, so publish the requested value immediately for responsive
        // UI feedback. The normal dirty/poll loop remains authoritative and will correct the state
        // if Voicemeeter clamps or otherwise changes the applied value.
        var topic = $"{BaseTopic}/{target.Kind}{target.Index}/{target.Property}";
        var payload = target.Property == "muted"
            ? (requested >= 0.5f ? "1" : "0")
            : ProtocolValues.FormatFloat(requested);
        var token = _cancellationSource?.Token ?? CancellationToken.None;
        await PublishStateAsync(topic, payload, true, MqttQualityOfServiceLevel.AtLeastOnce, force: true, token);
    }

    private async Task MarkVoicemeeterUnavailableAsync(string reason, CancellationToken cancellationToken)
    {
        if (!_voicemeeterReady)
            return;

        _voicemeeterReady = false;
        _voicemeeter.Dispose();
        Log.Warning("Voicemeeter backend marked unavailable: {Reason}", reason);

        if (_mqtt.IsConnected)
        {
            await _mqtt.PublishAsync(
                VoicemeeterStatusTopic,
                "offline",
                retain: true,
                qos: MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);
            _mqtt.RequestReconciliation("Voicemeeter backend became unavailable");
        }

        if (_cancellationSource is { IsCancellationRequested: false })
            EnsureVoicemeeterRetryRunning(_cancellationSource.Token);
    }

    private void SignalHomeAssistantBirth()
    {
        try { _haBirthSignal.Release(); } catch (SemaphoreFullException) { }
    }

    private async Task HomeAssistantBirthLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _haBirthSignal.WaitAsync(cancellationToken);
                while (_haBirthSignal.Wait(0)) { }
                await Task.Delay(Random.Shared.Next(0, 5001), cancellationToken);

                if (_mqtt.IsReady)
                {
                    // Discovery is retained, so HA receives it from the broker. Birth is used to
                    // refresh authoritative current state, especially non-retained peak values.
                    var availabilityOk = await PublishBackendAvailabilityAsync(cancellationToken);
                    var snapshotOk = await PublishFullSnapshotAsync(cancellationToken);
                    if (!availabilityOk || !snapshotOk)
                        _mqtt.RequestReconciliation("Home Assistant birth refresh failed");
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error handling Home Assistant birth event");
            }
        }
    }

    private void EnsureVoicemeeterRetryRunning(CancellationToken cancellationToken)
    {
        lock (_retrySync)
        {
            if (_voicemeeterRetryTask is { IsCompleted: false })
                return;
            _voicemeeterRetryTask = RetryVoicemeeterInitAsync(cancellationToken);
        }
    }

    private async Task RetryVoicemeeterInitAsync(CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (!cancellationToken.IsCancellationRequested && !_voicemeeterReady)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                attempt++;
                if (TryInitializeVoicemeeter())
                {
                    _voicemeeterReady = true;
                    Log.Information("Voicemeeter backend recovered on retry {Attempt}", attempt);
                    if (_mqtt.IsReady)
                        await ReconcileBackendChangeAsync(cancellationToken);
                    return;
                }
                if (attempt % 12 == 0)
                    Log.Debug("Still waiting for Voicemeeter backend ({Attempt} retries)", attempt);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Unexpected error while retrying Voicemeeter initialization");
            }
        }
    }

    private void EnsureAudioRetryRunning(CancellationToken cancellationToken)
    {
        lock (_retrySync)
        {
            if (_audioRetryTask is { IsCompleted: false })
                return;
            _audioRetryTask = RetryAudioInitAsync(cancellationToken);
        }
    }

    private async Task RetryAudioInitAsync(CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (!cancellationToken.IsCancellationRequested && !_audioReady)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                attempt++;
                if (_audioManager.Initialize())
                {
                    _audioReady = true;
                    _audioDeviceSignatures = CurrentAudioDeviceSignatures();
                    Log.Information("Windows audio backend recovered on retry {Attempt}", attempt);
                    if (_mqtt.IsReady)
                        await ReconcileBackendChangeAsync(cancellationToken);
                    return;
                }
                if (attempt % 12 == 0)
                    Log.Debug("Still waiting for Windows audio backend ({Attempt} retries)", attempt);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Unexpected error while retrying Windows audio initialization");
            }
        }
    }

    private Task ReconcileBackendChangeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _mqtt.RequestReconciliation("backend availability/topology changed");
        return Task.CompletedTask;
    }

    private string CurrentVoicemeeterDiscoverySignature()
    {
        if (!_voicemeeter.IsInitialized)
            return string.Empty;

        var builder = new StringBuilder();
        builder.Append(_topology.Type).Append(':').Append(_topology.StripCount).Append(':').Append(_topology.BusCount);
        for (var i = 0; i < _topology.StripCount; i++)
            builder.Append("|s").Append(i).Append('=').Append(_voicemeeter.GetParameterString($"Strip[{i}].Label") ?? string.Empty);
        for (var i = 0; i < _topology.BusCount; i++)
            builder.Append("|b").Append(i).Append('=').Append(_voicemeeter.GetParameterString($"Bus[{i}].Label") ?? string.Empty);
        return builder.ToString();
    }

    private HashSet<string> CurrentAudioDeviceSignatures() =>
        _audioManager.GetDevices()
            .Select(d => $"{d.Type}:{d.Id}:{d.Name}:{d.Volume.HasValue}:{d.IsMuted.HasValue}:{d.PeakLevel.HasValue}")
            .ToHashSet(StringComparer.Ordinal);

    private string AudioDevicePath(AudioDevice device, string key) => $"{BaseTopic}/audio/{device.Type}/{key}";

    private static string AudioDeviceKey(AudioDevice device)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes($"{device.Type}:{device.Id}"));
        return Convert.ToHexString(bytes.AsSpan(0, 8)).ToLowerInvariant();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        Log.Information("Stopping VoicemeeterMqttService...");
        _cancellationSource?.Cancel();

        await AwaitBackgroundTask(_voicemeeterLoopTask);
        await AwaitBackgroundTask(_audioLoopTask);
        await AwaitBackgroundTask(_voicemeeterRetryTask);
        await AwaitBackgroundTask(_audioRetryTask);
        _inboundMessages.Writer.TryComplete();
        await AwaitBackgroundTask(_haBirthTask);
        await AwaitBackgroundTask(_mqttMessageTask);

        if (_mqtt.IsConnected)
        {
            await _mqtt.PublishAsync(VoicemeeterStatusTopic, "offline", true, MqttQualityOfServiceLevel.AtLeastOnce);
            await _mqtt.PublishAsync(AudioStatusTopic, "offline", true, MqttQualityOfServiceLevel.AtLeastOnce);
        }

        await _mqtt.DisposeAsync();
        _voicemeeter.Dispose();
        _audioManager.Dispose();
        VoicemeeterInterop.UnloadDll();

        _cancellationSource?.Dispose();
        _reconcileLock.Dispose();
        _haBirthSignal.Dispose();
        Log.Information("VoicemeeterMqttService stopped");
    }

    private static async Task AwaitBackgroundTask(Task? task)
    {
        if (task == null)
            return;
        try { await task; }
        catch (OperationCanceledException) { }
        catch (Exception ex) { Log.Error(ex, "Background task failed during shutdown"); }
    }

    private readonly record struct InboundMqttMessage(string Topic, string Payload, bool Retained);
}
