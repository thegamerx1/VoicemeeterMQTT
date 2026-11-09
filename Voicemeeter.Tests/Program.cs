using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using MQTTnet.Server;
using VoicemeeterMQTT;

static void Assert(bool condition, string message)
{
    if (!condition)
        throw new InvalidOperationException(message);
}

static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, string message)
{
    var deadline = DateTime.UtcNow + timeout;
    while (DateTime.UtcNow < deadline)
    {
        if (condition())
            return;

        await Task.Delay(50);
    }

    throw new TimeoutException(message);
}

static int FindFreeTcpPort()
{
    var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    try
    {
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
    finally
    {
        listener.Stop();
    }
}

static MqttServer CreateBroker(int port)
{
    var options = new MqttServerOptionsBuilder()
        .WithDefaultEndpoint()
        .WithDefaultEndpointBoundIPAddress(IPAddress.Loopback)
        .WithDefaultEndpointPort(port)
        .WithDefaultEndpointReuseAddress()
        .Build();

    return new MqttFactory().CreateMqttServer(options);
}

static async Task<IMqttClient> ConnectProbeAsync(int port, string clientId)
{
    var client = new MqttFactory().CreateMqttClient();
    var options = new MqttClientOptionsBuilder()
        .WithTcpServer("127.0.0.1", port)
        .WithClientId(clientId)
        .WithCleanSession(true)
        .Build();

    await client.ConnectAsync(options);
    return client;
}

static async Task DisconnectAndDisposeAsync(IMqttClient? client)
{
    if (client == null)
        return;

    try
    {
        if (client.IsConnected)
            await client.DisconnectAsync(new MqttClientDisconnectOptionsBuilder().Build());
    }
    finally
    {
        client.Dispose();
    }
}

// Protocol values must be locale-independent and strict.
var originalCulture = CultureInfo.CurrentCulture;
try
{
    CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("es-ES");
    Assert(ProtocolValues.FormatFloat(-12.5f) == "-12.5", "MQTT numeric formatting must be invariant.");
    Assert(ProtocolValues.TryParseGain("-12.5", out var gain) && Math.Abs(gain + 12.5f) < 0.001f, "Invariant gain parse failed.");
    Assert(!ProtocolValues.TryParseGain("-12,5", out _), "Comma decimal payload must not be accepted as protocol syntax.");
    Assert(!ProtocolValues.TryParseGain("NaN", out _), "NaN must be rejected.");
    Assert(!ProtocolValues.TryParseGain("-12.25", out _), "Gain values outside the advertised 0.5 dB step must be rejected.");
    Assert(ProtocolValues.TryParseMute("1", out var muteOn) && muteOn == 1.0f, "Mute ON parse failed.");
    Assert(ProtocolValues.TryParseMute("0", out var muteOff) && muteOff == 0.0f, "Mute OFF parse failed.");
    Assert(!ProtocolValues.TryParseMute("garbage", out _), "Invalid mute payload must be rejected.");
}
finally
{
    CultureInfo.CurrentCulture = originalCulture;
}

// Command topics must match exactly, never by substring or malformed path normalization.
Assert(MqttCommandParser.TryParse("voicemeeter/pc-1", "voicemeeter/pc-1/strip2/gain/set", out var command), "Valid command topic was rejected.");
Assert(command.Kind == "strip" && command.Index == 2 && command.Property == "gain", "Command topic parsed incorrectly.");
Assert(!MqttCommandParser.TryParse("voicemeeter/pc-1", "voicemeeter/pc-1/notstrip2/gain/set", out _), "Substring topic matching regression.");
Assert(!MqttCommandParser.TryParse("voicemeeter/pc-1", "voicemeeter/pc-1/strip2/gain/set/extra", out _), "Extra command path segment must be rejected.");
Assert(!MqttCommandParser.TryParse("voicemeeter/pc-1", "voicemeeter/pc-1/strip2//gain/set", out _), "Empty command path segment must be rejected.");
Assert(!MqttCommandParser.TryParse("voicemeeter/pc-1", "voicemeeter/pc-1/strip02/gain/set", out _), "Non-canonical command aliases must be rejected.");
Assert(MqttCommandParser.TryParse("voicemeeter/pc-1", "voicemeeter/pc-1/voicemeeter/restart/set", out var restartCommand),
    "Valid engine restart command topic was rejected.");
Assert(restartCommand.Kind == "voicemeeter" && restartCommand.Index == 0 && restartCommand.Property == "restart",
    "Engine restart command target parsed incorrectly.");
foreach (var malformedRestartTopic in new[]
{
    "voicemeeter/pc-1/notvoicemeeter/restart/set",
    "voicemeeter/pc-1/Voicemeeter/restart/set",
    "voicemeeter/pc-1/voicemeeter/restarted/set",
    "voicemeeter/pc-1/voicemeeter/restart/SET",
    "voicemeeter/pc-1/voicemeeter/restart/set/extra",
    "voicemeeter/pc-1/voicemeeter//restart/set",
    "voicemeeter/pc-1/voicemeeter/restart",
    "voicemeeter/other-pc/voicemeeter/restart/set"
})
{
    Assert(!MqttCommandParser.TryParse("voicemeeter/pc-1", malformedRestartTopic, out _),
        $"Malformed engine restart topic was accepted: {malformedRestartTopic}");
}

// Engine restart requests must be serialized through recovery and rate-limited even after failures.
var restartClock = new ManualTimeProvider();
var restartRequestCount = 0;
var restartRecoveryCount = 0;
var restartRecovery = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
var restartCoordinator = new AudioEngineRestartCoordinator(
    () => { Interlocked.Increment(ref restartRequestCount); return true; },
    token => { Interlocked.Increment(ref restartRecoveryCount); return restartRecovery.Task.WaitAsync(token); },
    restartClock,
    TimeSpan.FromSeconds(10));
var pendingRestart = restartCoordinator.TryRestartAsync();
Assert(pendingRestart != null, "First engine restart request must be accepted.");
await WaitUntilAsync(() => Volatile.Read(ref restartRecoveryCount) == 1, TimeSpan.FromSeconds(2), "Engine restart did not enter recovery.");
Assert(restartCoordinator.IsRestarting, "Engine restart must stay in progress while recovery is pending.");
restartClock.Advance(TimeSpan.FromSeconds(20));
Assert(restartCoordinator.TryRestartAsync() == null && Volatile.Read(ref restartRequestCount) == 1,
    "A duplicate restart must not write the native command while recovery is pending, even after the cooldown elapsed.");
restartRecovery.SetResult(true);
Assert(await pendingRestart! == AudioEngineRestartResult.Succeeded && !restartCoordinator.IsRestarting,
    "Recovered engine restart must succeed and release the in-progress guard.");
var nextRestart = restartCoordinator.TryRestartAsync();
Assert(nextRestart != null && await nextRestart == AudioEngineRestartResult.Succeeded,
    "A completed restart must allow another request once its cooldown elapsed.");
Assert(restartCoordinator.TryRestartAsync() == null, "A completed restart must enforce the cooldown from its accepted request.");
restartClock.Advance(TimeSpan.FromSeconds(10) - TimeSpan.FromTicks(1));
Assert(restartCoordinator.TryRestartAsync() == null, "Engine restart cooldown must remain active immediately before its boundary.");
restartClock.Advance(TimeSpan.FromTicks(1));
var boundaryRestart = restartCoordinator.TryRestartAsync();
Assert(boundaryRestart != null && await boundaryRestart == AudioEngineRestartResult.Succeeded && Volatile.Read(ref restartRequestCount) == 3,
    "Engine restart must become available exactly at the cooldown boundary and write once per accepted request.");

var failedRestartClock = new ManualTimeProvider();
var failedRestartRecoveryCount = 0;
var failedRestartCoordinator = new AudioEngineRestartCoordinator(
    () => false,
    _ => { failedRestartRecoveryCount++; return Task.FromResult(true); },
    failedRestartClock);
var failedRestart = failedRestartCoordinator.TryRestartAsync();
Assert(failedRestart != null && await failedRestart == AudioEngineRestartResult.RequestFailed,
    "Failed native restart request must report failure.");
Assert(failedRestartRecoveryCount == 0 && !failedRestartCoordinator.IsRestarting,
    "A failed native restart request must skip recovery and release the in-progress guard.");
Assert(failedRestartCoordinator.TryRestartAsync() == null, "Failed restart attempts must also enforce the cooldown.");
failedRestartClock.Advance(TimeSpan.FromSeconds(10));
var retriedFailedRestart = failedRestartCoordinator.TryRestartAsync();
Assert(retriedFailedRestart != null && await retriedFailedRestart == AudioEngineRestartResult.RequestFailed,
    "A failed restart must not permanently block later requests.");

var timedOutRestartCoordinator = new AudioEngineRestartCoordinator(() => true, _ => Task.FromResult(false));
var timedOutRestart = timedOutRestartCoordinator.TryRestartAsync();
Assert(timedOutRestart != null && await timedOutRestart == AudioEngineRestartResult.RecoveryTimedOut && !timedOutRestartCoordinator.IsRestarting,
    "Recovery timeout must report the correct result and release the in-progress guard.");

var cancelledRestartClock = new ManualTimeProvider();
var cancelledRestartRecovery = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
var cancelledRestartRecoveryStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
var cancelledRestartCoordinator = new AudioEngineRestartCoordinator(
    () => true,
    token => { cancelledRestartRecoveryStarted.TrySetResult(true); return cancelledRestartRecovery.Task.WaitAsync(token); },
    cancelledRestartClock);
using (var restartCancellation = new CancellationTokenSource())
{
    var cancelledRestart = cancelledRestartCoordinator.TryRestartAsync(restartCancellation.Token);
    Assert(cancelledRestart != null, "Cancellable restart request must be accepted.");
    await cancelledRestartRecoveryStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));
    restartCancellation.Cancel();
    var restartWasCancelled = false;
    try { await cancelledRestart!; } catch (OperationCanceledException) { restartWasCancelled = true; }
    Assert(restartWasCancelled && !cancelledRestartCoordinator.IsRestarting,
        "Restart cancellation must propagate and release the in-progress guard.");
}
cancelledRestartClock.Advance(TimeSpan.FromSeconds(10));
cancelledRestartRecovery.SetResult(true);
var restartAfterCancellation = cancelledRestartCoordinator.TryRestartAsync();
Assert(restartAfterCancellation != null && await restartAfterCancellation == AudioEngineRestartResult.Succeeded,
    "Cancelled recovery must allow a later successful restart after cooldown.");

var throwingRestartCoordinator = new AudioEngineRestartCoordinator(
    () => throw new InvalidOperationException("Injected native restart failure."),
    _ => Task.FromResult(true));
var restartExceptionObserved = false;
try
{
    var throwingRestart = throwingRestartCoordinator.TryRestartAsync();
    Assert(throwingRestart != null, "Throwing restart callback must still be treated as an accepted attempt.");
    await throwingRestart!;
}
catch (InvalidOperationException ex) when (ex.Message == "Injected native restart failure.")
{
    restartExceptionObserved = true;
}
Assert(restartExceptionObserved && !throwingRestartCoordinator.IsRestarting,
    "Restart callback exceptions must propagate and release the in-progress guard.");

// Identity and topic namespace must be stable and installation-specific.
var config = new MqttConfig
{
    BrokerHost = "localhost",
    InstanceId = "Test PC",
    BaseTopic = "voicemeeter",
    ClientId = "voicemeeter-service",
    SkipCertificateValidation = false
};
config.NormalizeAndValidate();
Assert(config.InstanceId == "test-pc", "Instance ID sanitization failed.");
Assert(config.EffectiveBaseTopic == "voicemeeter/test-pc", "Per-instance base topic was not applied.");
Assert(config.EffectiveClientId.Length <= 23, "Effective MQTT client ID must remain compatible with strict MQTT 3.1.1 brokers.");
Assert(config.EffectiveClientId.StartsWith("voicemeeter", StringComparison.Ordinal), "Long MQTT client ID did not preserve a recognizable stable prefix.");
Assert(config.EffectiveClientId.All(char.IsAsciiLetterOrDigit), "Effective MQTT client ID must use the strict MQTT 3.1.1 alphanumeric compatibility set.");
var sameIdentity = new MqttConfig { BrokerHost = "localhost", InstanceId = "Test PC", ClientId = "voicemeeter-service" };
sameIdentity.NormalizeAndValidate();
Assert(sameIdentity.EffectiveClientId == config.EffectiveClientId, "Effective MQTT client ID is not deterministic for the same installation identity.");
var otherIdentity = new MqttConfig { BrokerHost = "localhost", InstanceId = "Other PC", ClientId = "voicemeeter-service" };
otherIdentity.NormalizeAndValidate();
Assert(otherIdentity.EffectiveClientId != config.EffectiveClientId, "Different installations generated the same effective MQTT client ID.");

var legacyConfig = new MqttConfig
{
    ConfigVersion = 0,
    BrokerHost = "broker.local",
    BrokerPort = 8883,
    InstanceId = "legacy",
    Username = "mqtt_user",
    Password = "mqtt_password",
    UseEncryption = false,
    SkipCertificateValidation = true
};
Assert(legacyConfig.NormalizeAndValidate(), "Legacy config should require migration.");
Assert(legacyConfig.ConfigVersion == 2 && legacyConfig.UseEncryption, "Legacy port-8883 TLS behavior was not preserved during migration.");
Assert(!legacyConfig.SkipCertificateValidation, "Legacy insecure certificate-validation default was not migrated to a secure default.");
Assert(legacyConfig.Username == null && legacyConfig.Password == null, "Prototype dummy MQTT credentials were not removed during migration.");

var legacyPlaintextConfig = new MqttConfig
{
    ConfigVersion = 0, BrokerHost = "broker.local", BrokerPort = 1883, InstanceId = "legacy-plain", UseEncryption = true
};
legacyPlaintextConfig.NormalizeAndValidate();
Assert(!legacyPlaintextConfig.UseEncryption, "Legacy non-8883 configuration must preserve the old application's effective plaintext behavior.");

var invalid = new MqttConfig { InstanceId = "test", BaseTopic = "bad/#" };
var rejected = false;
try { invalid.NormalizeAndValidate(); } catch (InvalidOperationException) { rejected = true; }
Assert(rejected, "MQTT wildcard in BaseTopic must be rejected.");

var passwordOnly = new MqttConfig { InstanceId = "test", Password = "secret" };
rejected = false;
try { passwordOnly.NormalizeAndValidate(); } catch (InvalidOperationException) { rejected = true; }
Assert(rejected, "MQTT password without a username must be rejected.");

var stateStoreTemp = Path.Combine(Path.GetTempPath(), "vmq-state-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(stateStoreTemp);
try
{
    var stateStore = new RuntimeStateStore(stateStoreTemp);
    var state = new BridgeRuntimeState
    {
        SchemaVersion = 3,
        InstanceId = "persisted",
        BaseTopic = "voicemeeter/persisted",
        DiscoveryPrefix = "homeassistant",
        LegacyDiscoveryCleaned = true,
        DiscoveryTopics = new() { "homeassistant/device/test/config" },
        RetainedTopics = new() { "voicemeeter/persisted/status" }
    };
    Assert(stateStore.Save(state), "Runtime migration registry could not be saved.");
    var loadedState = stateStore.Load();
    Assert(loadedState.SchemaVersion == 3 && loadedState.InstanceId == "persisted", "Runtime migration registry identity did not persist.");
    Assert(loadedState.DiscoveryTopics.SequenceEqual(state.DiscoveryTopics), "Runtime discovery registry did not persist.");
    Assert(loadedState.RetainedTopics.SequenceEqual(state.RetainedTopics), "Runtime retained-topic registry did not persist.");
}
finally
{
    try { Directory.Delete(stateStoreTemp, recursive: true); } catch { }
}

// Device-based Home Assistant discovery must be valid JSON, stable and capability-aware.
var discoveryTemp = Path.Combine(Path.GetTempPath(), "vmq-discovery-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(discoveryTemp);
try
{
    var discoveryConfig = new MqttConfig
    {
        BrokerHost = "localhost",
        InstanceId = "discovery-test",
        BaseTopic = "voicemeeter",
        ClientId = "vmq",
        SkipCertificateValidation = false
    };
    discoveryConfig.NormalizeAndValidate();

    await using var discoveryService = new VoicemeeterMqttService(discoveryConfig, discoveryTemp);
    var serviceType = typeof(VoicemeeterMqttService);
    var topologyField = serviceType.GetField("_topology", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Could not access topology field for discovery self-test.");
    topologyField.SetValue(discoveryService, new VoicemeeterTopology(3, 2, 1));

    var vmBuilder = serviceType.GetMethod("BuildVoicemeeterDiscoveryJson", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Could not access Voicemeeter discovery builder for self-test.");
    var vmJson = (string)(vmBuilder.Invoke(discoveryService, null)
        ?? throw new InvalidOperationException("Voicemeeter discovery builder returned null."));
    using (var vmDoc = JsonDocument.Parse(vmJson))
    {
        var root = vmDoc.RootElement;
        Assert(root.TryGetProperty("device", out _), "Voicemeeter device discovery is missing device metadata.");
        Assert(root.TryGetProperty("origin", out _), "Voicemeeter device discovery is missing origin metadata.");
        var components = root.GetProperty("components");
        Assert(components.EnumerateObject().Count() == 11, "Standard Voicemeeter discovery must expose gain/mute for 3 strips + 2 buses, plus the engine restart button.");
        Assert(components.GetProperty("strip0_gain").GetProperty("state_topic").GetString() == $"{discoveryConfig.EffectiveBaseTopic}/strip0/gain",
            "Voicemeeter discovery state topic is not instance-scoped.");
        Assert(components.GetProperty("strip0_gain").GetProperty("unique_id").GetString() == "discovery-test_strip0_gain",
            "Voicemeeter discovery unique ID is not stable.");
        Assert(components.GetProperty("strip0_gain").GetProperty("has_entity_name").GetBoolean(),
            "Voicemeeter discovery should use modern Home Assistant entity naming.");
        var restart = components.GetProperty("restart_audio_engine");
        Assert(restart.GetProperty("platform").GetString() == "button", "Engine restart discovery must create a button.");
        Assert(restart.GetProperty("command_topic").GetString() == $"{discoveryConfig.EffectiveBaseTopic}/voicemeeter/restart/set",
            "Engine restart command topic must be instance-scoped.");
        Assert(restart.GetProperty("payload_press").GetString() == "RESTART", "Engine restart button payload must be explicit.");
        Assert(!restart.GetProperty("retain").GetBoolean() && restart.GetProperty("qos").GetInt32() == 1,
            "Engine restart button commands must use non-retained QoS 1.");
        Assert(restart.GetProperty("device_class").GetString() == "restart" && restart.GetProperty("entity_category").GetString() == "config",
            "Engine restart button must be classified as a restart configuration entity.");
        Assert(!restart.TryGetProperty("state_topic", out _), "Engine restart button must not claim an authoritative state.");
        Assert(restart.GetProperty("availability_mode").GetString() == "all", "Engine restart button must require both backend and service availability.");
        var restartAvailability = restart.GetProperty("availability").EnumerateArray()
            .Select(item => item.GetProperty("topic").GetString()).ToHashSet(StringComparer.Ordinal);
        Assert(restartAvailability.SetEquals(new[] { $"{discoveryConfig.EffectiveBaseTopic}/status", $"{discoveryConfig.EffectiveBaseTopic}/voicemeeter/status" }),
            "Engine restart button must use service and Voicemeeter availability topics.");
    }

    var audioBuilder = serviceType.GetMethod("BuildAudioDiscoveryJson", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Could not access audio discovery builder for self-test.");
    var richPlayback = new AudioDevice
    {
        Id = "endpoint-1",
        Name = "Speaker \"A\"\nRoom",
        Type = "playback",
        Volume = 42.5,
        IsMuted = false,
        PeakLevel = 0.25f,
        IsDefault = true
    };
    var richCapture = richPlayback with { Type = "capture", IsDefault = false };
    var limitedPlayback = new AudioDevice { Id = "endpoint-2", Name = "Limited playback", Type = "playback" };
    var limitedCapture = new AudioDevice { Id = "endpoint-3", Name = "Limited capture", Type = "capture" };
    var endpoints = new[]
    {
        (Device: richPlayback, Key: "b8c3822c77df7224"),
        (Device: richCapture, Key: "c2ec790b2a103ad1"),
        (Device: limitedPlayback, Key: "dd28bffcc8493f29"),
        (Device: limitedCapture, Key: "5abd975865b44ed8")
    };
    var audioJson = (string)(audioBuilder.Invoke(discoveryService, new object[] { endpoints.Select(endpoint => endpoint.Device).ToArray() })
        ?? throw new InvalidOperationException("Audio discovery builder returned null."));
    using (var audioDoc = JsonDocument.Parse(audioJson))
    {
        var root = audioDoc.RootElement;
        var device = root.GetProperty("device");
        Assert(device.GetProperty("identifiers").EnumerateArray().Select(identifier => identifier.GetString())
                .SequenceEqual(new[] { "windows-audio-discovery-test" }),
            "All Windows audio endpoints must belong to one stable Home Assistant device.");
        Assert(device.GetProperty("name").GetString()?.Contains("Windows Audio", StringComparison.Ordinal) == true,
            "Grouped Windows audio discovery must have a recognizable device name.");
        Assert(root.TryGetProperty("origin", out _), "Grouped Windows audio discovery is missing origin metadata.");
        var components = root.GetProperty("components");
        Assert(components.EnumerateObject().Count() == 10,
            "Grouped discovery must expose four entities per capable endpoint and only default for endpoints with unknown capabilities.");
        var expectedComponents = new HashSet<string>(StringComparer.Ordinal);
        var uniqueIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var endpoint in endpoints)
        {
            var metrics = endpoint.Device.Volume.HasValue ? new[] { "volume", "muted", "peak", "default" } : new[] { "default" };
            foreach (var metric in metrics)
            {
                var componentKey = $"{endpoint.Key}_{metric}";
                expectedComponents.Add(componentKey);
                var component = components.GetProperty(componentKey);
                var uniqueId = component.GetProperty("unique_id").GetString();
                Assert(uniqueId == $"{discoveryConfig.InstanceId}_{endpoint.Key}_{metric}",
                    "Grouping Windows audio endpoints must preserve existing entity unique IDs.");
                Assert(uniqueId != null && uniqueIds.Add(uniqueId), "Grouped Windows audio entity unique IDs must not collide.");
                Assert(component.GetProperty("state_topic").GetString() == $"{discoveryConfig.EffectiveBaseTopic}/audio/{endpoint.Device.Type}/{endpoint.Key}/{metric}",
                    "Grouping Windows audio endpoints must preserve existing state topics.");
                Assert(component.GetProperty("has_entity_name").GetBoolean(), "Grouped audio discovery should use modern Home Assistant entity naming.");
                var name = component.GetProperty("name").GetString() ?? string.Empty;
                var direction = endpoint.Device.Type == "playback" ? "Playback" : "Capture";
                Assert(name.Contains(endpoint.Device.Name, StringComparison.Ordinal) && name.Contains(direction, StringComparison.Ordinal),
                    "Grouped audio entity names must preserve endpoint text, JSON escaping and playback/capture direction.");
            }
        }
        Assert(expectedComponents.SetEquals(components.EnumerateObject().Select(component => component.Name)),
            "Grouped audio discovery must use distinct endpoint-qualified component keys and omit unsupported capabilities.");
        Assert(components.GetProperty("b8c3822c77df7224_peak").GetProperty("qos").GetInt32() == 0,
            "Grouping must preserve QoS 0 for audio peak telemetry.");
        Assert(components.GetProperty("b8c3822c77df7224_volume").GetProperty("qos").GetInt32() == 1,
            "Grouping must preserve QoS 1 for audio snapshot state.");
    }

    // Grouped discovery must explicitly remove a disconnected endpoint without disturbing survivors.
    var audioRemovalBuilder = serviceType.GetMethod("BuildAudioComponentRemovalJson", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Could not access audio component-removal builder for self-test.");
    var runtimeStateField = serviceType.GetField("_runtimeState", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Could not access runtime state for audio hotplug self-test.");
    var audioRuntimeState = (BridgeRuntimeState)(runtimeStateField.GetValue(discoveryService)
        ?? throw new InvalidOperationException("Runtime state was null for audio hotplug self-test."));
    audioRuntimeState.RetainedTopics.AddRange(new[]
    {
        $"{discoveryConfig.EffectiveBaseTopic}/audio/playback/b8c3822c77df7224/default",
        $"{discoveryConfig.EffectiveBaseTopic}/audio/capture/c2ec790b2a103ad1/default"
    });
    var survivorAudioJson = (string)(audioBuilder.Invoke(discoveryService, new object[] { new[] { richCapture } })
        ?? throw new InvalidOperationException("Surviving audio endpoint discovery builder returned null."));
    var audioRemovalJson = audioRemovalBuilder.Invoke(discoveryService, new object[] { survivorAudioJson }) as string;
    Assert(audioRemovalJson != null, "Disconnecting one grouped audio endpoint must produce explicit component removals.");
    using (var removalDoc = JsonDocument.Parse(audioRemovalJson!))
    using (var survivorDoc = JsonDocument.Parse(survivorAudioJson))
    {
        var removedComponents = removalDoc.RootElement.GetProperty("components");
        var survivingComponents = survivorDoc.RootElement.GetProperty("components");
        Assert(removedComponents.EnumerateObject().Count() == 8,
            "Hotplug intermediate discovery must contain four removals and four surviving entities.");
        foreach (var removedMetric in new[] { (Name: "volume", Platform: "sensor"), (Name: "muted", Platform: "binary_sensor"), (Name: "peak", Platform: "sensor"), (Name: "default", Platform: "binary_sensor") })
        {
            var key = $"b8c3822c77df7224_{removedMetric.Name}";
            var marker = removedComponents.GetProperty(key);
            Assert(marker.EnumerateObject().Count() == 1 && marker.GetProperty("platform").GetString() == removedMetric.Platform,
                "Disconnected endpoint components must use Home Assistant platform-only removal markers.");
            Assert(!survivingComponents.TryGetProperty(key, out _), "Final grouped discovery must omit disconnected endpoint components.");
        }
        foreach (var survivor in survivingComponents.EnumerateObject())
            Assert(removedComponents.GetProperty(survivor.Name).GetRawText() == survivor.Value.GetRawText(),
                "Hotplug removal discovery must preserve every surviving entity's configuration.");
    }
    Assert(audioRemovalBuilder.Invoke(discoveryService, new object[] { audioJson }) == null,
        "Unchanged grouped audio discovery must not generate removal payloads.");
    var capabilityLossJson = (string)(audioBuilder.Invoke(discoveryService, new object[] { new[] { richPlayback with { PeakLevel = null }, richCapture } })
        ?? throw new InvalidOperationException("Audio capability-loss discovery builder returned null."));
    var capabilityRemovalJson = audioRemovalBuilder.Invoke(discoveryService, new object[] { capabilityLossJson }) as string;
    Assert(capabilityRemovalJson != null, "A missing audio capability must produce an explicit component removal.");
    using (var capabilityRemovalDoc = JsonDocument.Parse(capabilityRemovalJson!))
    {
        var components = capabilityRemovalDoc.RootElement.GetProperty("components");
        var peakRemoval = components.GetProperty("b8c3822c77df7224_peak");
        Assert(components.EnumerateObject().Count() == 8 && peakRemoval.EnumerateObject().Count() == 1 && peakRemoval.GetProperty("platform").GetString() == "sensor",
            "Capability loss must remove only the missing peak component while preserving current entities.");
    }

    var emptyAudioJson = (string)(audioBuilder.Invoke(discoveryService, new object[] { Array.Empty<AudioDevice>() })
        ?? throw new InvalidOperationException("Empty audio discovery builder returned null."));
    using (var emptyDoc = JsonDocument.Parse(emptyAudioJson))
    {
        Assert(!emptyDoc.RootElement.GetProperty("components").EnumerateObject().Any(),
            "No audio endpoints must produce no discovery entities.");
    }

}
finally
{
    try { Directory.Delete(discoveryTemp, recursive: true); } catch { }
}

// Exercise the real MQTT lifecycle against an in-process MQTTnet broker.
// This covers delayed broker startup, reconnect, lost-session resubscribe, QoS 1 delivery,
// retained graceful-offline state and the no-reconnect-after-shutdown invariant.
var port = FindFreeTcpPort();
var lifecycleConfig = new MqttConfig
{
    BrokerHost = "127.0.0.1",
    BrokerPort = port,
    InstanceId = "lifecycle-test",
    BaseTopic = "vmqtest",
    ClientId = "voicemeeter-service",
    KeepAliveSeconds = 5,
    ConnectTimeoutSeconds = 2,
    ReconnectMinSeconds = 1,
    ReconnectMaxSeconds = 1,
    SkipCertificateValidation = false
};
lifecycleConfig.NormalizeAndValidate();

var commandTopic = $"{lifecycleConfig.EffectiveBaseTopic}/test/set";
var reconcileCount = 0;
var receivedCount = 0;
MqttServer? broker = null;
IMqttClient? probe = null;
await using var publisher = new MqttPublisher(lifecycleConfig);

publisher.OnConnectedReconcileAsync = async cancellationToken =>
{
    Interlocked.Increment(ref reconcileCount);
    if (!await publisher.SubscribeAsync(commandTopic, MqttQualityOfServiceLevel.AtLeastOnce, cancellationToken))
        return false;

    return await publisher.PublishAsync(
        $"{lifecycleConfig.EffectiveBaseTopic}/status",
        "online",
        retain: true,
        qos: MqttQualityOfServiceLevel.AtLeastOnce,
        cancellationToken);
};

publisher.OnMessageReceived = (topic, payload, retained) =>
{
    if (topic == commandTopic && payload == "ping" && !retained)
        Interlocked.Increment(ref receivedCount);
    return Task.CompletedTask;
};

try
{
    // Start the application before the broker exists. It must keep retrying rather than dying.
    await publisher.StartAsync(CancellationToken.None);
    await Task.Delay(200);

    broker = CreateBroker(port);
    await broker.StartAsync();

    await WaitUntilAsync(
        () => publisher.IsReady && Volatile.Read(ref reconcileCount) >= 1,
        TimeSpan.FromSeconds(10),
        "Publisher did not recover when the broker appeared.");

    // Exercise the actual service command gates with a real MQTT connection and an injected native callback.
    var dispatchTemp = Path.Combine(Path.GetTempPath(), "vmq-dispatch-" + Guid.NewGuid().ToString("N"));
    try
    {
        var dispatchConfig = new MqttConfig
        {
            BrokerHost = "127.0.0.1", BrokerPort = port, InstanceId = "dispatch-test", BaseTopic = "vmqtest"
        };
        dispatchConfig.NormalizeAndValidate();
        await using var dispatchService = new VoicemeeterMqttService(dispatchConfig, dispatchTemp);
        var dispatchType = typeof(VoicemeeterMqttService);
        var dispatchRestartField = dispatchType.GetField("_audioEngineRestart", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Could not access restart coordinator for dispatch self-test.");
        var dispatchReadyField = dispatchType.GetField("_voicemeeterReady", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Could not access Voicemeeter readiness for dispatch self-test.");
        var dispatchMqttField = dispatchType.GetField("_mqtt", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Could not access MQTT publisher for dispatch self-test.");
        var dispatchHandler = dispatchType.GetMethod("HandleMqttMessageAsync", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Could not access command handler for dispatch self-test.");
        var dispatchNativeCalls = 0;
        dispatchRestartField.SetValue(dispatchService, new AudioEngineRestartCoordinator(
            () => { Interlocked.Increment(ref dispatchNativeCalls); return true; }, _ => Task.FromResult(true)));
        var dispatchMqtt = (MqttPublisher)(dispatchMqttField.GetValue(dispatchService)
            ?? throw new InvalidOperationException("Dispatch MQTT publisher was null."));
        Task DispatchAsync(string topic, string payload, bool retained = false) =>
            (Task)(dispatchHandler.Invoke(dispatchService, new object[] { topic, payload, retained })
                ?? throw new InvalidOperationException("Command handler returned null."));
        var dispatchTopic = $"{dispatchConfig.EffectiveBaseTopic}/voicemeeter/restart/set";

        dispatchReadyField.SetValue(dispatchService, true);
        await DispatchAsync(dispatchTopic, "RESTART");
        Assert(dispatchNativeCalls == 0, "Restart must be ignored before MQTT reconciliation is ready.");
        dispatchMqtt.OnConnectedReconcileAsync = _ => Task.FromResult(true);
        await dispatchMqtt.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => dispatchMqtt.IsReady, TimeSpan.FromSeconds(10), "Dispatch test MQTT connection did not become ready.");
        dispatchReadyField.SetValue(dispatchService, false);
        await DispatchAsync(dispatchTopic, "RESTART");
        Assert(dispatchNativeCalls == 0, "Restart must be ignored while Voicemeeter is unavailable.");
        dispatchReadyField.SetValue(dispatchService, true);
        await DispatchAsync(dispatchTopic, "RESTART", retained: true);
        Assert(dispatchNativeCalls == 0, "Retained engine restart commands must never invoke the native API.");
        foreach (var invalidRestartPayload in new[] { "", "restart", " RESTART", "RESTART ", "RESTART\n", "1" })
            await DispatchAsync(dispatchTopic, invalidRestartPayload);
        await DispatchAsync(dispatchTopic + "/extra", "RESTART");
        await DispatchAsync("vmqtest/other-instance/voicemeeter/restart/set", "RESTART");
        Assert(dispatchNativeCalls == 0, "Malformed engine restart payloads and topics must never invoke the native API.");
        await DispatchAsync(dispatchTopic, "RESTART");
        Assert(dispatchNativeCalls == 1, "A ready, exact, non-retained restart command must invoke the native API once.");
        await DispatchAsync(dispatchTopic, "RESTART");
        Assert(dispatchNativeCalls == 1, "Service dispatch must suppress duplicate restart commands during cooldown.");
    }
    finally
    {
        try { Directory.Delete(dispatchTemp, recursive: true); } catch { }
    }

    probe = await ConnectProbeAsync(port, "vmq-probe-1");
    await probe.PublishAsync(new MqttApplicationMessageBuilder()
        .WithTopic(commandTopic)
        .WithPayload("ping")
        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        .Build());

    await WaitUntilAsync(
        () => Volatile.Read(ref receivedCount) >= 1,
        TimeSpan.FromSeconds(5),
        "Initial command subscription did not receive a QoS 1 command.");

    await DisconnectAndDisposeAsync(probe);
    probe = null;

    // A broker restart destroys this clean session. The application must reconnect and subscribe again.
    await broker.StopAsync();
    await WaitUntilAsync(
        () => !publisher.IsConnected,
        TimeSpan.FromSeconds(5),
        "Publisher did not observe broker shutdown.");

    await Task.Delay(200);
    await broker.StartAsync();

    await WaitUntilAsync(
        () => publisher.IsReady && Volatile.Read(ref reconcileCount) >= 2,
        TimeSpan.FromSeconds(10),
        "Publisher did not reconcile after broker restart.");

    probe = await ConnectProbeAsync(port, "vmq-probe-2");
    await probe.PublishAsync(new MqttApplicationMessageBuilder()
        .WithTopic(commandTopic)
        .WithPayload("ping")
        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        .Build());

    await WaitUntilAsync(
        () => Volatile.Read(ref receivedCount) >= 2,
        TimeSpan.FromSeconds(5),
        "Command subscription was not restored after reconnect.");

    await DisconnectAndDisposeAsync(probe);
    probe = null;

    // Graceful stop must explicitly retain offline and must never reconnect afterwards.
    await publisher.StopAsync();

    var retainedStatus = await broker.GetRetainedMessageAsync($"{lifecycleConfig.EffectiveBaseTopic}/status");
    Assert(retainedStatus != null, "Graceful shutdown did not leave a retained status message.");
    Assert(Encoding.UTF8.GetString(retainedStatus.PayloadSegment.ToArray()) == "offline", "Graceful shutdown did not retain offline status.");

    await Task.Delay(1500);
    var clientsAfterStop = await broker.GetClientsAsync();
    Assert(clientsAfterStop.All(client => client.Id != lifecycleConfig.EffectiveClientId), "MQTT client reconnected after intentional shutdown.");
}
finally
{
    await DisconnectAndDisposeAsync(probe);
    if (broker != null)
    {
        try { await broker.StopAsync(); } catch { }
        broker.Dispose();
    }
}

Console.WriteLine("All VoicemeeterMQTT self-tests passed.");

sealed class ManualTimeProvider : TimeProvider
{
    private long _elapsedTicks;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;
    public override long GetTimestamp() => Interlocked.Read(ref _elapsedTicks);
    public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch.AddTicks(Interlocked.Read(ref _elapsedTicks));

    public void Advance(TimeSpan elapsed) => Interlocked.Add(ref _elapsedTicks, elapsed.Ticks);
}
