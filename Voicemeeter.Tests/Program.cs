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
        Assert(components.EnumerateObject().Count() == 10, "Standard Voicemeeter discovery must expose 3 strips + 2 buses with gain/mute entities.");
        Assert(components.GetProperty("strip0_gain").GetProperty("state_topic").GetString() == $"{discoveryConfig.EffectiveBaseTopic}/strip0/gain",
            "Voicemeeter discovery state topic is not instance-scoped.");
        Assert(components.GetProperty("strip0_gain").GetProperty("unique_id").GetString() == "discovery-test_strip0_gain",
            "Voicemeeter discovery unique ID is not stable.");
        Assert(components.GetProperty("strip0_gain").GetProperty("has_entity_name").GetBoolean(),
            "Voicemeeter discovery should use modern Home Assistant entity naming.");
    }

    var audioBuilder = serviceType.GetMethod("BuildAudioDiscoveryJson", BindingFlags.Instance | BindingFlags.NonPublic)
        ?? throw new InvalidOperationException("Could not access audio discovery builder for self-test.");
    var richAudioDevice = new AudioDevice
    {
        Id = "endpoint-1",
        Name = "Speaker \"A\"\nRoom",
        Type = "playback",
        Volume = 42.5,
        IsMuted = false,
        PeakLevel = 0.25f,
        IsDefault = true
    };
    var richAudioJson = (string)(audioBuilder.Invoke(discoveryService, new object[] { richAudioDevice, "abc123" })
        ?? throw new InvalidOperationException("Audio discovery builder returned null."));
    using (var audioDoc = JsonDocument.Parse(richAudioJson))
    {
        var components = audioDoc.RootElement.GetProperty("components");
        Assert(components.EnumerateObject().Count() == 4, "Fully capable audio endpoint should expose volume/muted/peak/default.");
        Assert(audioDoc.RootElement.GetProperty("device").GetProperty("name").GetString() == richAudioDevice.Name,
            "Audio discovery JSON escaping changed the device name.");
        Assert(components.GetProperty("volume").GetProperty("has_entity_name").GetBoolean(),
            "Audio discovery should use modern Home Assistant entity naming.");
    }

    var limitedAudioDevice = new AudioDevice
    {
        Id = "endpoint-2",
        Name = "Limited endpoint",
        Type = "capture",
        IsDefault = false
    };
    var limitedAudioJson = (string)(audioBuilder.Invoke(discoveryService, new object[] { limitedAudioDevice, "def456" })
        ?? throw new InvalidOperationException("Limited audio discovery builder returned null."));
    using (var limitedDoc = JsonDocument.Parse(limitedAudioJson))
    {
        var components = limitedDoc.RootElement.GetProperty("components");
        Assert(components.EnumerateObject().Select(x => x.Name).SequenceEqual(new[] { "default" }),
            "Unsupported audio capabilities must not create permanently unknown HA entities.");
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
