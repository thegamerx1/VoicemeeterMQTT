# Voicemeeter MQTT Service

Control and monitor Voicemeeter and Windows audio endpoints from Home Assistant through MQTT.

## Highlights

- Automatic MQTT reconnect with exponential backoff and jitter
- Retained Last Will availability
- Deterministic resubscription and state reconciliation after every reconnect
- Home Assistant device-based MQTT discovery, with all Windows audio endpoints grouped under one Windows Audio device per installation
- Home Assistant button to restart the Voicemeeter audio engine
- Stable per-installation MQTT/Home Assistant identity
- Retained authoritative gain/mute/audio snapshot state
- Home Assistant birth handling for state refresh
- Independent Windows audio monitoring, including real audio-only operation
- TLS certificate validation enabled by default

## Requirements

- Windows
- Voicemeeter for Voicemeeter entities (Windows audio monitoring works without it)
- .NET 10
- MQTT broker

## Build and test

```bash
dotnet build ./Voicemeeter/Voicemeeter.csproj -c Release
dotnet run --project ./Voicemeeter.Tests/Voicemeeter.Tests.csproj -c Release
dotnet run --project ./Voicemeeter/Voicemeeter.csproj -c Release
```

The release build intentionally does not use full trimming because the application depends on COM and dynamically loaded native exports.

## Configuration

Config file:

```text
%LOCALAPPDATA%\voicemeetermqtt\config.json
```

Safe defaults are generated automatically. `InstanceId` is generated once and persisted, so multiple PCs can use the same broker without client-ID, topic or Home Assistant entity collisions. The effective MQTT Client ID is derived deterministically from `ClientId` + `InstanceId` and compacted to at most 23 alphanumeric characters when necessary for compatibility with strict MQTT 3.1.1 brokers.


Example:

```json
{
  "BrokerHost": "localhost",
  "BrokerPort": 1883,
  "ClientId": "voicemeeter-service",
  "InstanceId": "studio-pc-a1b2c3d4",
  "BaseTopic": "voicemeeter",
  "UseInstanceInBaseTopic": true,
  "DiscoveryPrefix": "homeassistant",
  "HomeAssistantStatusTopic": "homeassistant/status",
  "UpdateIntervalMs": 1000,
  "AudioUpdateIntervalMs": 1000,
  "KeepAliveSeconds": 60,
  "ReconnectMinSeconds": 1,
  "ReconnectMaxSeconds": 60,
  "Username": "",
  "Password": "",
  "UseEncryption": false,
  "SkipCertificateValidation": false
}
```

With `UseInstanceInBaseTopic=true`, the effective topic prefix is:

```text
{BaseTopic}/{InstanceId}
```

For example:

```text
voicemeeter/studio-pc-a1b2c3d4
```

`UseEncryption` explicitly controls MQTT TLS. Port 8883 no longer silently changes TLS behavior. `SkipCertificateValidation=true` is insecure and should only be used in controlled test environments.

## Topics

Voicemeeter state and commands:

```text
{effectiveBaseTopic}/strip{n}/gain
{effectiveBaseTopic}/strip{n}/gain/set
{effectiveBaseTopic}/strip{n}/muted
{effectiveBaseTopic}/strip{n}/muted/set
{effectiveBaseTopic}/bus{n}/gain
{effectiveBaseTopic}/bus{n}/gain/set
{effectiveBaseTopic}/bus{n}/muted
{effectiveBaseTopic}/bus{n}/muted/set
```

The number of strips and buses is detected from the installed Voicemeeter edition rather than hard-coded.

Restart the Voicemeeter audio engine:

```text
{effectiveBaseTopic}/voicemeeter/restart/set
```

Publish the exact payload `RESTART`, non-retained, with QoS 1. Home Assistant exposes this command as a configuration button on the Voicemeeter device. It requires both the service and Voicemeeter to be available. The command restarts the engine and briefly interrupts audio; it does not restart the Voicemeeter application or the MQTT service. Repeated requests received while a restart is pending or within 10 seconds of the previous accepted attempt are ignored. The service allows a one-second recovery grace period and waits up to 30 seconds for Voicemeeter to recover before publishing fresh state.

Availability:

```text
{effectiveBaseTopic}/status
{effectiveBaseTopic}/voicemeeter/status
{effectiveBaseTopic}/audio/status
```

Windows audio endpoint state:

```text
{effectiveBaseTopic}/audio/{playback|capture}/{stableEndpointHash}/volume
{effectiveBaseTopic}/audio/{playback|capture}/{stableEndpointHash}/muted
{effectiveBaseTopic}/audio/{playback|capture}/{stableEndpointHash}/peak
{effectiveBaseTopic}/audio/{playback|capture}/{stableEndpointHash}/default
```

Home Assistant discovery uses retained device discovery:

```text
{DiscoveryPrefix}/device/voicemeeter-{InstanceId}/config
{DiscoveryPrefix}/device/windows-audio-{InstanceId}/config
```

All active Windows playback and capture endpoints appear as entities on the same Windows Audio device. Entity names include the endpoint name and Playback/Capture direction. Endpoint state topics and entity unique IDs remain stable; only their Home Assistant device grouping changes.

This grouping is a breaking change for existing Home Assistant device assignments. No dedicated migration is provided for the previous device-per-endpoint layout. If upgrading leaves old endpoint devices behind, remove the old retained discovery messages at `{DiscoveryPrefix}/device/windows-audio-{InstanceId}-{endpointHash}/config` by publishing an empty retained payload, remove any obsolete Home Assistant device entries, and restart the service. Existing legacy per-entity discovery cleanup remains unchanged.

## MQTT behavior

- Service Last Will: `offline`, retained, QoS 1
- Discovery: retained, QoS 1
- Gain/mute/audio snapshot state: retained, QoS 1
- Peak level: non-retained, QoS 0
- Commands: expected non-retained; retained command messages are rejected
- On every MQTT reconnect: resubscribe, reconcile discovery, republish current state, then mark service online
- On `homeassistant/status = online`: current state is replayed after randomized jitter; retained discovery is not flooded again

## License

MIT
