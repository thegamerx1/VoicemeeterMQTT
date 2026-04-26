# Voicemeeter MQTT Service

Control and monitor your audio setup from Home Assistant. Bridge Voicemeeter to your smart home - adjust strip volumes, mute channels, and automate audio routing based on scenes and presence detection.

**Why use this:**

- Real-time audio control in Home Assistant dashboards
- Automate audio routing with presence/scene changes
- Monitor stream health and audio levels
- Lightweight (~20MB) - no performance hit
- Direct integration with existing MQTT infrastructure
- Open source, runs locally on Windows
- No plugin required for Home Assistant, Plug and play with your MQTT server.

## Requirements

- [Voicemeeter](https://vb-audio.com/Voicemeeter)
- [.NET 10](https://dotnet.microsoft.com)
- MQTT broker

## Build & Run

```bash
dotnet build
dotnet run -c Release
dotnet publish -c Release -r win-x64 -p:PublishSingleFile=true -p:PublishTrimmed=true -p:PublishReadyToRun=false -p:DebugType=None -p:DebugSymbols=false -p:TrimMode=full --self-contained false
```

Config file: `%LOCALAPPDATA%\voicemeetermqtt\config.json`

```json
{
	"BrokerHost": "localhost",
	"BrokerPort": 1883,
	"ClientId": "voicemeeter-service",
	"BaseTopic": "voicemeeter",
	"UpdateIntervalMs": 5000,
	"Username": "",
	"Password": "",
	"UseEncryption": false,
	"SkipCertificateValidation": true
}
```

## Topics

`{BaseTopic}/master/{volume,mute}`
`{BaseTopic}/strip{0-6}/{volume,mute}` (add `/set` suffix to control)
`{BaseTopic}/bus{0-6}/{volume,mute}` (add `/set` suffix to control)

## License

MIT
