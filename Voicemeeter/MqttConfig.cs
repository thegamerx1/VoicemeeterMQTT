using System.Text.Json.Serialization;
namespace VoicemeeterMQTT;

/// <summary>
/// Configuration for MQTT connection and behavior
/// Loaded from JSON file in AppData/Local/voicemeetermqtt/config.json
/// </summary>
public class MqttConfig
{
	public string BrokerHost { get; set; } = "localhost";
	public int BrokerPort { get; set; } = 1883;
	public string ClientId { get; set; } = "voicemeeter-service";
	public string BaseTopic { get; set; } = "voicemeeter";
	public int UpdateIntervalMs { get; set; } = 5000;
	public string? Username { get; set; } = "mqtt_user";
	public string? Password { get; set; } = "mqtt_password";
	public bool UseEncryption { get; set; } = false;
	public bool SkipCertificateValidation { get; set; } = true;
}


[JsonSerializable(typeof(MqttConfig))]
public partial class AppJsonContext : JsonSerializerContext
{
}