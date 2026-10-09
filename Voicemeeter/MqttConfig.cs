using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;

namespace VoicemeeterMQTT;

/// <summary>
/// Configuration for MQTT connection and bridge behavior.
/// </summary>
public sealed class MqttConfig
{
    public int ConfigVersion { get; set; }
    public string BrokerHost { get; set; } = "localhost";
    public int BrokerPort { get; set; } = 1883;
    public string ClientId { get; set; } = "voicemeeter-service";
    public string InstanceId { get; set; } = string.Empty;
    public string BaseTopic { get; set; } = "voicemeeter";
    public bool UseInstanceInBaseTopic { get; set; } = true;
    public string DiscoveryPrefix { get; set; } = "homeassistant";
    public string HomeAssistantStatusTopic { get; set; } = "homeassistant/status";
    public int UpdateIntervalMs { get; set; } = 1000;
    public int AudioUpdateIntervalMs { get; set; } = 1000;
    public int KeepAliveSeconds { get; set; } = 60;
    public int ConnectTimeoutSeconds { get; set; } = 10;
    public int ReconnectMinSeconds { get; set; } = 1;
    public int ReconnectMaxSeconds { get; set; } = 60;
    public string? Username { get; set; }
    public string? Password { get; set; }
    public bool UseEncryption { get; set; }
    public bool SkipCertificateValidation { get; set; }

    [JsonIgnore]
    public string EffectiveBaseTopic { get; private set; } = string.Empty;

    [JsonIgnore]
    public string EffectiveClientId { get; private set; } = string.Empty;

    /// <summary>
    /// Normalizes persisted values, creates a stable per-installation identity when needed,
    /// and validates values before any network work starts. Returns true when persisted
    /// configuration should be written back to disk.
    /// </summary>
    public bool NormalizeAndValidate()
    {
        var changed = false;

        // Version 1 (the uploaded application) ignored UseEncryption and enabled TLS solely on
        // port 8883. Preserve that *effective* transport behavior during migration instead of
        // interpreting a previously ignored setting for the first time. The old application also
        // disabled certificate validation by default; upgrades deliberately move to validation on.
        if (ConfigVersion < 2)
        {
            var legacyEffectiveTls = BrokerPort == 8883;
            if (UseEncryption != legacyEffectiveTls)
            {
                UseEncryption = legacyEffectiveTls;
                changed = true;
            }

            if (SkipCertificateValidation)
            {
                SkipCertificateValidation = false;
                changed = true;
            }

            if (string.Equals(Username, "mqtt_user", StringComparison.Ordinal) &&
                string.Equals(Password, "mqtt_password", StringComparison.Ordinal))
            {
                Username = null;
                Password = null;
                changed = true;
            }

            ConfigVersion = 2;
            changed = true;
        }

        var normalizedBrokerHost = BrokerHost.Trim();
        if (!string.Equals(normalizedBrokerHost, BrokerHost, StringComparison.Ordinal)) { BrokerHost = normalizedBrokerHost; changed = true; }
        if (string.IsNullOrWhiteSpace(BrokerHost))
            throw new InvalidOperationException("BrokerHost cannot be empty.");
        if (BrokerPort is < 1 or > 65535)
            throw new InvalidOperationException("BrokerPort must be between 1 and 65535.");
        if (UpdateIntervalMs < 50)
            throw new InvalidOperationException("UpdateIntervalMs must be at least 50 ms.");
        if (AudioUpdateIntervalMs < 100)
            throw new InvalidOperationException("AudioUpdateIntervalMs must be at least 100 ms.");
        if (KeepAliveSeconds is < 5 or > 3600)
            throw new InvalidOperationException("KeepAliveSeconds must be between 5 and 3600.");
        if (ConnectTimeoutSeconds is < 1 or > 120)
            throw new InvalidOperationException("ConnectTimeoutSeconds must be between 1 and 120.");
        if (ReconnectMinSeconds is < 1 or > 300)
            throw new InvalidOperationException("ReconnectMinSeconds must be between 1 and 300.");
        if (ReconnectMaxSeconds < ReconnectMinSeconds || ReconnectMaxSeconds > 3600)
            throw new InvalidOperationException("ReconnectMaxSeconds must be >= ReconnectMinSeconds and <= 3600.");
        if (string.IsNullOrWhiteSpace(Username) && !string.IsNullOrEmpty(Password))
            throw new InvalidOperationException("MQTT Password cannot be configured without a Username.");

        var normalizedBaseTopic = NormalizeTopicPrefix(BaseTopic, "BaseTopic");
        if (!string.Equals(normalizedBaseTopic, BaseTopic, StringComparison.Ordinal)) { BaseTopic = normalizedBaseTopic; changed = true; }
        var normalizedDiscoveryPrefix = NormalizeTopicPrefix(DiscoveryPrefix, "DiscoveryPrefix");
        if (!string.Equals(normalizedDiscoveryPrefix, DiscoveryPrefix, StringComparison.Ordinal)) { DiscoveryPrefix = normalizedDiscoveryPrefix; changed = true; }
        var normalizedStatusTopic = NormalizeTopic(HomeAssistantStatusTopic, "HomeAssistantStatusTopic");
        if (!string.Equals(normalizedStatusTopic, HomeAssistantStatusTopic, StringComparison.Ordinal)) { HomeAssistantStatusTopic = normalizedStatusTopic; changed = true; }

        if (string.IsNullOrWhiteSpace(InstanceId))
        {
            InstanceId = CreateInstanceId();
            changed = true;
        }
        else
        {
            var normalized = SanitizeId(InstanceId);
            if (!string.Equals(normalized, InstanceId, StringComparison.Ordinal)) { InstanceId = normalized; changed = true; }
        }

        var normalizedClientId = string.IsNullOrWhiteSpace(ClientId) ? "voicemeeter-service" : SanitizeId(ClientId);
        if (!string.Equals(normalizedClientId, ClientId, StringComparison.Ordinal)) { ClientId = normalizedClientId; changed = true; }

        EffectiveClientId = BuildEffectiveClientId(ClientId, InstanceId);

        EffectiveBaseTopic = UseInstanceInBaseTopic && !BaseTopic.EndsWith($"/{InstanceId}", StringComparison.Ordinal)
            ? $"{BaseTopic}/{InstanceId}"
            : BaseTopic;

        return changed;
    }


    private static string NormalizeTopicPrefix(string value, string propertyName)
    {
        value = value.Trim().Trim('/');
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{propertyName} cannot be empty.");
        ValidateTopicCharacters(value, propertyName);
        return value;
    }

    private static string NormalizeTopic(string value, string propertyName)
    {
        value = value.Trim().Trim('/');
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{propertyName} cannot be empty.");
        ValidateTopicCharacters(value, propertyName);
        return value;
    }

    private static void ValidateTopicCharacters(string value, string propertyName)
    {
        if (value.Contains('+') || value.Contains('#') || value.Contains('\0'))
            throw new InvalidOperationException($"{propertyName} cannot contain MQTT wildcards (+/#) or NUL characters.");
    }


    private static string BuildEffectiveClientId(string clientId, string instanceId)
    {
        var candidate = clientId.EndsWith($"-{instanceId}", StringComparison.Ordinal)
            ? clientId
            : $"{clientId}-{instanceId}";

        // MQTT 3.1.1 servers are only required to accept ClientIds of 1-23 characters
        // drawn from 0-9, a-z and A-Z. Preserve readability while using a deterministic
        // hash to keep installations unique even when the configured IDs are long.
        if (candidate.Length <= 23 && candidate.All(char.IsAsciiLetterOrDigit))
            return candidate;

        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(candidate));
        var suffix = Convert.ToHexString(digest.AsSpan(0, 6)).ToLowerInvariant(); // 12 alphanumeric chars
        const int maxLength = 23;
        var prefixLength = maxLength - suffix.Length;
        var readable = new string(clientId.Where(char.IsAsciiLetterOrDigit).ToArray()).ToLowerInvariant();
        if (string.IsNullOrEmpty(readable))
            readable = "vmq";
        var prefix = readable.Length > prefixLength ? readable[..prefixLength] : readable;
        return prefix + suffix;
    }

    private static string CreateInstanceId()
    {
        var machine = SanitizeId(Environment.MachineName);
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return $"{machine}-{suffix}";
    }

    private static string SanitizeId(string value)
    {
        var sb = new StringBuilder(value.Length);
        foreach (var c in value.Trim().ToLowerInvariant())
        {
            if (char.IsAsciiLetterOrDigit(c) || c is '-' or '_')
                sb.Append(c);
            else if (sb.Length == 0 || sb[^1] != '-')
                sb.Append('-');
        }

        var result = sb.ToString().Trim('-');
        return string.IsNullOrWhiteSpace(result) ? "instance" : result;
    }
}

public sealed class BridgeRuntimeState
{
    public int SchemaVersion { get; set; } = 3;
    public bool LegacyDiscoveryCleaned { get; set; }
    public bool LegacyRetainedCleaned { get; set; }
    public string? InstanceId { get; set; }
    public string? BaseTopic { get; set; }
    public string? DiscoveryPrefix { get; set; }
    public List<string> DiscoveryTopics { get; set; } = new();
    public List<string> RetainedTopics { get; set; } = new();
}

[JsonSerializable(typeof(MqttConfig))]
[JsonSerializable(typeof(BridgeRuntimeState))]
public partial class AppJsonContext : JsonSerializerContext
{
}
