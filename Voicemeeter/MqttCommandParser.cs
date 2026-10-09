namespace VoicemeeterMQTT;

public readonly record struct MqttCommandTarget(string Kind, int Index, string Property);

public static class MqttCommandParser
{
    public static bool TryParse(string baseTopic, string topic, out MqttCommandTarget target)
    {
        target = default;
        var prefix = baseTopic.TrimEnd('/') + "/";
        if (!topic.StartsWith(prefix, StringComparison.Ordinal))
            return false;

        var relative = topic[prefix.Length..];
        var parts = relative.Split('/');
        if (parts.Length != 3 || parts.Any(string.IsNullOrEmpty) || !string.Equals(parts[2], "set", StringComparison.Ordinal))
            return false;
        if (parts[1] is not ("gain" or "muted"))
            return false;

        string kind;
        string indexText;
        if (parts[0].StartsWith("strip", StringComparison.Ordinal))
        {
            kind = "strip";
            indexText = parts[0][5..];
        }
        else if (parts[0].StartsWith("bus", StringComparison.Ordinal))
        {
            kind = "bus";
            indexText = parts[0][3..];
        }
        else
        {
            return false;
        }

        if (!int.TryParse(indexText, System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture, out var index) || index < 0)
            return false;

        // Reject aliases such as strip00. Keeping one canonical command topic per target avoids
        // accidental duplicate command namespaces and makes ACLs/auditing deterministic.
        var canonicalTarget = kind == "strip" ? $"strip{index}" : $"bus{index}";
        if (!string.Equals(parts[0], canonicalTarget, StringComparison.Ordinal))
            return false;

        target = new MqttCommandTarget(kind, index, parts[1]);
        return true;
    }
}
