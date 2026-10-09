using System.Globalization;

namespace VoicemeeterMQTT;

public static class ProtocolValues
{
    public const float MinGainDb = -60.0f;
    public const float MaxGainDb = 12.0f;
    public const float GainStepDb = 0.5f;

    public static string FormatFloat(float value) => value.ToString("0.###", CultureInfo.InvariantCulture);

    public static bool TryParseGain(string payload, out float gain)
    {
        if (!float.TryParse(payload, NumberStyles.Float, CultureInfo.InvariantCulture, out gain) ||
            !float.IsFinite(gain) || gain < MinGainDb || gain > MaxGainDb)
            return false;

        var steps = (gain - MinGainDb) / GainStepDb;
        return Math.Abs(steps - MathF.Round(steps)) < 0.001f;
    }

    public static bool TryParseMute(string payload, out float value)
    {
        if (payload == "1")
        {
            value = 1.0f;
            return true;
        }
        if (payload == "0")
        {
            value = 0.0f;
            return true;
        }
        value = 0;
        return false;
    }
}
