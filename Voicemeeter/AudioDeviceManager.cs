using NAudio.CoreAudioApi;
using Serilog;

namespace VoicemeeterMQTT;

/// <summary>
/// Polls Windows Core Audio independently from the Voicemeeter backend.
/// Invalid reads remain unknown rather than being converted into false/zero state.
/// </summary>
public sealed class AudioDeviceManager : IDisposable
{
    private readonly object _sync = new();
    private MMDeviceEnumerator? _enumerator;
    private Dictionary<string, AudioDevice> _deviceCache = new();
    private bool _disposed;

    public bool IsInitialized { get; private set; }

    public bool Initialize()
    {
        lock (_sync)
        {
            if (_disposed)
                return false;
            if (IsInitialized && _enumerator != null)
                return true;

            try
            {
                _enumerator?.Dispose();
                _enumerator = new MMDeviceEnumerator();
                IsInitialized = true;
                if (!RefreshDevicesLocked())
                    return false;
                Log.Information("Audio device manager initialized. Found {DeviceCount} devices", _deviceCache.Count);
                return true;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error initializing audio device manager");
                IsInitialized = false;
                _enumerator?.Dispose();
                _enumerator = null;
                return false;
            }
        }
    }

    public bool RefreshDevices()
    {
        lock (_sync)
        {
            if (!IsInitialized || _enumerator == null)
                return false;
            return RefreshDevicesLocked();
        }
    }

    private bool RefreshDevicesLocked()
    {
        if (_enumerator == null)
            return false;

        try
        {
            string? defaultRenderId = null;
            string? defaultCaptureId = null;

            try
            {
                using var defaultRender = _enumerator.GetDefaultAudioEndpoint(DataFlow.Render, Role.Multimedia);
                defaultRenderId = defaultRender.ID;
            }
            catch (Exception ex)
            {
                Log.Debug(ex, "Could not read default playback endpoint");
            }

            try
            {
                using var defaultCapture = _enumerator.GetDefaultAudioEndpoint(DataFlow.Capture, Role.Multimedia);
                defaultCaptureId = defaultCapture.ID;
            }
            catch (Exception ex)
            {
                Log.Debug(ex, "Could not read default capture endpoint");
            }

            var next = new Dictionary<string, AudioDevice>(StringComparer.Ordinal);
            ReadFlow(DataFlow.Render, "playback", defaultRenderId, next);
            ReadFlow(DataFlow.Capture, "capture", defaultCaptureId, next);
            _deviceCache = next;
            return true;
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Error refreshing audio devices");
            IsInitialized = false;
            return false;
        }
    }

    private void ReadFlow(DataFlow flow, string type, string? defaultId, Dictionary<string, AudioDevice> target)
    {
        if (_enumerator == null)
            return;

        using var collection = _enumerator.EnumerateAudioEndPoints(flow, DeviceState.Active);
        foreach (var device in collection)
        {
                try
                {
                    _deviceCache.TryGetValue(device.ID, out var previous);
                    var audioDevice = new AudioDevice
                    {
                        Id = device.ID,
                        Name = device.FriendlyName,
                        Type = type,
                        IsDefault = defaultId != null && string.Equals(device.ID, defaultId, StringComparison.Ordinal),
                        // Preserve last-known values if one COM property read fails transiently.
                        // New endpoints still remain unknown until their first successful read.
                        PeakLevel = previous?.PeakLevel,
                        Volume = previous?.Volume,
                        IsMuted = previous?.IsMuted
                    };

                    try
                    {
                        audioDevice.PeakLevel = device.AudioMeterInformation?.MasterPeakValue;
                    }
                    catch (Exception ex)
                    {
                        Log.Debug(ex, "Could not read peak level for audio device {Device}", device.FriendlyName);
                    }

                    try
                    {
                        if (device.AudioEndpointVolume != null)
                        {
                            audioDevice.Volume = device.AudioEndpointVolume.MasterVolumeLevelScalar * 100.0;
                            audioDevice.IsMuted = device.AudioEndpointVolume.Mute;
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Debug(ex, "Could not read volume/mute for audio device {Device}", device.FriendlyName);
                    }

                    target[device.ID] = audioDevice;
                }
            finally
            {
                device.Dispose();
            }
        }
    }

    public IReadOnlyList<AudioDevice> GetDevices()
    {
        lock (_sync)
            return _deviceCache.Values.Select(device => device with { }).ToArray();
    }

    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed)
                return;
            _enumerator?.Dispose();
            _enumerator = null;
            IsInitialized = false;
            _disposed = true;
        }
    }
}

public sealed record AudioDevice
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public double? Volume { get; set; }
    public bool? IsMuted { get; set; }
    public bool IsDefault { get; set; }
    public float? PeakLevel { get; set; }
}
