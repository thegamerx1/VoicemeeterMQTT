using NAudio.CoreAudioApi;
using Serilog;

namespace VoicemeeterMQTT;

/// <summary>
/// Manages audio device state using NAudio
/// Minimal footprint - only query devices when needed
/// </summary>
public class AudioDeviceManager
{
	private MMDeviceEnumerator? _enumerator;
	private readonly Dictionary<string, AudioDevice> _deviceCache = new();
	private bool _disposed = false;

	public void Initialize()
	{
		try
		{
			_enumerator = new MMDeviceEnumerator();
			RefreshDevices();
			Log.Information("Audio device manager initialized. Found {DeviceCount} devices", _deviceCache.Count);
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Error initializing audio device manager");
		}
	}

	public void RefreshDevices()
	{
		if (_enumerator == null) return;

		try
		{
			_deviceCache.Clear();

			// Playback devices
			foreach (var device in _enumerator.EnumerateAudioEndPoints(DataFlow.Render, DeviceState.Active))
			{
				var audioDevice = new AudioDevice
				{
					Id = device.ID,
					Name = device.FriendlyName,
					Type = "playback",
					IsDefault = device.ID == _enumerator.GetDefaultAudioEndpoint(DataFlow.Render, Role.Multimedia).ID
				};

				try
				{
					if (device.AudioMeterInformation != null)
					{
						audioDevice.PeakLevel = device.AudioMeterInformation.MasterPeakValue;
					}
					if (device.AudioEndpointVolume != null)
					{
						audioDevice.Volume = device.AudioEndpointVolume.MasterVolumeLevelScalar * 100;
						audioDevice.IsMuted = device.AudioEndpointVolume.Mute;
					}
				}
				catch { /* silently ignore meter read errors */ }

				_deviceCache[device.ID] = audioDevice;
			}

			// Capture devices
			foreach (var device in _enumerator.EnumerateAudioEndPoints(DataFlow.Capture, DeviceState.Active))
			{
				var audioDevice = new AudioDevice
				{
					Id = device.ID,
					Name = device.FriendlyName,
					Type = "capture",
					IsDefault = device.ID == _enumerator.GetDefaultAudioEndpoint(DataFlow.Capture, Role.Multimedia).ID
				};

				try
				{
					if (device.AudioMeterInformation != null)
					{
						audioDevice.PeakLevel = device.AudioMeterInformation.MasterPeakValue;
					}
					if (device.AudioEndpointVolume != null)
					{
						audioDevice.Volume = device.AudioEndpointVolume.MasterVolumeLevelScalar * 100;
						audioDevice.IsMuted = device.AudioEndpointVolume.Mute;
					}
				}
				catch { /* silently ignore meter read errors */ }

				_deviceCache[device.ID] = audioDevice;
			}
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Error refreshing audio devices");
		}
	}

	public IEnumerable<AudioDevice> GetDevices() => _deviceCache.Values;

	public AudioDevice? GetDevice(string deviceId)
	{
		return _deviceCache.TryGetValue(deviceId, out var device) ? device : null;
	}

	public void Dispose()
	{
		if (!_disposed)
		{
			_enumerator?.Dispose();
			_disposed = true;
		}
	}
}

public class AudioDevice
{
	public string Id { get; set; } = string.Empty;
	public string Name { get; set; } = string.Empty;
	public string Type { get; set; } = string.Empty; // "playback" or "capture"
	public double? Volume { get; set; } // 0-100
	public bool IsMuted { get; set; }
	public bool IsDefault { get; set; }
	public float PeakLevel { get; set; } // 0-1
}
