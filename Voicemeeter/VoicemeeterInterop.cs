using System.Runtime.InteropServices;
using System.Text;
using Serilog;

namespace VoicemeeterMQTT;

/// <summary>
/// P/Invoke wrapper for Voicemeeter Remote API
/// Minimal, efficient access to Voicemeeter parameters
/// </summary>
public static class VoicemeeterInterop
{
	private static IntPtr _dllHandle = IntPtr.Zero;

	private static string? GetVoicemeeterDllPath()
	{
		var possiblePaths = new[]
		{
			Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86), "VB", "Voicemeeter", "VoicemeeterRemote64.dll"),
			Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86), "VB", "Voicemeeter", "VoicemeeterRemote.dll"),
			Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "VB", "Voicemeeter", "VoicemeeterRemote64.dll"),
			Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "VB", "Voicemeeter", "VoicemeeterRemote.dll"),
			Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86), "VB-Audio", "Voicemeeter", "VoicemeeterRemote64.dll"),
			Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86), "VB-Audio", "Voicemeeter", "VoicemeeterRemote.dll"),
			Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "VB-Audio", "Voicemeeter", "VoicemeeterRemote64.dll"),
			Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "VB-Audio", "Voicemeeter", "VoicemeeterRemote.dll"),
			"VoicemeeterRemote64.dll",
			"VoicemeeterRemote.dll"
		};

		foreach (var path in possiblePaths)
		{
			if (File.Exists(path))
			{
				Log.Debug("Found Voicemeeter DLL at {Path}", path);
				return path;
			}
		}
		return null;
	}

	public static bool LoadDll()
	{
		if (_dllHandle != IntPtr.Zero) return true;

		string? dllPath = GetVoicemeeterDllPath();
		if (dllPath == null) return false;

		try
		{
			_dllHandle = NativeLibrary.Load(dllPath);
			Log.Information("Loaded VoicemeeterRemote.dll from {Path}", dllPath);
			InitializeDelegates();
			return true;
		}
		catch (Exception ex)
		{
			Log.Error(ex, "Failed to load VoicemeeterRemote.dll");
			return false;
		}
	}

	public static void UnloadDll()
	{
		if (_dllHandle != IntPtr.Zero)
		{
			try
			{
				NativeLibrary.Free(_dllHandle);
				_dllHandle = IntPtr.Zero;
				// Clear delegates
				_login = null;
				_logout = null;
				_getParameterFloat = null;
				_setParameterFloat = null;
				_isParameterQueueNotEmpty = null;
				_getLevel = null;
				_isParametersDirty = null;
				_getParameterString = null;
				Log.Debug("Unloaded VoicemeeterRemote.dll");
			}
			catch (Exception ex)
			{
				Log.Error(ex, "Error unloading VoicemeeterRemote.dll");
			}
		}
	}

	private static T? GetDelegate<T>(string functionName) where T : Delegate
	{
		if (_dllHandle == IntPtr.Zero) return null;
		try
		{
			IntPtr functionPtr = NativeLibrary.GetExport(_dllHandle, functionName);
			return (T?)Marshal.GetDelegateForFunctionPointer(functionPtr, typeof(T));
		}
		catch { return null; }
	}

	[UnmanagedFunctionPointer(CallingConvention.StdCall)]
	private delegate int VbvmrLogin();

	[UnmanagedFunctionPointer(CallingConvention.StdCall)]
	private delegate int VbvmrLogout();

	[UnmanagedFunctionPointer(CallingConvention.StdCall)]
	private delegate int VbvmrGetParameterFloat([MarshalAs(UnmanagedType.LPStr)] string paramName, out float value);

	[UnmanagedFunctionPointer(CallingConvention.StdCall)]
	private delegate int VbvmrSetParameterFloat([MarshalAs(UnmanagedType.LPStr)] string paramName, float value);

	[UnmanagedFunctionPointer(CallingConvention.StdCall)]
	private delegate int VbvmrIsParameterQueueNotEmpty();

	[UnmanagedFunctionPointer(CallingConvention.StdCall)]
	private delegate int VbvmrGetLevel(int nuType, int nuChannel, out float value);

	[UnmanagedFunctionPointer(CallingConvention.StdCall)]
	private delegate int VbvmrIsParametersDirty();

	[UnmanagedFunctionPointer(CallingConvention.StdCall)]
	private delegate int VbvmrGetParameterStringA([MarshalAs(UnmanagedType.LPStr)] string paramName, [MarshalAs(UnmanagedType.LPStr)] StringBuilder value);

	private static VbvmrLogin? _login;
	private static VbvmrLogout? _logout;
	private static VbvmrGetParameterFloat? _getParameterFloat;
	private static VbvmrSetParameterFloat? _setParameterFloat;
	private static VbvmrIsParameterQueueNotEmpty? _isParameterQueueNotEmpty;
	private static VbvmrGetLevel? _getLevel;
	private static VbvmrIsParametersDirty? _isParametersDirty;
	private static VbvmrGetParameterStringA? _getParameterString;

	private static void InitializeDelegates()
	{
		_login = GetDelegate<VbvmrLogin>("VBVMR_Login");
		_logout = GetDelegate<VbvmrLogout>("VBVMR_Logout");
		_getParameterFloat = GetDelegate<VbvmrGetParameterFloat>("VBVMR_GetParameterFloat");
		_setParameterFloat = GetDelegate<VbvmrSetParameterFloat>("VBVMR_SetParameterFloat");
		_isParameterQueueNotEmpty = GetDelegate<VbvmrIsParameterQueueNotEmpty>("VBVMR_IsParameterQueueNotEmpty");
		_getLevel = GetDelegate<VbvmrGetLevel>("VBVMR_GetLevel");
		_getParameterString = GetDelegate<VbvmrGetParameterStringA>("VBVMR_GetParameterStringA");
		_isParametersDirty = GetDelegate<VbvmrIsParametersDirty>("VBVMR_IsParametersDirty");
	}

	public static int VBVMR_Login() => _login?.Invoke() ?? -1;
	public static int VBVMR_Logout() => _logout?.Invoke() ?? -1;
	public static int VBVMR_GetParameterFloat(string paramName, out float value)
	{
		value = 0;
		return _getParameterFloat?.Invoke(paramName, out value) ?? -1;
	}
	public static int VBVMR_SetParameterFloat(string paramName, float value) => _setParameterFloat?.Invoke(paramName, value) ?? -1;
	public static int VBVMR_IsParameterQueueNotEmpty() => _isParameterQueueNotEmpty?.Invoke() ?? -1;
	public static int VBVMR_GetLevel(int nuType, int nuChannel, out float value)
	{
		value = 0;
		return _getLevel?.Invoke(nuType, nuChannel, out value) ?? -1;
	}
	public static int VBVMR_GetParameterString(string paramName, StringBuilder value) => _getParameterString?.Invoke(paramName, value) ?? -1;
	public static int VBVMR_IsParametersDirty() => _isParametersDirty?.Invoke() ?? -1;
}

/// <summary>
/// Wrapper class for safe Voicemeeter API access
/// </summary>
public class VoicemeeterAPI : IDisposable
{
	private bool _isInitialized = false;
	private readonly object _lockObj = new();

	public bool Initialize()
	{
		lock (_lockObj)
		{
			try
			{
				int result = VoicemeeterInterop.VBVMR_Login();
				_isInitialized = result == 0;

				if (_isInitialized)
				{
					Log.Information("Voicemeeter API initialized successfully");
				}
				else
				{
					Log.Error("Failed to initialize Voicemeeter API. Code: {ResultCode}", result);
				}

				return _isInitialized;
			}
			catch (DllNotFoundException)
			{
				Log.Error("VoicemeeterRemote.dll not found. Please install Voicemeeter.");
				return false;
			}
			catch (Exception ex)
			{
				Log.Error(ex, "Error initializing Voicemeeter API");
				return false;
			}
		}
	}

	public bool Update()
	{
		if (!_isInitialized) return false;

		lock (_lockObj)
		{
			try
			{
				return VoicemeeterInterop.VBVMR_IsParametersDirty() == 1;
			}
			catch (Exception ex)
			{
				Log.Debug(ex, "Error checking if parameters are dirty");
				return false;
			}
		}
	}

	public float? GetParameter(string paramName)
	{
		if (!_isInitialized) return null;

		lock (_lockObj)
		{
			try
			{
				int result = VoicemeeterInterop.VBVMR_GetParameterFloat(paramName, out float value);
				return result == 0 ? value : null;
			}
			catch (Exception ex)
			{
				Log.Debug(ex, "Error getting parameter {ParamName}", paramName);
				return null;
			}
		}
	}

	public bool SetParameter(string paramName, float value)
	{
		if (!_isInitialized) return false;

		lock (_lockObj)
		{
			try
			{
				int result = VoicemeeterInterop.VBVMR_SetParameterFloat(paramName, value);
				return result == 0;
			}
			catch (Exception ex)
			{
				Log.Debug(ex, "Error setting parameter {ParamName} to {Value}", paramName, value);
				return false;
			}
		}
	}

	public float? GetLevel(int levelType, int channel)
	{
		if (!_isInitialized) return null;

		lock (_lockObj)
		{
			try
			{
				int result = VoicemeeterInterop.VBVMR_GetLevel(levelType, channel, out float value);
				return result == 0 ? value : null;
			}
			catch (Exception ex)
			{
				Log.Debug(ex, "Error getting level type={LevelType} channel={Channel}", levelType, channel);
				return null;
			}
		}
	}

	public string? GetParameterString(string paramName)
	{
		if (!_isInitialized) return null;

		lock (_lockObj)
		{
			try
			{
				var sb = new StringBuilder(512);
				int result = VoicemeeterInterop.VBVMR_GetParameterString(paramName, sb);
				if (result == 0)
				{
					string value = sb.ToString();
					return string.IsNullOrWhiteSpace(value) ? null : value;
				}

				return null;
			}
			catch (Exception ex)
			{
				Log.Debug(ex, "Error getting string parameter {ParamName}", paramName);
				return null;
			}
		}
	}

	public void Dispose()
	{
		lock (_lockObj)
		{
			if (_isInitialized)
			{
				try
				{
					VoicemeeterInterop.VBVMR_Logout();
					_isInitialized = false;
					Log.Information("Voicemeeter API disconnected");
				}
				catch (Exception ex)
				{
					Log.Error(ex, "Error during Voicemeeter logout");
				}
			}
		}
	}
}
