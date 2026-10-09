using System.Runtime.InteropServices;
using System.Text;
using Serilog;

namespace VoicemeeterMQTT;

public enum VoicemeeterPollResult
{
    Unavailable,
    NoChanges,
    Changed
}

public readonly record struct VoicemeeterTopology(int StripCount, int BusCount, int Type);

/// <summary>
/// Dynamic wrapper for Voicemeeter Remote API. DLL load/unload has one synchronized owner.
/// The library is intentionally kept loaded during normal service operation.
/// </summary>
public static class VoicemeeterInterop
{
    private static readonly object LifecycleLock = new();
    private static IntPtr _dllHandle = IntPtr.Zero;

    [UnmanagedFunctionPointer(CallingConvention.StdCall)] private delegate int VbvmrLogin();
    [UnmanagedFunctionPointer(CallingConvention.StdCall)] private delegate int VbvmrLogout();
    [UnmanagedFunctionPointer(CallingConvention.StdCall)] private delegate int VbvmrGetParameterFloat([MarshalAs(UnmanagedType.LPStr)] string paramName, out float value);
    [UnmanagedFunctionPointer(CallingConvention.StdCall)] private delegate int VbvmrSetParameterFloat([MarshalAs(UnmanagedType.LPStr)] string paramName, float value);
    [UnmanagedFunctionPointer(CallingConvention.StdCall)] private delegate int VbvmrGetLevel(int nuType, int nuChannel, out float value);
    [UnmanagedFunctionPointer(CallingConvention.StdCall)] private delegate int VbvmrIsParametersDirty();
    [UnmanagedFunctionPointer(CallingConvention.StdCall)] private delegate int VbvmrGetParameterStringA([MarshalAs(UnmanagedType.LPStr)] string paramName, [MarshalAs(UnmanagedType.LPStr)] StringBuilder value);
    [UnmanagedFunctionPointer(CallingConvention.StdCall)] private delegate int VbvmrGetVoicemeeterType(out int type);

    private static VbvmrLogin? _login;
    private static VbvmrLogout? _logout;
    private static VbvmrGetParameterFloat? _getParameterFloat;
    private static VbvmrSetParameterFloat? _setParameterFloat;
    private static VbvmrGetLevel? _getLevel;
    private static VbvmrIsParametersDirty? _isParametersDirty;
    private static VbvmrGetParameterStringA? _getParameterString;
    private static VbvmrGetVoicemeeterType? _getVoicemeeterType;

    public static bool IsLoaded
    {
        get
        {
            lock (LifecycleLock)
                return _dllHandle != IntPtr.Zero;
        }
    }

    public static bool LoadDll()
    {
        lock (LifecycleLock)
        {
            if (_dllHandle != IntPtr.Zero)
                return true;

            foreach (var candidate in GetDllCandidates())
            {
                if (Path.IsPathRooted(candidate) && !File.Exists(candidate))
                    continue;

                IntPtr handle = IntPtr.Zero;
                try
                {
                    handle = NativeLibrary.Load(candidate);
                    _dllHandle = handle;
                    BindDelegatesOrThrow();
                    Log.Information("Loaded VoicemeeterRemote DLL from {Path}", candidate);
                    return true;
                }
                catch (Exception ex)
                {
                    Log.Debug(ex, "Could not use VoicemeeterRemote DLL candidate {Path}", candidate);
                    ClearDelegates();
                    _dllHandle = IntPtr.Zero;
                    if (handle != IntPtr.Zero)
                    {
                        try { NativeLibrary.Free(handle); } catch { }
                    }
                }
            }

            return false;
        }
    }

    public static void UnloadDll()
    {
        lock (LifecycleLock)
        {
            if (_dllHandle == IntPtr.Zero)
                return;

            try
            {
                NativeLibrary.Free(_dllHandle);
                Log.Debug("Unloaded VoicemeeterRemote DLL");
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Error unloading VoicemeeterRemote DLL");
            }
            finally
            {
                _dllHandle = IntPtr.Zero;
                ClearDelegates();
            }
        }
    }

    private static IEnumerable<string> GetDllCandidates()
    {
        var programFilesX86 = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86);
        var programFiles = Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles);
        var names = Environment.Is64BitProcess
            ? new[] { "VoicemeeterRemote64.dll", "VoicemeeterRemote.dll" }
            : new[] { "VoicemeeterRemote.dll", "VoicemeeterRemote64.dll" };

        foreach (var root in new[] { programFilesX86, programFiles })
        {
            foreach (var vendor in new[] { "VB", "VB-Audio" })
            {
                foreach (var name in names)
                    yield return Path.Combine(root, vendor, "Voicemeeter", name);
            }
        }

        // Also allow normal Windows DLL search semantics.
        foreach (var name in names)
            yield return name;
    }

    private static void BindDelegatesOrThrow()
    {
        _login = GetRequiredDelegate<VbvmrLogin>("VBVMR_Login");
        _logout = GetRequiredDelegate<VbvmrLogout>("VBVMR_Logout");
        _getParameterFloat = GetRequiredDelegate<VbvmrGetParameterFloat>("VBVMR_GetParameterFloat");
        _setParameterFloat = GetRequiredDelegate<VbvmrSetParameterFloat>("VBVMR_SetParameterFloat");
        _getLevel = GetRequiredDelegate<VbvmrGetLevel>("VBVMR_GetLevel");
        _getParameterString = GetRequiredDelegate<VbvmrGetParameterStringA>("VBVMR_GetParameterStringA");
        _isParametersDirty = GetRequiredDelegate<VbvmrIsParametersDirty>("VBVMR_IsParametersDirty");
        _getVoicemeeterType = GetOptionalDelegate<VbvmrGetVoicemeeterType>("VBVMR_GetVoicemeeterType");
    }

    private static T GetRequiredDelegate<T>(string name) where T : Delegate
    {
        if (_dllHandle == IntPtr.Zero || !NativeLibrary.TryGetExport(_dllHandle, name, out var address))
            throw new MissingMethodException($"Required Voicemeeter export '{name}' was not found.");
        return Marshal.GetDelegateForFunctionPointer<T>(address);
    }

    private static T? GetOptionalDelegate<T>(string name) where T : Delegate
    {
        if (_dllHandle == IntPtr.Zero || !NativeLibrary.TryGetExport(_dllHandle, name, out var address))
            return null;
        return Marshal.GetDelegateForFunctionPointer<T>(address);
    }

    private static void ClearDelegates()
    {
        _login = null;
        _logout = null;
        _getParameterFloat = null;
        _setParameterFloat = null;
        _getLevel = null;
        _isParametersDirty = null;
        _getParameterString = null;
        _getVoicemeeterType = null;
    }

    // Every native call shares the same lifecycle lock as load/unload. This guarantees that
    // NativeLibrary.Free cannot run while a delegate is executing, even if this static wrapper
    // is used outside VoicemeeterAPI in the future.
    public static int VBVMR_Login()
    {
        lock (LifecycleLock)
            return _login?.Invoke() ?? -1;
    }

    public static int VBVMR_Logout()
    {
        lock (LifecycleLock)
            return _logout?.Invoke() ?? -1;
    }

    public static int VBVMR_GetParameterFloat(string name, out float value)
    {
        lock (LifecycleLock)
        {
            value = 0;
            return _getParameterFloat?.Invoke(name, out value) ?? -1;
        }
    }

    public static int VBVMR_SetParameterFloat(string name, float value)
    {
        lock (LifecycleLock)
            return _setParameterFloat?.Invoke(name, value) ?? -1;
    }

    public static int VBVMR_GetLevel(int type, int channel, out float value)
    {
        lock (LifecycleLock)
        {
            value = 0;
            return _getLevel?.Invoke(type, channel, out value) ?? -1;
        }
    }

    public static int VBVMR_GetParameterString(string name, StringBuilder value)
    {
        lock (LifecycleLock)
            return _getParameterString?.Invoke(name, value) ?? -1;
    }

    public static int VBVMR_IsParametersDirty()
    {
        lock (LifecycleLock)
            return _isParametersDirty?.Invoke() ?? -1;
    }

    public static int VBVMR_GetVoicemeeterType(out int type)
    {
        lock (LifecycleLock)
        {
            type = 0;
            return _getVoicemeeterType?.Invoke(out type) ?? -1;
        }
    }
}

/// <summary>
/// Serialized access to the Voicemeeter API. It does not unload the native DLL while calls can run.
/// </summary>
public sealed class VoicemeeterAPI : IDisposable
{
    private readonly object _lockObj = new();
    private bool _isInitialized;

    public bool IsInitialized
    {
        get { lock (_lockObj) return _isInitialized; }
    }

    public bool Initialize()
    {
        lock (_lockObj)
        {
            if (_isInitialized)
                return true;
            if (!VoicemeeterInterop.LoadDll())
                return false;

            try
            {
                var result = VoicemeeterInterop.VBVMR_Login();
                _isInitialized = result == 0;
                if (_isInitialized)
                    Log.Information("Voicemeeter API initialized successfully");
                else
                    Log.Warning("Voicemeeter API login failed with code {ResultCode}", result);
                return _isInitialized;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error initializing Voicemeeter API");
                _isInitialized = false;
                return false;
            }
        }
    }

    public VoicemeeterPollResult Poll()
    {
        lock (_lockObj)
        {
            if (!_isInitialized)
                return VoicemeeterPollResult.Unavailable;

            try
            {
                var result = VoicemeeterInterop.VBVMR_IsParametersDirty();
                if (result == 1)
                    return VoicemeeterPollResult.Changed;
                if (result == 0)
                    return VoicemeeterPollResult.NoChanges;

                MarkUnavailable();
                Log.Warning("Voicemeeter API became unavailable (dirty result {Result})", result);
                return VoicemeeterPollResult.Unavailable;
            }
            catch (Exception ex)
            {
                MarkUnavailable();
                Log.Warning(ex, "Voicemeeter API became unavailable while polling");
                return VoicemeeterPollResult.Unavailable;
            }
        }
    }

    private void MarkUnavailable()
    {
        try { VoicemeeterInterop.VBVMR_Logout(); } catch { }
        _isInitialized = false;
    }

    public VoicemeeterTopology? GetTopology()
    {
        lock (_lockObj)
        {
            if (!_isInitialized)
                return null;

            if (VoicemeeterInterop.VBVMR_GetVoicemeeterType(out var type) == 0)
            {
                return type switch
                {
                    1 => new VoicemeeterTopology(3, 2, type),
                    2 => new VoicemeeterTopology(5, 5, type),
                    3 => new VoicemeeterTopology(8, 8, type),
                    _ => ProbeTopology(type)
                };
            }

            return ProbeTopology(0);
        }
    }

    private static VoicemeeterTopology ProbeTopology(int type)
    {
        var strips = 0;
        for (var i = 0; i < 8; i++)
        {
            if (VoicemeeterInterop.VBVMR_GetParameterFloat($"Strip[{i}].Gain", out _) != 0)
                break;
            strips++;
        }

        var buses = 0;
        for (var i = 0; i < 8; i++)
        {
            if (VoicemeeterInterop.VBVMR_GetParameterFloat($"Bus[{i}].Gain", out _) != 0)
                break;
            buses++;
        }

        return new VoicemeeterTopology(strips, buses, type);
    }

    public float? GetParameter(string paramName)
    {
        lock (_lockObj)
        {
            if (!_isInitialized)
                return null;
            try
            {
                var result = VoicemeeterInterop.VBVMR_GetParameterFloat(paramName, out var value);
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
        lock (_lockObj)
        {
            if (!_isInitialized)
                return false;
            try
            {
                return VoicemeeterInterop.VBVMR_SetParameterFloat(paramName, value) == 0;
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
        lock (_lockObj)
        {
            if (!_isInitialized)
                return null;
            try
            {
                return VoicemeeterInterop.VBVMR_GetLevel(levelType, channel, out var value) == 0 ? value : null;
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
        lock (_lockObj)
        {
            if (!_isInitialized)
                return null;
            try
            {
                var sb = new StringBuilder(512);
                if (VoicemeeterInterop.VBVMR_GetParameterString(paramName, sb) != 0)
                    return null;
                var value = sb.ToString();
                return string.IsNullOrWhiteSpace(value) ? null : value;
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
            if (!_isInitialized)
                return;
            try
            {
                VoicemeeterInterop.VBVMR_Logout();
                Log.Information("Voicemeeter API disconnected");
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Error during Voicemeeter logout");
            }
            finally
            {
                _isInitialized = false;
            }
        }
    }
}
