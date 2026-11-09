namespace VoicemeeterMQTT;

public enum AudioEngineRestartResult
{
    Succeeded,
    RequestFailed,
    RecoveryTimedOut
}

/// <summary>
/// Runs one restart and recovery check at a time, with a monotonic cooldown for repeated commands.
/// </summary>
public sealed class AudioEngineRestartCoordinator
{
    private readonly object _sync = new();
    private readonly Func<bool> _requestRestart;
    private readonly Func<CancellationToken, Task<bool>> _waitForRecovery;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _cooldown;
    private long? _lastAttempt;
    private bool _isRestarting;

    public AudioEngineRestartCoordinator(
        Func<bool> requestRestart,
        Func<CancellationToken, Task<bool>> waitForRecovery,
        TimeProvider? timeProvider = null,
        TimeSpan? cooldown = null)
    {
        ArgumentNullException.ThrowIfNull(requestRestart);
        ArgumentNullException.ThrowIfNull(waitForRecovery);
        _requestRestart = requestRestart;
        _waitForRecovery = waitForRecovery;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _cooldown = cooldown ?? TimeSpan.FromSeconds(10);
        if (_cooldown < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(cooldown));
    }

    public bool IsRestarting
    {
        get { lock (_sync) return _isRestarting; }
    }

    // A null result means that the command was suppressed, rather than queued for later execution.
    public Task<AudioEngineRestartResult>? TryRestartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            if (_isRestarting ||
                (_lastAttempt.HasValue && _timeProvider.GetElapsedTime(_lastAttempt.Value) < _cooldown))
                return null;

            _isRestarting = true;
            _lastAttempt = _timeProvider.GetTimestamp();
        }

        return RestartAsync(cancellationToken);
    }

    private async Task<AudioEngineRestartResult> RestartAsync(CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_requestRestart())
                return AudioEngineRestartResult.RequestFailed;

            return await _waitForRecovery(cancellationToken)
                ? AudioEngineRestartResult.Succeeded
                : AudioEngineRestartResult.RecoveryTimedOut;
        }
        finally
        {
            lock (_sync)
                _isRestarting = false;
        }
    }
}
