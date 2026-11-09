using SolaceSystems.Solclient.Messaging;

namespace QueueFix.Solace;

/// <summary>Lifecycle state of a <see cref="Connection"/>.</summary>
public enum ConnectionState
{
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    /// <summary>The connection was lost and all reconnect attempts failed. Call <see cref="Connection.Connect"/> to try again.</summary>
    Failed,
}

/// <summary>
/// Connects to a Solace PubSub+ broker and keeps the connection up.
/// Based on https://tutorials.solace.dev/dotnet/publish-subscribe/
/// </summary>
/// <remarks>
/// Reconnecting is done by the Solace API, using the reconnect and keep-alive settings in
/// <see cref="Configuration"/>. With the default <see cref="Configuration.ReconnectRetries"/> of -1 it
/// retries forever and re-adds subscriptions after each reconnect. <see cref="StateChanged"/> reports progress.
/// </remarks>
public sealed class Connection : IDisposable
{
    private static readonly Lock FactoryLock = new();
    private static int _factoryUsers;

    private readonly Configuration _configuration;
    private readonly Lock _lock = new();
    private IContext? _context;
    private ISession? _session;
    private ConnectionState _state = ConnectionState.Disconnected;
    private bool _factoryAcquired;
    private bool _disposed;

    public Connection(Configuration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();
        _configuration = configuration;
    }

    /// <summary>Current connection state.</summary>
    public ConnectionState State => _state;

    public bool IsConnected => _state == ConnectionState.Connected;

    /// <summary>Raised whenever <see cref="State"/> changes. Called on a Solace API thread.</summary>
    public event EventHandler<ConnectionState>? StateChanged;

    /// <summary>Raised for every session event from the broker (errors, acknowledgements and so on).</summary>
    public event EventHandler<SessionEventArgs>? SessionEvent;

    /// <summary>
    /// Raised for every message delivered to this session. Called on a Solace API thread.
    /// The message is disposed after the handlers return, so copy anything you need to keep.
    /// </summary>
    public event EventHandler<MessageEventArgs>? MessageReceived;

    /// <summary>
    /// The underlying Solace session, for publishing and subscribing.
    /// Throws if <see cref="Connect"/> has not been called.
    /// </summary>
    public ISession Session => _session ?? throw new InvalidOperationException("Not connected. Call Connect first.");

    /// <summary>
    /// Connects to the broker. Blocks until the connection is up, or until
    /// <see cref="Configuration.ConnectRetries"/> attempts have failed.
    /// </summary>
    /// <exception cref="OperationErrorException">The Solace API could not establish the connection.</exception>
    /// <exception cref="InvalidOperationException">The connection attempt returned a non-OK result code.</exception>
    public void Connect()
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_state is ConnectionState.Connected or ConnectionState.Connecting or ConnectionState.Reconnecting)
                return;

            CloseSession();
            AcquireFactory();

            SetState(ConnectionState.Connecting);
            try
            {
                _context = ContextFactory.Instance.CreateContext(new ContextProperties(), null);
                _session = _context.CreateSession(_configuration.ToSessionProperties(), OnMessage, OnSessionEvent);

                var result = _session.Connect();
                if (result != ReturnCode.SOLCLIENT_OK)
                {
                    var error = ContextFactory.Instance.GetLastSDKErrorInfo();
                    throw new InvalidOperationException(
                        $"Failed to connect to Solace broker {_configuration.Host} (VPN '{_configuration.VpnName}'): " +
                        $"{result} - {error?.ErrorStr}");
                }

                SetState(ConnectionState.Connected);
            }
            catch
            {
                CloseSession();
                SetState(ConnectionState.Disconnected);
                throw;
            }
        }
    }

    /// <summary>Disconnects from the broker. <see cref="Connect"/> can be called again afterwards.</summary>
    public void Disconnect()
    {
        lock (_lock)
        {
            if (_session is null)
                return;

            CloseSession();
            SetState(ConnectionState.Disconnected);
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed)
                return;

            _disposed = true;
            CloseSession();
            SetState(ConnectionState.Disconnected);
            ReleaseFactory();
        }
    }

    private void OnSessionEvent(object? sender, SessionEventArgs args)
    {
        switch (args.Event)
        {
            case global::SolaceSystems.Solclient.Messaging.SessionEvent.Reconnecting:
                SetState(ConnectionState.Reconnecting);
                break;
            case global::SolaceSystems.Solclient.Messaging.SessionEvent.Reconnected:
            case global::SolaceSystems.Solclient.Messaging.SessionEvent.UpNotice:
                SetState(ConnectionState.Connected);
                break;
            case global::SolaceSystems.Solclient.Messaging.SessionEvent.DownError:
                // Sent only after the API has used up its reconnect retries.
                SetState(ConnectionState.Failed);
                break;
        }

        SessionEvent?.Invoke(this, args);
    }

    private void OnMessage(object? sender, MessageEventArgs args)
    {
        using var message = args.Message;
        MessageReceived?.Invoke(this, args);
    }

    private void SetState(ConnectionState state)
    {
        if (_state == state)
            return;

        _state = state;
        StateChanged?.Invoke(this, state);
    }

    private void CloseSession()
    {
        var session = _session;
        var context = _context;
        _session = null;
        _context = null;

        if (session is not null)
        {
            session.Disconnect();
            session.Dispose();
        }

        context?.Dispose();
    }

    // ContextFactory.Init and Cleanup are process-wide, so they are reference-counted across connections.
    private void AcquireFactory()
    {
        if (_factoryAcquired)
            return;

        lock (FactoryLock)
        {
            if (_factoryUsers++ == 0)
            {
                var properties = new ContextFactoryProperties { SolClientLogLevel = _configuration.LogLevel };
                properties.LogToConsoleError();
                ContextFactory.Instance.Init(properties);
            }
        }

        _factoryAcquired = true;
    }

    private void ReleaseFactory()
    {
        if (!_factoryAcquired)
            return;

        lock (FactoryLock)
        {
            if (--_factoryUsers == 0)
                ContextFactory.Instance.Cleanup();
        }

        _factoryAcquired = false;
    }
}
