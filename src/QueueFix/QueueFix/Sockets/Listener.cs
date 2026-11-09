using System.Net;
using System.Net.Sockets;
using System.Text;

namespace QueueFix.Sockets;

/// <summary>
/// Listens for incoming TCP connections on the local endpoint described by a <see cref="Configuration"/>.
/// Based on the server pattern in
/// https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/sockets/socket-services
/// </summary>
/// <remarks>
/// Set <see cref="Configuration.Host"/> to "0.0.0.0" (all IPv4 interfaces), "::" (all IPv6 interfaces),
/// a specific IP address, or a host name (binds to the first address it resolves to).
/// </remarks>
public sealed class Listener : IAsyncDisposable, IDisposable
{
    private readonly Configuration _configuration;
    private Socket? _socket;

    public Listener(Configuration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();
        _configuration = configuration;
    }

    /// <summary>True after <see cref="StartAsync"/> succeeds and until <see cref="Stop"/> is called.</summary>
    public bool IsListening => _socket is not null;

    /// <summary>The local endpoint the listener is bound to, if listening.</summary>
    public EndPoint? LocalEndPoint => _socket?.LocalEndPoint;

    /// <summary>Binds to the configured endpoint and starts listening for connections.</summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (IsListening)
            throw new InvalidOperationException("Listener is already listening.");

        var addresses = await _configuration.ResolveAddressesAsync(cancellationToken).ConfigureAwait(false);
        var endPoint = new IPEndPoint(addresses[0], _configuration.Port);

        var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            socket.Bind(endPoint);
            socket.Listen(_configuration.Backlog);
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        _socket = socket;
    }

    /// <summary>Waits for and accepts the next incoming connection.</summary>
    public async Task<Connection> AcceptAsync(CancellationToken cancellationToken = default)
    {
        var socket = _socket ?? throw new InvalidOperationException("Listener is not listening. Call StartAsync first.");

        var handler = await socket.AcceptAsync(cancellationToken).ConfigureAwait(false);
        handler.NoDelay = _configuration.NoDelay;
        return new Connection(handler, _configuration.ReceiveBufferSize);
    }

    /// <summary>
    /// Stops listening and closes the listening socket. Connections already accepted stay open
    /// and must be closed separately. A pending <see cref="AcceptAsync"/> fails with an exception.
    /// </summary>
    public void Stop() => Interlocked.Exchange(ref _socket, null)?.Dispose();

    public void Dispose() => Stop();

    public ValueTask DisposeAsync()
    {
        Stop();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// A connected TCP socket. Created by <see cref="Initiator.ConnectAsync"/> (client side)
/// or <see cref="Listener.AcceptAsync"/> (server side).
/// </summary>
public sealed class Connection : IAsyncDisposable, IDisposable
{
    private readonly Socket _socket;
    private readonly int _receiveBufferSize;
    private int _disposed;

    internal Connection(Socket socket, int receiveBufferSize)
    {
        _socket = socket;
        _receiveBufferSize = receiveBufferSize;
    }

    /// <summary>True while the underlying socket is connected.</summary>
    public bool IsConnected => Volatile.Read(ref _disposed) == 0 && _socket.Connected;

    public EndPoint? RemoteEndPoint => _socket.RemoteEndPoint;

    public EndPoint? LocalEndPoint => _socket.LocalEndPoint;

    /// <summary>Sends all of <paramref name="data"/> over the connection.</summary>
    public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        while (!data.IsEmpty)
        {
            int sent = await _socket.SendAsync(data, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            data = data[sent..];
        }
    }

    /// <summary>Encodes <paramref name="message"/> as UTF-8 and sends it.</summary>
    public Task SendAsync(string message, CancellationToken cancellationToken = default) =>
        SendAsync(Encoding.UTF8.GetBytes(message), cancellationToken);

    /// <summary>
    /// Receives up to <see cref="Configuration.ReceiveBufferSize"/> bytes.
    /// Returns an empty array when the remote side has closed the connection.
    /// </summary>
    public async Task<byte[]> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var buffer = new byte[_receiveBufferSize];
        int received = await _socket.ReceiveAsync(buffer, SocketFlags.None, cancellationToken).ConfigureAwait(false);
        return buffer[..received];
    }

    /// <summary>Shuts down send and receive, then closes the socket.</summary>
    public void Disconnect()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try
        {
            if (_socket.Connected)
                _socket.Shutdown(SocketShutdown.Both);
        }
        catch (SocketException)
        {
            // The peer may already have closed the connection.
        }
        finally
        {
            _socket.Dispose();
        }
    }

    public void Dispose() => Disconnect();

    public ValueTask DisposeAsync()
    {
        Disconnect();
        return ValueTask.CompletedTask;
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
            throw new InvalidOperationException("Connection has been closed.");
    }
}
