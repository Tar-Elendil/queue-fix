using System.Net;
using System.Net.Sockets;

namespace QueueFix.Sockets;

/// <summary>
/// Opens a TCP client connection to the endpoint described by a <see cref="Configuration"/>.
/// Based on the client pattern in
/// https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/sockets/socket-services
/// </summary>
public sealed class Initiator : IAsyncDisposable, IDisposable
{
    private readonly Configuration _configuration;
    private Connection? _connection;

    public Initiator(Configuration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        configuration.Validate();
        _configuration = configuration;
    }

    /// <summary>True while the underlying socket is connected.</summary>
    public bool IsConnected => _connection?.IsConnected ?? false;

    /// <summary>The remote endpoint of the active connection, if any.</summary>
    public EndPoint? RemoteEndPoint => _connection?.RemoteEndPoint;

    /// <summary>
    /// Resolves the configured host and connects to the first address that accepts the connection.
    /// </summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (IsConnected)
            throw new InvalidOperationException("Initiator is already connected.");

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_configuration.ConnectTimeout);
        var token = timeoutCts.Token;

        try
        {
            var addresses = await _configuration.ResolveAddressesAsync(token).ConfigureAwait(false);

            Exception? lastError = null;
            foreach (var address in addresses)
            {
                var endPoint = new IPEndPoint(address, _configuration.Port);
                var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                {
                    NoDelay = _configuration.NoDelay
                };

                try
                {
                    await socket.ConnectAsync(endPoint, token).ConfigureAwait(false);
                    _connection = new Connection(socket, _configuration.ReceiveBufferSize);
                    return;
                }
                catch (SocketException ex)
                {
                    socket.Dispose();
                    lastError = ex;
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }
            }

            throw lastError!;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Timed out connecting to {_configuration.Host}:{_configuration.Port} after {_configuration.ConnectTimeout}.");
        }
    }

    /// <summary>Sends all of <paramref name="data"/> over the connection.</summary>
    public Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
        GetConnection().SendAsync(data, cancellationToken);

    /// <summary>Encodes <paramref name="message"/> as UTF-8 and sends it.</summary>
    public Task SendAsync(string message, CancellationToken cancellationToken = default) =>
        GetConnection().SendAsync(message, cancellationToken);

    /// <summary>
    /// Receives up to <see cref="Configuration.ReceiveBufferSize"/> bytes.
    /// Returns an empty array when the remote side has closed the connection.
    /// </summary>
    public Task<byte[]> ReceiveAsync(CancellationToken cancellationToken = default) =>
        GetConnection().ReceiveAsync(cancellationToken);

    /// <summary>Shuts down send and receive, then closes the socket.</summary>
    public void Disconnect() => Interlocked.Exchange(ref _connection, null)?.Disconnect();

    public void Dispose() => Disconnect();

    public ValueTask DisposeAsync()
    {
        Disconnect();
        return ValueTask.CompletedTask;
    }

    private Connection GetConnection() =>
        _connection is { IsConnected: true } connection
            ? connection
            : throw new InvalidOperationException("Initiator is not connected. Call ConnectAsync first.");
}
