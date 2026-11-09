using System.Net;
using System.Net.Sockets;

namespace QueueFix.Sockets;

/// <summary>
/// Settings describing a TCP endpoint: the remote endpoint an <see cref="Initiator"/> connects to,
/// or the local endpoint a <see cref="Listener"/> binds to.
/// </summary>
public class Configuration
{
    /// <summary>Host name or IP address of the remote endpoint, e.g. "localhost" or "10.0.0.5".</summary>
    public required string Host { get; init; }

    /// <summary>TCP port of the remote endpoint (1-65535).</summary>
    public required int Port { get; init; }

    /// <summary>Maximum time allowed to establish the connection. Defaults to 30 seconds.</summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Size in bytes of the buffer used for each receive. Defaults to 1024.</summary>
    public int ReceiveBufferSize { get; init; } = 1_024;

    /// <summary>Disables Nagle's algorithm so small messages are sent immediately. Defaults to true.</summary>
    public bool NoDelay { get; init; } = true;

    /// <summary>Maximum length of the pending-connections queue for a <see cref="Listener"/>. Defaults to 100.</summary>
    public int Backlog { get; init; } = 100;

    /// <summary>
    /// Resolves <see cref="Host"/> to its IP addresses. An IP literal (e.g. "0.0.0.0", "::") is used as-is.
    /// </summary>
    internal async Task<IPAddress[]> ResolveAddressesAsync(CancellationToken cancellationToken)
    {
        IPAddress[] addresses = IPAddress.TryParse(Host, out var literal)
            ? [literal]
            : (await Dns.GetHostEntryAsync(Host, cancellationToken).ConfigureAwait(false)).AddressList;

        return addresses.Length > 0
            ? addresses
            : throw new SocketException((int)SocketError.HostNotFound);
    }

    /// <summary>Throws if any setting is invalid.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Host))
            throw new ArgumentException("Host must be specified.", nameof(Host));
        if (Port is < 1 or > 65_535)
            throw new ArgumentOutOfRangeException(nameof(Port), Port, "Port must be between 1 and 65535.");
        if (ConnectTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ConnectTimeout), ConnectTimeout, "ConnectTimeout must be positive.");
        if (ReceiveBufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(ReceiveBufferSize), ReceiveBufferSize, "ReceiveBufferSize must be positive.");
        if (Backlog <= 0)
            throw new ArgumentOutOfRangeException(nameof(Backlog), Backlog, "Backlog must be positive.");
    }
}
