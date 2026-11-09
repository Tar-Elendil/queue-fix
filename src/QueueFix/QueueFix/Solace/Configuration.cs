using SolaceSystems.Solclient.Messaging;

namespace QueueFix.Solace;

/// <summary>
/// Settings for connecting to a Solace PubSub+ event broker.
/// </summary>
public class Configuration
{
    /// <summary>
    /// Broker address, e.g. "tcp://localhost:55555" or "tcps://mr-xyz.messaging.solace.cloud:55443".
    /// A comma-separated list of hosts may be given for failover.
    /// </summary>
    public required string Host { get; init; }

    /// <summary>Message VPN to connect to, e.g. "default".</summary>
    public required string VpnName { get; init; }

    /// <summary>Client username.</summary>
    public required string UserName { get; init; }

    /// <summary>Client password.</summary>
    public string? Password { get; init; }

    /// <summary>Optional client name. The broker generates one when null.</summary>
    public string? ClientName { get; init; }

    /// <summary>Maximum time allowed for each connection attempt. Defaults to 30 seconds.</summary>
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Number of times to retry the initial connection after the first attempt fails.
    /// -1 retries forever. Defaults to 3.
    /// </summary>
    public int ConnectRetries { get; init; } = 3;

    /// <summary>
    /// Number of times to retry after an established connection is lost.
    /// -1 retries forever (the default), which keeps the connection up through broker restarts or network outages.
    /// </summary>
    public int ReconnectRetries { get; init; } = -1;

    /// <summary>Delay between reconnect attempts. Defaults to 3 seconds.</summary>
    public TimeSpan ReconnectRetryWait { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>Interval between keep-alive messages to the broker. Defaults to 3 seconds.</summary>
    public TimeSpan KeepAliveInterval { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Number of consecutive keep-alive responses that may be missed before the connection is treated as lost.
    /// Defaults to 3.
    /// </summary>
    public int KeepAliveIntervalsLimit { get; init; } = 3;

    /// <summary>Re-adds topic subscriptions automatically after a reconnect. Defaults to true.</summary>
    public bool ReapplySubscriptions { get; init; } = true;

    /// <summary>
    /// Validates the broker's TLS certificate when using "tcps://". Defaults to true.
    /// Only turn this off for local testing.
    /// </summary>
    public bool ValidateCertificate { get; init; } = true;

    /// <summary>Solace API log level written to stderr. Defaults to <see cref="SolLogLevel.Warning"/>.</summary>
    public SolLogLevel LogLevel { get; init; } = SolLogLevel.Warning;

    /// <summary>Throws if any setting is invalid.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Host))
            throw new ArgumentException("Host must be specified.", nameof(Host));
        if (string.IsNullOrWhiteSpace(VpnName))
            throw new ArgumentException("VpnName must be specified.", nameof(VpnName));
        if (string.IsNullOrWhiteSpace(UserName))
            throw new ArgumentException("UserName must be specified.", nameof(UserName));
        if (ConnectTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ConnectTimeout), ConnectTimeout, "ConnectTimeout must be positive.");
        if (ConnectRetries < -1)
            throw new ArgumentOutOfRangeException(nameof(ConnectRetries), ConnectRetries, "ConnectRetries must be -1 or greater.");
        if (ReconnectRetries < -1)
            throw new ArgumentOutOfRangeException(nameof(ReconnectRetries), ReconnectRetries, "ReconnectRetries must be -1 or greater.");
        if (ReconnectRetryWait < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(ReconnectRetryWait), ReconnectRetryWait, "ReconnectRetryWait cannot be negative.");
        if (KeepAliveInterval < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(KeepAliveInterval), KeepAliveInterval, "KeepAliveInterval cannot be negative.");
        if (KeepAliveIntervalsLimit < 3)
            throw new ArgumentOutOfRangeException(nameof(KeepAliveIntervalsLimit), KeepAliveIntervalsLimit, "KeepAliveIntervalsLimit must be at least 3.");
    }

    internal SessionProperties ToSessionProperties() => new()
    {
        Host = Host,
        VPNName = VpnName,
        UserName = UserName,
        Password = Password,
        ClientName = ClientName,
        ConnectTimeoutInMsecs = ToMsecs(ConnectTimeout),
        ConnectRetries = ConnectRetries,
        ReconnectRetries = ReconnectRetries,
        ReconnectRetriesWaitInMsecs = ToMsecs(ReconnectRetryWait),
        KeepAliveIntervalInMsecs = ToMsecs(KeepAliveInterval),
        KeepAliveIntervalsLimit = KeepAliveIntervalsLimit,
        ReapplySubscriptions = ReapplySubscriptions,
        SSLValidateCertificate = ValidateCertificate,
        ConnectBlocking = true,
    };

    private static int ToMsecs(TimeSpan value) => (int)Math.Min(value.TotalMilliseconds, int.MaxValue);
}
