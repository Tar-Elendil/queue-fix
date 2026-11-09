using Microsoft.Extensions.Logging;
using QuickFix;
using QuickFix.Store;
using QuickFix.Transport;
using System.Text;

namespace QueueFix.Sample;

class Program
{
    static readonly string session_settings =
"""
[DEFAULT]
ConnectionType=initiator
ReconnectInterval=60
[SESSION]
BeginString=FIX.4.4
SenderCompID=APP
TargetCompID=QFIX
SocketConnectPort=5001
SocketConnectHost=localhost
""";

    [STAThread]
    static void Main(string[] args)
    {
        Console.WriteLine("Connecting to Client QFIX on localhost:5001");
        Console.WriteLine("===========================================");

        try
        {
            SessionSettings settings = new SessionSettings(new StringReader(session_settings));
            IApplication app = new Application();
            IMessageStoreFactory storeFactory = new MemoryStoreFactory();

            using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
            });
            var initiator = new SocketInitiator(app, storeFactory, settings, loggerFactory);

            initiator.Start();
            Console.WriteLine("press <enter> to quit");
            Console.Read();
            initiator.Stop();
        }
        catch (Exception e)
        {
            Console.WriteLine("==FATAL ERROR==");
            Console.WriteLine(e.ToString());
        }
    }
}
