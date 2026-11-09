using QuickFix;

using Message = QuickFix.Message;

namespace QueueFix.Sample;

public class Application : IApplication
{
    public void FromAdmin(Message message, SessionID sessionID)
    {
        Console.WriteLine($"Received admin message on {sessionID}: {message}");
    }

    public void FromApp(Message message, SessionID sessionID)
    {
        Console.WriteLine($"Received business message on {sessionID}: {message}");
    }

    public void OnCreate(SessionID sessionID)
    {
        Console.WriteLine($"Created session: {sessionID}");
    }

    public void OnLogon(SessionID sessionID)
    {
        Console.WriteLine($"{sessionID} has LoggedOn");
    }

    public void OnLogout(SessionID sessionID)
    {
        Console.WriteLine($"{sessionID} has LoggedOff");
    }

    public void ToAdmin(Message message, SessionID sessionID)
    {
        Console.WriteLine($"Sent admin message on {sessionID}: {message}");
    }

    public void ToApp(Message message, SessionID sessionID)
    {
        Console.WriteLine($"Sent business message on {sessionID}: {message}");
    }
}