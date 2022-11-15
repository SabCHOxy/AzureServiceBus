namespace QueueProcess.ServiceBus.Contract.Enums
{
    public enum QueuingState
    {
        None = 0,
        Sent = 10,
        Pending = 20,
        Canceled = 30
    }
}
