namespace QueueProcess.ServiceBus.Contract
{
    public class OrderQueued
    {
        public int OrderId { get; set; }
        public bool sent { get; set; }
    }
}
