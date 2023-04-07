using System;

public class LogTimer {
    public Guid Id { get; set; }
    public string Queue { get; set; }
    public DateTime SentTimestamp {get; set; }
    public DateTime ReceiveTimestamp { get;set; }
    public long ProcesMs { get; set; }
}