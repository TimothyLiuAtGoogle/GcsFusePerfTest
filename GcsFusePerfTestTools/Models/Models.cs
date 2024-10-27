namespace Models;

public class ObjectOperationLogEntry {
    public string? Protocol { get; set; }
    public string? Operation { get; set; }
    public int ThreadCount { get; set; }
    public int ThreadId { get; set; }
    public DateTime Start { get; set; }
    public DateTime End { get; set; }
    public ulong Size { get; set; }
    public TimeSpan Duration { get; set; }
    public string? Bucket { get; set; }
    public string? Object { get; set; }

    public bool EnableHardDriveIo { get; set; }
    public List<AtomOperationLogEntry> AtomOperationLogEntries { get; } = new List<AtomOperationLogEntry>();
}

public class AtomOperationLogEntry {
    public int Size { get; set; }
    public TimeSpan Duration { get; set; }
    public double CpuUtil { get; set; }
    public double MemoryUtil { get; set; }
}

public enum Operation {
    Read,
    Write,
}

public enum Protocol {
    JSON,
    GRPC,
}