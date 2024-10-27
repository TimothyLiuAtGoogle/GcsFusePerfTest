namespace GenerateLoad;

using Models;
using System.Text.Json;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

class Program {
    public static void Main(string[] args) {
        int bufferSize64 = 1024 * 1024 * 64, bufferSize256 = 1024 * 1024 * 256;
        string originalName = "ubuntu.iso";
        string oldName = originalName;
        for (int i = 0; i < 30; i++) {
            string mountPoint = "/home/liutimothy/bucket-http";
            string newName = Guid.NewGuid().ToString();
            File.Move($"{mountPoint}/{oldName}", $"{mountPoint}/{newName}");
            oldName = newName;
            System.Console.WriteLine($"Start {i + 1} HTTP reading {newName} ...");
            ReadSingleFile("gcs-grpc-team-liutimothy-bucket-fuse-001", mountPoint, newName, Protocol.JSON, 1,
                1,
                false, bufferSize64);

            System.Console.WriteLine("Sleep 10s ...");
            Thread.Sleep(1000 * 10);
        }

        File.Move(oldName, originalName);

        System.Console.WriteLine("Sleep 5m for switching from HTTP to gRPC ...");
        Thread.Sleep(1000 * 60 * 5);

        oldName = originalName;
        for (int i = 0; i < 30; i++) {
            string mountPoint = "/home/liutimothy/bucket-grpc";
            string newName = Guid.NewGuid().ToString();
            File.Move($"{mountPoint}/{oldName}", $"{mountPoint}/{newName}");
            oldName = newName;
            System.Console.WriteLine($"Start {i + 1} gRPC 64MB reading {newName} ...");
            ReadSingleFile("gcs-grpc-team-liutimothy-bucket-fuse-002", mountPoint, newName, Protocol.GRPC, 1,
                1,
                false,
                bufferSize64);

            System.Console.WriteLine("Sleep 10s ...");
            Thread.Sleep(1000 * 10);
        }

        File.Move(oldName, originalName);

        System.Console.WriteLine("Sleep 5m for switching from 64MB buffer to 256MB buffer ...");
        Thread.Sleep(1000 * 60 * 5);

        oldName = originalName;
        for (int i = 0; i < 30; i++) {
            string mountPoint = "/home/liutimothy/bucket-grpc";
            string newName = Guid.NewGuid().ToString();
            File.Move($"{mountPoint}/{oldName}", $"{mountPoint}/{newName}");
            oldName = newName;
            System.Console.WriteLine($"Start {i + 1} gRPC 256MB reading {newName} ...");
            ReadSingleFile("gcs-grpc-team-liutimothy-bucket-fuse-002", mountPoint, newName, Protocol.GRPC, 1,
                1,
                false,
                bufferSize256);

            System.Console.WriteLine("Sleep 10s ...");
            Thread.Sleep(1000 * 10);
        }
    }

    private static void ReadSingleFile(string bucket, string mountPoint, string obj, Protocol protocol,
        int threadCount, int threadId, bool enableHardDriveIo, int bufferSize) {
        try {
            ObjectOperationLogEntry objectOperationLogEntry = new ObjectOperationLogEntry() {
                Protocol = protocol,
                Operation = Operation.Read,
                Bucket = bucket,
                Object = obj,
                ThreadCount = threadCount,
                ThreadId = threadId,
                EnableHardDriveIo = enableHardDriveIo,
                Start = DateTime.Now
            };

            using FileStream readStream = new FileStream($"{mountPoint}/{obj}", FileMode.Open, FileAccess.Read);
            using BufferedStream bufferedStream = new BufferedStream(readStream, bufferSize);
            byte[] buffer = new byte[bufferSize];

            long[] init = GetCpuValues();
            Stopwatch stopwatch = new Stopwatch();
            ulong objectSize = 0;
            string tempLocalFile = $"./{obj}-{threadId}.temp";
            using FileStream writeStream = new FileStream(tempLocalFile, FileMode.Create, FileAccess.Write);
            while (true) {
                stopwatch.Reset();
                stopwatch.Start();
                int bytesRead = bufferedStream.Read(buffer, 0, buffer.Length);
                objectSize += (ulong)bytesRead;
                stopwatch.Stop();
                if (bytesRead == 0) break;

                if (enableHardDriveIo) {
                    writeStream.Write(buffer, 0, bytesRead);
                }

                long[] curr = GetCpuValues();
                double cpuUtil = Math.Round(CalculateCpuUtilization(init, curr) * 100, 2);
                var memInfo = GetMemoryValues();
                double memUtil = Math.Round((double)memInfo["MemAvailable:"] / memInfo["MemTotal:"], 2);
                AtomOperationLogEntry atomOperationLogEntry = new AtomOperationLogEntry {
                    Size = bytesRead,
                    Duration = stopwatch.Elapsed,
                    CpuUtil = cpuUtil,
                    MemoryUtil = memUtil
                };

                objectOperationLogEntry.AtomOperationLogEntries.Add(atomOperationLogEntry);
            }

            writeStream.Flush();
            writeStream.Close();
            File.Delete(tempLocalFile);

            objectOperationLogEntry.Size = objectSize;
            objectOperationLogEntry.End = DateTime.Now;
            objectOperationLogEntry.Duration = objectOperationLogEntry.End - objectOperationLogEntry.Start;

            string timestamp = objectOperationLogEntry.End.ToString("yyyy-MM-dd-HH-mm-ss-fff");
            string jsonFile = $"/home/liutimothy/temp/logs/{bucket}-{threadId}-{timestamp}.json";
            string json = JsonSerializer.Serialize<ObjectOperationLogEntry>(objectOperationLogEntry);
            File.WriteAllText(jsonFile, json);
            Console.WriteLine($"Operation: {jsonFile}");
        } catch (Exception ex) {
            Console.WriteLine($"Error when reading `{mountPoint}/{obj}`. {ex.Message}");
        }
    }

    private static long[] GetCpuValues() {
        string cpuStatLine = File.ReadAllLines("/proc/stat")[0];
        string[] rawValues = cpuStatLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        long[] values = rawValues.Skip(1).Select(v => long.Parse(v)).ToArray();
        return values;
    }

    private static double CalculateCpuUtilization(long[] init, long[] curr) {
        int[] idleIndices = new[] { 3, 4 }, nonIdleIndices = new[] { 0, 1, 2, 5, 6, 7 };

        long prevIdle = 0, idle = 0;
        foreach (var i in idleIndices) {
            prevIdle += init[i];
            idle += curr[i];
        }

        long prevNonIdle = 0, nonIdle = 0;
        foreach (var i in nonIdleIndices) {
            prevNonIdle += init[i];
            nonIdle += curr[i];
        }

        long prevTotal = prevIdle + prevNonIdle, total = idle + nonIdle;
        double totald = total - prevTotal, idled = idle - prevIdle;

        return (totald - idled) / totald;
    }

    private static Dictionary<string, long> GetMemoryValues() {
        var values = File.ReadAllLines("/proc/meminfo")
            .Select(line => line.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .ToDictionary(parts => parts[0], parts => long.Parse(parts[1]));
        return values;
    }
}