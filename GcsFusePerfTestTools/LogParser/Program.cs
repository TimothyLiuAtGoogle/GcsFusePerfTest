using System.Text.Json;

namespace LogParser;

using Models;

class Program {
    static void Main(string[] args) {
        string workingDirectory = "/Users/liutimothy/Desktop/gcsfuse_test";
        string logDirector = $"{workingDirectory}/write_logs";
        string[] jsonFiles = Directory.GetFiles(logDirector, "*.json");
        List<ObjectOperationLogEntry> logEntries = jsonFiles
            .OrderBy(jsonFile => Path.GetFileNameWithoutExtension(jsonFile))
            .Select(le => JsonSerializer.Deserialize<ObjectOperationLogEntry>(File.ReadAllText(le)))
            .ToList();

        List<string> objectOperationLogs = new List<string>();
        List<string> atomOperationLogs = new List<string>();

        objectOperationLogs.Add(
            "Bucket\tObject\tOperation\tProtocol\tSizeInByte\tStart\tDurationInMillisecond\tSpeedInBitPerSec\tCpuUtil\tMemoryUtil");
        atomOperationLogs.Add("Object\tSerial\tSizeInByte\tDurationInMillisecond\tSpeedInBitPerSec\tCpuUtil\tMemoryUtil");
        foreach (var oLog in logEntries) {
            double avgCpuUtil = oLog.AtomOperationLogEntries.Average(entry => entry.CpuUtil);
            double avgMemoryUtil = oLog.AtomOperationLogEntries.Average(entry => entry.MemoryUtil);
            string oLogLine =
                $"{oLog.Bucket}\t{oLog.Object}\t{oLog.Operation}\t{oLog.Protocol}\t{oLog.Size}\t{oLog.Start}\t{oLog.Duration.Microseconds}\t{oLog.Size * 8 / (ulong)oLog.Duration.Seconds}\t{avgCpuUtil}\t{avgMemoryUtil}";
            objectOperationLogs.Add(oLogLine);
            int counter = 0;
            foreach (var aLog in oLog.AtomOperationLogEntries) {
                string aLogLine =
                    $"{oLog.Object}\t{++counter}\t{aLog.Size}\t{aLog.Duration.Microseconds}\t{aLog.Size * 8 / aLog.Duration.Seconds}\t{aLog.CpuUtil}\t{aLog.MemoryUtil}";
                atomOperationLogs.Add(aLogLine);
            }
        }

        string objectOperationOutput = $"{workingDirectory}/object_write.txt";
        string atomOperationOutput = $"{workingDirectory}/atom_write.txt";

        if (File.Exists(objectOperationOutput)) File.Delete(objectOperationOutput);
        if (File.Exists(atomOperationOutput)) File.Delete(atomOperationOutput);

        File.WriteAllLines(objectOperationOutput, objectOperationLogs);
        File.WriteAllLines(atomOperationOutput, atomOperationLogs);
        Console.WriteLine("Done!");
    }
}