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

        foreach (var oLog in logEntries) {
            string oLogLine =
                $"{oLog.Bucket},{oLog.Object},{oLog.Operation},{oLog.Protocol},{oLog.Size},{oLog.Start},{oLog.Duration}";
            objectOperationLogs.Add(oLogLine);
            int counter = 0;
            foreach (var aLog in oLog.AtomOperationLogEntries) {
                string aLogLine =
                    $"{oLog.Object},{++counter},{aLog.Size},{aLog.Duration},{aLog.CpuUtil},{aLog.MemoryUtil}";
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