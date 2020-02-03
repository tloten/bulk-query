using Newtonsoft.Json;
using System;
using System.IO;

namespace BulkQuery
{
    public class UserSettingsManager<T> where T : class
    {
        public UserSettingsManager(string fileName)
        {
            FilePath = GetLocalFilePath(fileName);
        }

        public string FilePath { get; private set; }

        private static string GetLocalFilePath(string fileName)
        {
            string appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            return Path.Combine(appData, fileName);
        }

        public T LoadSettings() =>
            File.Exists(FilePath) ?
                JsonConvert.DeserializeObject<T>(File.ReadAllText(FilePath)) :
                null;

        public void SaveSettings(T settings)
        {
            string json = JsonConvert.SerializeObject(settings);
            File.WriteAllText(FilePath, json);
        }
    }
}
