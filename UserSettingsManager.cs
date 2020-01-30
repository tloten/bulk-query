using Newtonsoft.Json;
using System;
using System.IO;

namespace BulkQuery
{
    public class UserSettingsManager<T> where T : class
    {
        private readonly string _filePath;

        public UserSettingsManager(string fileName)
        {
            _filePath = GetLocalFilePath(fileName);
        }

        private static string GetLocalFilePath(string fileName)
        {
            string appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            return Path.Combine(appData, fileName);
        }

        public T LoadSettings() =>
            File.Exists(_filePath) ?
                JsonConvert.DeserializeObject<T>(File.ReadAllText(_filePath)) :
                null;

        public void SaveSettings(T settings)
        {
            string json = JsonConvert.SerializeObject(settings);
            File.WriteAllText(_filePath, json);
        }
    }
}
