using System;
using System.Collections.Generic;
using System.Configuration;

namespace BulkQuery
{

    [Serializable]
    [SettingsSerializeAs(SettingsSerializeAs.Xml)]
    public class ServerDefinition
    {
        public string ConnectionString { get; set; }
        public string DisplayName { get; set; }
        public List<String> SelectedDatabases { get; set; }

        // Empty ctor for Xml Serialization
        public ServerDefinition() { }

        public ServerDefinition(string displayName, string connectionString)
        {
            DisplayName = displayName;
            ConnectionString = connectionString;
        }
    }
}
