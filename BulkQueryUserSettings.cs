using System.Collections.Generic;

namespace BulkQuery
{
    public class BulkQueryUserSettings
    {
        public List<ServerDefinition> Servers { get; set; }

        public bool HideSystemDatabases { get; set; }

        public int SqlTimeout { get; set; }
    }
}
