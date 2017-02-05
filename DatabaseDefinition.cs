using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkQuery
{
    public class DatabaseDefinition
    {
        public ServerDefinition Server { get; }
        public string DatabaseName { get; }

        public DatabaseDefinition(string databaseName, ServerDefinition server)
        {
            DatabaseName = databaseName;
            Server = server;
        }
    }
}
