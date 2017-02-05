using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace BulkQuery
{
    public static class QueryRunner
    {
        public static List<DatabaseDefinition> GetDatabasesForServer(ServerDefinition server)
        {
            var databases = new List<DatabaseDefinition>();
            const string query = "SELECT name FROM master.dbo.sysdatabases";
            var builder = new SqlConnectionStringBuilder(server.ConnectionString);
            builder.ConnectTimeout = 5;
            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);
                connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                try
                {
                    while (reader.Read())
                    {
                        var dbName = reader["name"] as string;
                        databases.Add(new DatabaseDefinition(dbName, server));
                    }
                }
                finally
                {
                    // Always call Close when done reading.
                    reader.Close();
                }
            }
            return databases;
        }

        private static bool AreColumnsIdentical(DataColumnCollection columnsA, DataColumnCollection columnsB)
        {
            if (columnsA.Count != columnsB.Count)
                return false;

            for (int i = 0; i < columnsA.Count; i++)
            {
                var colA = columnsA[i];
                var colB = columnsB[i];

                if (colA.DataType != colB.DataType || colA.ColumnName != colB.ColumnName || colA.Ordinal != colB.Ordinal)
                    return false;
            }

            return true;
        }

        public class QueryResult
        {
            public List<string> Messages { get; set; }
            public DataTable ResultTable { get; set; }
        }

        public static QueryResult BulkQuery(IList<DatabaseDefinition> databases, string query)
        {
            DataTable resultTable = null;
            var messages = new List<string>();
            string schemaDatabaseFriendlyName = null;

            foreach (var db in databases)
            {
                var isDbNameUnique = databases.Count(d => d.DatabaseName == db.DatabaseName) == 1;
                var friendlyDbName = db.DatabaseName;
                if (!isDbNameUnique)
                {
                    friendlyDbName += " - " + db.Server.DisplayName;
                }

                var builder = new SqlConnectionStringBuilder(db.Server.ConnectionString);
                builder.InitialCatalog = db.DatabaseName;
                using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
                {
                    try
                    {
                        connection.Open();
                        var command = connection.CreateCommand();
                        command.CommandText = query;
                        using (var dataReader = command.ExecuteReader())
                        {
                            var table = ReadTable(dataReader);

                            // Add a column that shows which DB it came from.
                            var sourceCol = new DataColumn("Source Database", typeof(string));
                            table.Columns.Add(sourceCol);
                            foreach (var col in table.Columns.Cast<DataColumn>().ToList())
                            {
                                if (col != sourceCol)
                                    col.SetOrdinal(col.Ordinal + 1);
                            }
                            sourceCol.SetOrdinal(0);

                            foreach (var row in table.Rows.Cast<DataRow>())
                            {
                                row[sourceCol] = friendlyDbName;
                            }

                            if (resultTable == null)
                            {
                                // First set of results.. we'll use this as the 'schema' and all subsequent results must match it.
                                resultTable = table;
                                schemaDatabaseFriendlyName = friendlyDbName;
                            }
                            else
                            {
                                // Check that the 'schema' of this db's result matches the initial schema.
                                if (!AreColumnsIdentical(resultTable.Columns, table.Columns))
                                {
                                    messages.Add($"Columns returned by {friendlyDbName} does not match those of {schemaDatabaseFriendlyName}.");
                                }
                                else
                                {
                                    // Copy the rows over to the existing datatable.
                                    foreach (var row in table.Rows.Cast<DataRow>())
                                    {
                                        resultTable.Rows.Add(row.ItemArray);
                                    }
                                }
                            }
                        }
                    }
                    catch (SqlException ex)
                    {
                        messages.Add($"Error on {friendlyDbName}: {ex.Message}");
                    }
                }
            }

            return new QueryResult
            {
                Messages = messages,
                ResultTable = resultTable
            };
        }

        // The built in dataTable.Load(dataReader) method is very slow for large data sets.
        // This one is taken from http://stackoverflow.com/questions/18961938/populate-data-table-from-data-reader
        // and seems to be significantly faster.
        private static DataTable ReadTable(SqlDataReader dataReader)
        {
            var dtSchema = dataReader.GetSchemaTable();
            var dt = new DataTable();
            var listCols = new List<DataColumn>();

            if (dtSchema != null)
            {
                foreach (DataRow drow in dtSchema.Rows)
                {
                    string columnName = Convert.ToString(drow["ColumnName"]);
                    DataColumn column = new DataColumn(columnName, (Type)(drow["DataType"]));
                    column.AllowDBNull = (bool)drow["AllowDBNull"];
                    listCols.Add(column);
                    dt.Columns.Add(column);
                }
            }

            // Read rows from DataReader and populate the DataTable
            while (dataReader.Read())
            {
                DataRow dataRow = dt.NewRow();
                for (var i = 0; i < listCols.Count; i++)
                {
                    dataRow[listCols[i]] = dataReader[i];
                }
                dt.Rows.Add(dataRow);
            }
            return dt;
        }
    }
}
