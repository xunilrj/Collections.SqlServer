using System;
using System.Collections.Generic;
using System.Linq;

namespace MachinaAurum.Collections.SqlServer
{
    public class SqlSetParameters
    {
        public string ConnectionString { get; set; }
        public string TableName { get; set; }
        public string[] ColumnsName { get; set; }
        public string TypeFormat { get; set; }
        public string StoredProcedureFormat { get; set; }

        public SqlSetParameters(string connectionString, string tableName, string[] columnsName)
        {
            ConnectionString = connectionString;
            TableName = tableName;
            ColumnsName = columnsName;
            TypeFormat = "{0}Type";
            StoredProcedureFormat = "{0}AddIfNotExist";
        }
    }

    public class SqlSet
    {
        ISQLServer Server;
        SqlSetParameters Parameters;

        public SqlSet(SqlSetParameters parameters) : this(new SQLServer(parameters.ConnectionString), parameters)
        {
        }

        public SqlSet(ISQLServer server, SqlSetParameters parameters)
        {
            Server = server;
            Parameters = parameters;
        }

        public void CreateObjects()
        {
            var typeName = string.Format(Parameters.TypeFormat, Parameters.TableName);
            var spName = string.Format(Parameters.StoredProcedureFormat, Parameters.TableName);
            var columnsNames = string.Join(",", Parameters.ColumnsName);

            var sql = $@"IF TYPE_ID('{typeName}') IS NULL
            BEGIN
                DECLARE @Names VARCHAR(8000)  
                SELECT @Names = COALESCE(@Names + ',', '') + COLUMN_NAME + ' ' + DATA_TYPE +
	                CASE WHEN DATA_TYPE IN ('char', 'varchar','nchar','nvarchar') THEN '('+
                             CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX'
                                  ELSE CONVERT(VARCHAR(4),
                                               CASE WHEN DATA_TYPE IN ('nchar','nvarchar')
                                               THEN  CHARACTER_MAXIMUM_LENGTH/2 ELSE CHARACTER_MAXIMUM_LENGTH END )
                                  END +')'
                          WHEN DATA_TYPE IN ('decimal','numeric')
                                  THEN '('+ CONVERT(VARCHAR(4),NUMERIC_PRECISION)+','
                                          + CONVERT(VARCHAR(4),NUMERIC_SCALE)+')'
                                  ELSE '' END
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = N'{Parameters.TableName}' AND COLUMN_NAME IN ({string.Join(",", Parameters.ColumnsName.Select(x => $"'{x}'"))})
                SET @Names = 'CREATE TYPE [{typeName}] AS TABLE(' + @Names + ')'
                EXEC(@Names)
            END
            IF OBJECT_ID('{spName}') IS NULL
            BEGIN
                EXEC('CREATE PROCEDURE [{spName}](@datasource {typeName} READONLY)
                AS
                    MERGE [{Parameters.TableName}] as [Target]
                    USING(SELECT {columnsNames} from @datasource) as [Source]
                    ({columnsNames}) on
                        {string.Join(" AND ", Parameters.ColumnsName.Select(x => $"[Target].{x} = [Source].{x}"))}
                    WHEN NOT MATCHED THEN
                        INSERT({columnsNames})
                        VALUES({string.Join(",", Parameters.ColumnsName.Select(x => $"[Source].{x}"))});')
            END";
            Server.Execute(sql);
        }

        public void AddIfNotExists(IEnumerable<object> item)
        {
            var typeName = string.Format(Parameters.TypeFormat, Parameters.TableName);
            var spName = string.Format(Parameters.StoredProcedureFormat, Parameters.TableName);
            var columnsNames = string.Join(",", Parameters.ColumnsName);
            var values = string.Join(",", item.Select(x => GetValues(Parameters.ColumnsName, x)));

            Server.Execute($@"DECLARE @datasource AS {typeName}
            INSERT @datasource ({columnsNames})
            VALUES {values}
            EXEC {spName} @datasource");
        }

        private string GetValues(string[] columnsName, object item)
        {
            var values = columnsName.Select(x => item.GetType().GetProperty(x).GetValue(item));
            return $"({string.Join(",", GetAsParameters(values))})";
        }

        private IEnumerable<string> GetAsParameters(IEnumerable<object> values)
        {
            foreach (var item in values)
            {
                if(item.GetType() == typeof(string))
                {
                    yield return $"'{item}'";
                }
                else
                {
                    yield return item.ToString();
                }
            }
        }

        public void Add()
        {

        }
    }
}
