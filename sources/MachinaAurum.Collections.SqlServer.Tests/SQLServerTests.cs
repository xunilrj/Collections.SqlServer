using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace MachinaAurum.Collections.SqlServer.Tests
{
    public class SQLServerTests
    {
        [Fact]
        public void SQLServerAddMustInsertRowInTableAndCallOnSuccess()
        {
            using (var connection = GetConnection())
            {
                bool onSuccess = false;
                var sqlserver = new SQLServer(() => connection);
                sqlserver.Add("keyvalue", "key", "value", "somekey", "somevalue", () => { onSuccess = true; }, () => { });

                Assert.True(onSuccess);
            }
        }

        [Fact]
        public void SQLServerAddMustCallOnErrorWhenFailToInsertRow()
        {
            using (var connection = GetConnection())
            {
                bool onSuccess = false;
                var sqlserver = new SQLServer(() => connection);
                sqlserver.Add("keyvalue", "key1", "value", "somekey", "somevalue", () => { }, () => { onSuccess = true; });

                Assert.True(onSuccess);
            }
        }

        [Fact]
        public void SQLServerRemoveMustDeleteRowInTableAndCallOnSuccess()
        {
            using (var connection = GetConnection())
            {
                bool onSuccess = false;
                var sqlserver = new SQLServer(() => connection);
                sqlserver.Remove("keyvalue", "key", "somekey", () => { onSuccess = true; });

                Assert.True(onSuccess);
            }
        }

        [Fact]
        public void SQLServerClearMustTruncateTableAndCallSucess()
        {
            using (var connection = GetConnection())
            {
                bool onSuccess = false;
                var sqlserver = new SQLServer(() => connection);
                sqlserver.Clear("keyvalue", () => { onSuccess = true; });

                Assert.True(onSuccess);
            }
        }

        public IDbConnection GetConnection()
        {
            var dbconnection = new SQLiteConnection("Data Source=:memory:");
            dbconnection.Open();
            using (var command = dbconnection.CreateCommand())
            {
                command.CommandText = "CREATE TABLE [keyvalue]([key] nvarchar(50) not null, [value] ntext, CONSTRAINT PK_keyvalue_key PRIMARY KEY (key))";
                command.ExecuteNonQuery();
            }

            return dbconnection;
        }
    }
}
