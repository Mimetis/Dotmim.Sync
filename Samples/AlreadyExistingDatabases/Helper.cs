using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Threading.Tasks;

namespace AlreadyExstingDatabases
{
    public static class Helper
    {

        public static async Task CreateSqlServerServiceTicketsTableAsync(DbConnection c)
        {
            string commandText = @"if (not exists (select * from sys.tables where name = 'ServiceTickets'))
                begin
                    CREATE TABLE [ServiceTickets](
                        [ServiceTicketID] [uniqueidentifier] NOT NULL,
                        [Title] [nvarchar](max) NOT NULL,
                        [StatusValue] [int] NOT NULL,
                        [Opened] [datetime] NULL,
                    CONSTRAINT [PK_ServiceTickets] PRIMARY KEY CLUSTERED ( [ServiceTicketID] ASC ));
                end";

            using var cmd = c.CreateCommand();

            cmd.CommandText = commandText;
            cmd.Connection = c;
            cmd.CommandType = CommandType.Text;

            await c.OpenAsync().ConfigureAwait(false);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            await c.CloseAsync().ConfigureAwait(false);
        }

        public static async Task CreateSqliteServiceTicketsTableAsync(DbConnection c)
        {
            string commandText = @"CREATE TABLE IF NOT EXISTS [ServiceTickets](
                  [ServiceTicketID] text NOT NULL,
                  [Title] text NOT NULL,
                  [StatusValue] integer NOT NULL,
                  [Opened] datetime NULL,
                PRIMARY KEY ( [ServiceTicketID] ASC ))";

            using var cmd = c.CreateCommand();

            cmd.CommandText = commandText;
            cmd.Connection = c;
            cmd.CommandType = CommandType.Text;

            await c.OpenAsync().ConfigureAwait(false);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            await c.CloseAsync().ConfigureAwait(false);
        }

        public static async Task AddRowsAsync(DbConnection connection)
        {

            var command = connection.CreateCommand();
            command.CommandText = $@"INSERT INTO [ServiceTickets] ([ServiceTicketID], [Title], [StatusValue], [Opened]) 
                                VALUES (@ServiceTicketID, @Title, @StatusValue, @Opened)";

            var parameter = command.CreateParameter();
            parameter.DbType = DbType.Guid;
            parameter.ParameterName = "@ServiceTicketID";
            parameter.Value = Guid.NewGuid();
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.DbType = DbType.String;
            parameter.ParameterName = "@Title";
            parameter.Value = $"Title - {Guid.NewGuid().ToString()}";
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.DbType = DbType.Int32;
            parameter.ParameterName = "@StatusValue";
            parameter.Value = new Random().Next(0, 10);
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.DbType = DbType.DateTime;
            parameter.ParameterName = "@Opened";
            parameter.Value = DateTime.Now;
            command.Parameters.Add(parameter);

            try
            {
                await connection.OpenAsync().ConfigureAwait(false);
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                await connection.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
            finally
            {
                if (connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        public static async Task DropRowsAsync(DbConnection connection)
        {

            var command = connection.CreateCommand();
            command.CommandText = $@"DELETE FROM [ServiceTickets]";

            try
            {
                await connection.OpenAsync().ConfigureAwait(false);
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                await connection.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
            finally
            {
                if (connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }
    }
}