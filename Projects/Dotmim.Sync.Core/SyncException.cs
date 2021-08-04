using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.Serialization;
using System.Security.Permissions;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Exception
    /// </summary>
    public class SyncException : Exception
    {
        public SyncException(string message) : base(message)
        {

        }

        public SyncException(Exception exception, SyncStage stage = SyncStage.None) : base(exception.Message, exception)
        {
            this.SyncStage = stage;

            if (exception is null)
                return;

            this.TypeName = exception.GetType().Name;
        }

        /// <summary>
        /// Gets or Sets type name of exception
        /// </summary>
        public string TypeName { get; set; }

        /// <summary>
        /// Sync stage when exception occured
        /// </summary>
        public SyncStage SyncStage { get; set; }

        /// <summary>
        /// Data source error number if available
        /// </summary>
        public int Number { get; set; }

        /// <summary>
        /// Gets or Sets data source if available
        /// </summary>
        public string DataSource { get; set; }

        /// <summary>
        /// Gets or Sets initial catalog if available
        /// </summary>
        public string InitialCatalog { get; set; }

        /// <summary>
        /// Gets or Sets if error is Local or Remote side
        /// </summary>
        public SyncSide Side { get; set; }

    }

    /// <summary>
    /// Unknown Exception
    /// </summary>
    public class UnknownException : Exception
    {
        public UnknownException(string message) : base(message) { }
    }

    /// <summary>
    /// Rollback Exception
    /// </summary>
    public class RollbackException : Exception
    {
        public RollbackException(string message) : base(message) { }
    }

    /// <summary>
    /// Occurs when trying to launch another sync during an in progress sync.
    /// </summary>
    public class AlreadyInProgressException : Exception
    {
        const string message = "Synchronization already in progress";

        public AlreadyInProgressException() : base(message) { }
    }



    /// <summary>
    /// Occurs when trying to use a closed connection
    /// </summary>
    public class ConnectionClosedException : Exception
    {
        const string message = "The connection to database {0} is closed.";

        public ConnectionClosedException(DbConnection connection) : base(string.Format(message, connection.Database)) { }
    }

    /// <summary>
    /// Occurs when trying to launch another sync during an in progress sync.
    /// </summary>
    public class FormatTypeException : Exception
    {
        const string message = "The type {0} is not supported ";

        public FormatTypeException(Type type) : base(string.Format(message, type.Name)) { }
    }


    public class FormatDbTypeException : Exception
    {
        const string message = "The DbType {0} is not supported ";

        public FormatDbTypeException(DbType type) : base(string.Format(message, type.ToString())) { }
    }


    /// <summary>
    /// Occurs when a bad SyncProvision is provided to a local orchestrator
    /// </summary>
    public class InvalidRemoteOrchestratorException : Exception
    {
        const string message = "The remote orchestrator used here is not able to intercept the OnApplyChangedFailed event, since this event is occuring on the server side only";

        public InvalidRemoteOrchestratorException() : base(message) { }
    }


    /// <summary>
    /// Occurs when a bad SyncProvision is provided to a local orchestrator
    /// </summary>
    public class InvalidProvisionForLocalOrchestratorException : Exception
    {
        const string message = "A local database should not have a server scope table. Please provide a correct SyncProvision flag.";

        public InvalidProvisionForLocalOrchestratorException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a bad SyncProvision is provided to a remote orchestrator
    /// </summary>
    public class InvalidProvisionForRemoteOrchestratorException : Exception
    {
        const string message = "A server database should not have a client scope table. Please provide a correct SyncProvision flag.";

        public InvalidProvisionForRemoteOrchestratorException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a connection is missing
    /// </summary>
    public class MissingConnectionException : Exception
    {
        const string message = "Connection is null";

        public MissingConnectionException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a file is missing
    /// </summary>
    public class MissingFileException : Exception
    {
        const string message = "File {0} doesn't exist.";

        public MissingFileException(string fileName) : base(string.Format(message, fileName)) { }
    }

    /// <summary>
    /// Occurs when a schema is needed, but does not exists
    /// </summary>
    public class MissingLocalOrchestratorSchemaException : Exception
    {
        const string message = "Schema does not exists yet in your local database. You must make a first sync with your server, to initialize everything required locally.";

        public MissingLocalOrchestratorSchemaException() : base(message) { }
    }


    /// <summary>
    /// Occurs when a schema is needed, but does not exists
    /// </summary>
    public class MissingRemoteOrchestratorSchemaException : Exception
    {
        const string message = "Schema does not exists yet in your remote database. You must make a first sync with your server, to initialize everything required locally.";

        public MissingRemoteOrchestratorSchemaException() : base(message) { }
    }


    /// <summary>
    /// Occurs when a scope info is needed, but does not exists
    /// </summary>
    public class MissingClientScopeInfoException : Exception
    {
        const string message = "The client scope info is invalid. You need to make a first sync before.";

        public MissingClientScopeInfoException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a scope info is needed, but does not exists
    /// </summary>
    public class MissingServerScopeInfoException : Exception
    {
        const string message = "The server scope info is invalid. You need to make a first sync before.";

        public MissingServerScopeInfoException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a scope info is not good, conflicting with the one from the orchestrator
    /// </summary>
    public class InvalidScopeInfoException : Exception
    {
        const string message = "The scope name is invalid. Be sure to declare a scope name correctly.";

        public InvalidScopeInfoException() : base(message) { }
    }



    /// <summary>
    /// Occurs when primary key is missing in the table schema
    /// </summary>
    public class MissingPrimaryKeyException : Exception
    {
        const string message = "Table {0} does not have any primary key.";

        public MissingPrimaryKeyException(string tableName) : base(string.Format(message, tableName)) { }
    }

    /// <summary>
    /// Setup table exception. Used when a setup table is defined that does not exist in the data source
    /// </summary>
    public class MissingTableException : Exception
    {
        const string message = "Table {0} does not exists.";

        public MissingTableException(string tableName) : base(string.Format(message, tableName)) { }
    }

    /// <summary>
    /// Setup column exception. Used when a setup column  is defined that does not exist in the data source table
    /// </summary>
    public class MissingColumnException : Exception
    {
        const string message = "Column {0} does not exists in the table {1}.";

        public MissingColumnException(string columnName, string sourceTableName) : base(string.Format(message, columnName, sourceTableName)) { }
    }

    /// <summary>
    /// Setup columns exception. Used when a setup table has no columns during provisioning.
    /// </summary>
    public class MissingsColumnException : Exception
    {
        const string message = "Table {0} has no columns.";

        public MissingsColumnException(string sourceTableName) : base(string.Format(message, sourceTableName)) { }
    }


    /// <summary>
    /// Setup column exception. Used when a setup column  is defined that does not exist in the data source table
    /// </summary>
    public class MissingPrimaryKeyColumnException : Exception
    {
        const string message = "Primary key column {0} should be part of the columns list in your Setup table {1}.";

        public MissingPrimaryKeyColumnException(string columnName, string sourceTableName) : base(string.Format(message, columnName, sourceTableName)) { }
    }


    /// <summary>
    /// Setup table exception. Used when a your setup does not contains any table
    /// </summary>
    public class MissingTablesException : Exception
    {
        const string message = "Your setup does not contains any table.";

        public MissingTablesException() : base(message) { }
    }


    /// <summary>
    /// Setup table exception. Used when a your setup does not contains any columns in table
    /// </summary>
    public class MissingColumnsException : Exception
    {
        const string message = "Your setup does not contains any column.";

        public MissingColumnsException() : base(message) { }
    }


    /// <summary>
    /// During a migration, droping a table is not allowed
    /// </summary>
    public class MigrationTableDropNotAllowedException : Exception
    {
        const string message = "During a migration, droping a table is not allowed";

        public MigrationTableDropNotAllowedException() : base(message) { }
    }

    /// <summary>
    /// Metadata exception.
    /// </summary>
    public class MetadataException : Exception
    {
        const string message = "No metadatas rows found for table {0}.";

        public MetadataException(string tableName) : base(string.Format(message, tableName)) { }
    }


    /// <summary>
    /// Occurs when a row is too big for download batch size
    /// </summary>
    public class RowOverSizedException : Exception
    {
        const string message = "Row is too big ({0} kb.) for the current DownloadBatchSizeInKB.";

        public RowOverSizedException(string finalFieldSize) : base(string.Format(message, finalFieldSize)) { }
    }

    /// <summary>
    /// Occurs when a command is missing
    /// </summary>
    public class MissingCommandException : Exception
    {
        const string message = "Missing command {0}.";

        public MissingCommandException(string commandType) : base(string.Format(message, commandType)) { }
    }

    /// <summary>
    /// Occurs when we use change tracking and it's not enabled on the source database
    /// </summary>
    public class MissingChangeTrackingException : Exception
    {
        const string message = "Change Tracking is not activated for database {0}. Please execute this statement : Alter database {0} SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)";

        public MissingChangeTrackingException(string databaseName) : base(string.Format(message, databaseName)) { }
    }

    /// <summary>
    /// Occurs when we local orchestrator tries to update untracked rows, but no tracking table exists
    /// </summary>
    public class MissingTrackingTableException : Exception
    {
        const string message = "No tracking table for table {0}. Please Provision your database before calling this method";

        public MissingTrackingTableException(string tableName) : base(string.Format(message, tableName)) { }
    }


    /// <summary>
    /// Occurs when we check database existence
    /// </summary>
    public class MissingDatabaseException : Exception
    {

        const string message = "Database {0} does not exist";

        public MissingDatabaseException(string databaseName) : base(string.Format(message, databaseName)) { }
    }


    /// <summary>
    /// Occurs when a column is not supported by the Dotmim.Sync framework
    /// </summary>
    public class UnsupportedColumnTypeException : Exception
    {
        const string message = "The Column {0} of type {1} from provider {2} is not currently supported.";

        public UnsupportedColumnTypeException(string columnName, string columnType, string provider) : base(string.Format(message, columnName, columnType, provider)) { }
    }
    /// <summary>
    /// Occurs when a column name is not supported by the Dotmim.Sync framework
    /// </summary>
    public class UnsupportedColumnNameException : Exception
    {
        const string message = "The Column name {0} is not allowed. Please consider to change the column name.";

        public UnsupportedColumnNameException(string columnName, string columnType, string provider) : base(string.Format(message, columnName, columnType, provider)) { }
    }

    /// <summary>
    /// Occurs when a column name is not supported by the Dotmim.Sync framework for a primary key
    /// </summary>
    public class UnsupportedPrimaryKeyColumnNameException : Exception
    {
        const string message = "The Column name {0} is not allowed as a primary key. Please consider to change the column name or choose another primary key for your table.";

        public UnsupportedPrimaryKeyColumnNameException(string columnName, string columnType, string provider) : base(string.Format(message, columnName, columnType, provider)) { }
    }


    /// <summary>
    /// Occurs when a provider not supported as a server provider is used with a RemoteOrchestrator.
    /// </summary>
    public class UnsupportedServerProviderException : Exception
    {
        const string message = "The provider {0} can not be used as a server provider";

        public UnsupportedServerProviderException(string provider) : base(string.Format(message, provider)) { }
    }



    /// <summary>
    /// Occurs when sync metadatas are out of date
    /// </summary>
    public class OutOfDateException : Exception
    {
        const string message = "Client database is out of date. Last client sync timestamp:{0}. Last server cleanup metadata:{1} Try to make a Reinitialize sync.";

        public OutOfDateException(long? timestampLimit, long serverLastCleanTimestamp) : base(string.Format(message, timestampLimit, serverLastCleanTimestamp)) { }
    }

    /// <summary>
    /// Http empty response exception.
    /// </summary>
    public class HttpEmptyResponseContentException : Exception
    {
        const string message = "The reponse has an empty body.";

        public HttpEmptyResponseContentException() : base(message) { }
    }

    /// <summary>
    /// Http response exception.
    /// </summary>
    public class HttpResponseContentException : Exception
    {
        public HttpResponseContentException(string content) : base(content) { }
    }

    /// <summary>
    /// Occurs when a header is missing in the http request
    /// </summary>
    public class HttpHeaderMissingExceptiopn : Exception
    {
        const string message = "Header {0} is missing.";

        public HttpHeaderMissingExceptiopn(string header) : base(string.Format(message, header)) { }
    }

    /// <summary>
    /// Occurs when a cache is not set on the server
    /// </summary>
    public class HttpCacheNotConfiguredException : Exception
    {
        const string message = "Cache is not configured! Please add memory cache (distributed or not). See https://docs.microsoft.com/en-us/aspnet/core/performance/caching/response?view=aspnetcore-2.2).";

        public HttpCacheNotConfiguredException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a Serializer is not available on the server side
    /// </summary>
    public class HttpSerializerNotConfiguredException : Exception
    {
        const string message = "Unexpected value for serializer. Available serializers on the server: {0}";
        const string messageEmpty = "Unexpected value for serializer. Server has not any serializer registered";

        public HttpSerializerNotConfiguredException(IEnumerable<string> serializers) :
            base(
                serializers.Count() > 0 ?
                    string.Format(message, string.Join(".", serializers))
                    : messageEmpty
                )
        { }
    }
    /// <summary>
    /// Occurs when a Serializer is not available on the server side
    /// </summary>
    public class HttpConverterNotConfiguredException : Exception
    {
        const string message = "Unexpected value for converter. Available converters on the server: {0}";
        const string messageEmpty = "Unexpected value for converter. Server has not any converter registered";


        public HttpConverterNotConfiguredException(IEnumerable<string> converters) :
            base(
                converters.Count() > 0 ?
                    string.Format(message, string.Join(".", converters))
                    : messageEmpty
                )
        { }
    }

    /// <summary>
    /// Occurs when a parameter has been already added in a filter parameter list
    /// </summary>
    public class HttpScopeNameInvalidException : Exception
    {
        const string message = "The scope {0} does not exist on the server side. Please provider a correct scope name";

        public HttpScopeNameInvalidException(string scopeName) : base(string.Format(message, scopeName)) { }
    }

    /// <summary>
    /// Occurs when a session is lost during a sync session
    /// </summary>
    public class HttpSessionLostException : Exception
    {
        const string message = "Session loss: No batchPartInfo could found for the current sessionId. It seems the session was lost. Please try again.";

        public HttpSessionLostException() : base(message) { }
    }



    /// <summary>
    /// Occurs when a parameter has been already added in a filter parameter list
    /// </summary>
    public class FilterParameterAlreadyExistsException : Exception
    {
        const string message = "The parameter {0} has been already added for the {1} changes stored procedure";

        public FilterParameterAlreadyExistsException(string parameterName, string tableName) : base(string.Format(message, parameterName, tableName)) { }
    }

    /// <summary>
    /// Occurs when a filter already exists for a named table
    /// </summary>
    public class FilterAlreadyExistsException : Exception
    {
        const string message = "The filter for the {0} changes stored procedure already exists";

        public FilterAlreadyExistsException(string tableName) : base(string.Format(message, tableName)) { }
    }


    /// <summary>
    /// Occurs when a filter column used as a filter for a tracking table, has not been added to the column parameters list
    /// </summary>
    public class FilterTrackingWhereException : Exception
    {
        const string message = "The column {0} does not exist in the columns parameters list, so can't be add as a where filter clause to the tracking table";

        public FilterTrackingWhereException(string columName) : base(string.Format(message, columName)) { }
    }


    /// <summary>
    /// Occurs when a filter column used as a filter for a tracking table, but not exists
    /// </summary>
    public class FilterParamColumnNotExistsException : Exception
    {
        const string message = "The parameter {0} does not exist as a column in the table {1}";

        public FilterParamColumnNotExistsException(string columName, string tableName) : base(string.Format(message, columName, tableName)) { }
    }

    /// <summary>
    /// Occurs when a filter column used as a filter for a tracking table, but not exists
    /// </summary>
    public class FilterParamTableNotExistsException : Exception
    {
        const string message = "The table {0} does not exist";

        public FilterParamTableNotExistsException(string tableName) : base(string.Format(message, tableName)) { }
    }

    /// <summary>
    /// Occurs when a parameter has been already added to the parameter collection
    /// </summary>
    public class SyncParameterAlreadyExistsException : Exception
    {
        const string message = "The parameter {0} already exists in the parameter list.";

        public SyncParameterAlreadyExistsException(string parameterName) : base(string.Format(message, parameterName)) { }
    }


    /// <summary>
    /// Occurs when trying to apply a snapshot that does not exists
    /// </summary>
    public class SnapshotNotExistsException : Exception
    {
        const string message = "The snapshot {0} does not exists.";

        public SnapshotNotExistsException(string directoryName) : base(string.Format(message, directoryName)) { }
    }

    /// <summary>
    /// Occurs when trying to create a snapshot but no directory and size have been set in the options
    /// </summary>
    public class SnapshotMissingMandatariesOptionsException : Exception
    {
        const string message = "To be able to create a snapshot, you need to precise SnapshotsDirectory and BatchSize in the SyncOptions from the RemoteOrchestrator";

        public SnapshotMissingMandatariesOptionsException() : base(message) { }
    }


    /// <summary>
    /// Occurs when options references are not the same
    /// </summary>
    public class OptionsReferencesAreNotSameExecption : Exception
    {
        const string message = "Remote orchestrator options instance is different from Local orchestrator options instance. Please use the same instance.";

        public OptionsReferencesAreNotSameExecption() : base(message) { }
    }

    /// <summary>
    /// Occurs when setup references are not the same
    /// </summary>
    public class SetupReferencesAreNotSameExecption : Exception
    {
        const string message = "Remote orchestrator setup instance is different from Local orchestrator setup instance. Please use the same instance.";

        public SetupReferencesAreNotSameExecption() : base(message) { }
    }


    /// <summary>
    /// Occurs when a hash from client or server is different from the hash recalculated from server or client
    /// </summary>
    public class SyncHashException : Exception
    {
        const string message = "The batch file is corrupted. Hash is not valid";

        public SyncHashException(string hash1, string hash2) : base(string.Format(message, hash1, hash2)) { }
    }
}
