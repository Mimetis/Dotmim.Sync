using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Extensions;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Dotmim.Sync
{
    public abstract class DotmimBaseException : Exception
    {
        protected static ISerializer serializer = SerializersCollection.JsonSerializerFactory.GetSerializer();

        protected DotmimBaseException()
        {
        }

        protected DotmimBaseException(string message) : base(message)
        {
        }

        protected DotmimBaseException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Exception
    /// </summary>
    public class SyncException : DotmimBaseException
    {
        public SyncException(string message, SyncStage stage = SyncStage.None) : base(message)
        {
            this.SyncStage = stage;
        }

        public SyncException(Exception innerException, SyncStage stage = SyncStage.None) : this(innerException, innerException.Message, stage)
        {
        }

        public SyncException(Exception innerException, string message, SyncStage stage = SyncStage.None) : base(message, innerException)
        {
            this.SyncStage = stage;

            if (innerException is null)
                return;

            if (innerException is SyncException se)
            {
                this.DataSource = se.DataSource;
                this.InitialCatalog = se.InitialCatalog;
                this.TypeName = se.TypeName;
                this.Number = se.Number;
            }
            else
            {
                this.TypeName = innerException.GetType().Name;
            }
        }

        /// <summary>
        /// Base message
        /// </summary>
        public string BaseMessage { get; set; }

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

    }

    /// <summary>
    /// Unknown Exception
    /// </summary>
    public class UnknownException : DotmimBaseException
    {
        public UnknownException(string message) : base(message) { }
    }

    /// <summary>
    /// Rollback Exception
    /// </summary>
    public class RollbackException : DotmimBaseException
    {
        public RollbackException(string message) : base(message) { }
    }

    /// <summary>
    /// Occurs when trying to launch another sync during an in progress sync.
    /// </summary>
    public class AlreadyInProgressException : DotmimBaseException
    {
        const string message = "Synchronization already in progress";

        public AlreadyInProgressException() : base(message) { }
    }



    /// <summary>
    /// Occurs when trying to use a closed connection
    /// </summary>
    public class ConnectionClosedException : DotmimBaseException
    {
        const string message = "The connection to database {0} is closed.";

        public ConnectionClosedException(DbConnection connection) : base(string.Format(message, connection.Database)) { }
    }

    /// <summary>
    /// Occurs when trying to launch another sync during an in progress sync.
    /// </summary>
    public class FormatTypeException : DotmimBaseException
    {
        const string message = "The type {0} is not supported ";

        public FormatTypeException(Type type) : base(string.Format(message, type.Name)) { }
    }


    public class FormatDbTypeException : DotmimBaseException
    {
        const string message = "The DbType {0} is not supported ";

        public FormatDbTypeException(DbType type) : base(string.Format(message, type.ToString())) { }
    }


    /// <summary>
    /// Occurs when a bad SyncProvision is provided to a local orchestrator
    /// </summary>
    public class InvalidRemoteOrchestratorException : DotmimBaseException
    {
        const string message = "The remote orchestrator used here is not able to intercept the OnApplyChangedFailed event, since this event is occuring on the server side only";

        public InvalidRemoteOrchestratorException() : base(message) { }
    }


    /// <summary>
    /// Occurs when a bad SyncProvision is provided to a local orchestrator
    /// </summary>
    public class InvalidProvisionForLocalOrchestratorException : DotmimBaseException
    {
        const string message = "A local database should not have a server scope table. Please provide a correct SyncProvision flag.";

        public InvalidProvisionForLocalOrchestratorException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a bad SyncProvision is provided to a remote orchestrator
    /// </summary>
    public class InvalidProvisionForRemoteOrchestratorException : DotmimBaseException
    {
        const string message = "A server database should not have a client scope table. Please provide a correct SyncProvision flag.";

        public InvalidProvisionForRemoteOrchestratorException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a connection is missing
    /// </summary>
    public class MissingConnectionException : DotmimBaseException
    {
        const string message = "Connection is null";

        public MissingConnectionException() : base(message) { }
    }


    /// <summary>
    /// Occurs when a schema is needed, but does not exists
    /// </summary>
    public class MissingLocalOrchestratorSchemaException : DotmimBaseException
    {
        const string message = "Schema does not exists yet in your local database. You must make a first sync with your server, to initialize everything required locally.";

        public MissingLocalOrchestratorSchemaException() : base(message) { }
    }


    /// <summary>
    /// Occurs when a schema is needed, but does not exists
    /// </summary>
    public class MissingRemoteOrchestratorSchemaException : DotmimBaseException
    {
        const string message = "Schema does not exists yet in your remote database. You must make a first sync with your server, to initialize everything required locally.";

        public MissingRemoteOrchestratorSchemaException() : base(message) { }
    }


    /// <summary>
    /// Occurs when a scope info is needed, but does not exists
    /// </summary>
    public class MissingClientScopeInfoException : DotmimBaseException
    {
        const string message = "The client scope info is invalid. You need to make a first sync before.";

        public MissingClientScopeInfoException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a scope info is needed, but does not exists
    /// </summary>
    public class MissingServerScopeInfoException : DotmimBaseException
    {
        const string message = "The server scope info is invalid. You need to make a first sync before.";

        public MissingServerScopeInfoException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a scope info is not good, conflicting with the one from the orchestrator
    /// </summary>
    public class InvalidScopeInfoException : DotmimBaseException
    {
        const string message = "The scope name is invalid. Be sure to declare a scope name correctly.";

        public InvalidScopeInfoException() : base(message) { }
    }


    /// <summary>
    /// Occurs when a scope info is not good, conflicting with the one from the orchestrator
    /// </summary>
    public class InvalidColumnAutoIncrementException : DotmimBaseException
    {
        const string message = "The column {0} is an auto increment column, but it's not used as a primary key for the table {1}. It's not allowed in DMS. Please consider to remove this column from your sync setup.";

        public InvalidColumnAutoIncrementException(string columnName, string sourceTableName) : base(string.Format(message, columnName, sourceTableName)) { }
    }




    /// <summary>
    /// Occurs when primary key is missing in the table schema
    /// </summary>
    public class MissingPrimaryKeyException : DotmimBaseException
    {
        const string message = "Table {0} does not have any primary key.";

        public MissingPrimaryKeyException(string tableName) : base(string.Format(message, tableName)) { }
    }

    /// <summary>
    /// Setup table exception. Used when a setup table is defined that does not exist in the data source
    /// </summary>
    public class MissingTableException : DotmimBaseException
    {
        const string message = "Table {0} does not exists in database {1}.";

        public MissingTableException(string tableName, string schemaName, string databaseName) : base(string.Format(message, string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}", databaseName)) { }
    }


    /// <summary>
    /// Setup Conflict, when setup provided by the user in code is different from the one in database.
    /// </summary>
    public class SetupConflictOnClientException : DotmimBaseException
    {
        const string message = "Seems you are trying another Setup that what is stored in your client scope database.\n" +
                               "You have already made a sync with a setup that has been stored in the client database.\n" +
                               "And you are trying now a new setup in your code, different from the one you have used before.\n" +
                               "If you want to use 2 differents setups, please use a different a scope name for each setup.\n" +
                               "If you want to replace the setup stored in database with a new one, make a migration (see docs).\n" +
                               "-----------------------------------------------------\n" +
                               "Setup you trying to use from your code: {0}\n" +
                               "-----------------------------------------------------\n" +
                               "Setup found in your database: {1}\n" +
                               "-----------------------------------------------------\n";

        public SetupConflictOnClientException(SyncSetup inputSetup, SyncSetup clientScopeInfoSetup) : base(string.Format(message, serializer.Serialize(inputSetup).ToUtf8String(), serializer.Serialize(clientScopeInfoSetup).ToUtf8String())) { }
    }

    /// <summary>
    /// Setup Conflict, when setup provided by the user in code is different from the one in database.
    /// </summary>
    public class SetupConflictOnServerException : DotmimBaseException
    {
        const string message = "Seems you are trying another Setup that what is stored in your server scope database.\n" +
                               "You have already made a sync with a setup that has been stored in the server (and client) database.\n" +
                               "And you are trying now a new setup in your code, different from the one you have used before.\n" +
                               "If you want to use 2 differents setups, please use a different a scope name for each setup.\n" +
                               "If you want to replace the setup stored in database with a new one, make a migration (see docs).\n" +
                               "-----------------------------------------------------\n" +
                               "Setup you trying to use from your code: {0}\n" +
                               "-----------------------------------------------------\n" +
                               "Setup found in your database: {1}\n" +
                               "-----------------------------------------------------\n";

        public SetupConflictOnServerException(SyncSetup inputSetup, SyncSetup clientScopeInfoSetup) : base(string.Format(message, serializer.Serialize(inputSetup).ToUtf8String(), serializer.Serialize(clientScopeInfoSetup).ToUtf8String())) { }
    }

    /// <summary>
    /// Setup column exception. Used when a setup column  is defined that does not exist in the data source table
    /// </summary>
    public class MissingColumnException : DotmimBaseException
    {
        const string message = "Column {0} does not exists in the table {1}.";

        public MissingColumnException(string columnName, string sourceTableName) : base(string.Format(message, columnName, sourceTableName)) { }
    }

    /// <summary>
    /// Setup columns exception. Used when a setup table has no columns during provisioning.
    /// </summary>
    public class MissingsColumnException : DotmimBaseException
    {
        const string message = "Table {0} has no columns.";

        public MissingsColumnException(string sourceTableName) : base(string.Format(message, sourceTableName)) { }
    }


    /// <summary>
    /// Setup column exception. Used when a setup column  is defined that does not exist in the data source table
    /// </summary>
    public class MissingPrimaryKeyColumnException : DotmimBaseException
    {
        const string message = "Primary key column {0} should be part of the columns list in your Setup table {1}.";

        public MissingPrimaryKeyColumnException(string columnName, string sourceTableName) : base(string.Format(message, columnName, sourceTableName)) { }
    }

    /// <summary>
    /// Setup table exception. Used when a your setup does not contains any table
    /// </summary>
    public class MissingProviderException : DotmimBaseException
    {
        const string message = "You need a provider for {0}.";

        public MissingProviderException(string methodName) : base(string.Format(message, methodName)) { }
    }
    /// <summary>
    /// Setup table exception. Used when a your setup does not contains any table
    /// </summary>
    public class MissingTablesException : DotmimBaseException
    {
        const string message = "Your setup does not contains any table.";

        public MissingTablesException() : base(message) { }
    }

    /// <summary>
    /// Setup table exception. Used when a your setup does not contains any table
    /// </summary>
    public class MissingServerScopeTablesException : DotmimBaseException
    {
        const string message = "Your server scope {0} is not existing on server, or you did not provide a setup with tables to provision on the server.";

        public MissingServerScopeTablesException(string scopeName) : base(string.Format(message, scopeName)) { }
    }


    /// <summary>
    /// No schema in the scope
    /// </summary>
    public class MissingSchemaInScopeException : DotmimBaseException
    {
        const string message = "Your scope does not contains any schema.";

        public MissingSchemaInScopeException() : base(message) { }
    }


    /// <summary>
    /// Setup table exception. Used when a your setup does not contains any columns in table
    /// </summary>
    public class MissingColumnsException : DotmimBaseException
    {
        const string message = "Your setup does not contains any column.";

        public MissingColumnsException() : base(message) { }
    }


    /// <summary>
    /// During a migration, droping a table is not allowed
    /// </summary>
    public class MigrationTableDropNotAllowedException : DotmimBaseException
    {
        const string message = "During a migration, droping a table is not allowed";

        public MigrationTableDropNotAllowedException() : base(message) { }
    }

    /// <summary>
    /// Metadata exception.
    /// </summary>
    public class MetadataException : DotmimBaseException
    {
        const string message = "No metadatas rows found for table {0}.";

        public MetadataException(string tableName) : base(string.Format(message, tableName)) { }
    }


    /// <summary>
    /// Occurs when a row is too big for download batch size
    /// </summary>
    public class RowOverSizedException : DotmimBaseException
    {
        const string message = "Row is too big ({0} kb.) for the current DownloadBatchSizeInKB.";

        public RowOverSizedException(string finalFieldSize) : base(string.Format(message, finalFieldSize)) { }
    }

    /// <summary>
    /// Occurs when a command is missing
    /// </summary>
    public class MissingCommandException : DotmimBaseException
    {
        const string message = "Missing command {0}.";

        public MissingCommandException(string commandType) : base(string.Format(message, commandType)) { }
    }

    /// <summary>
    /// Occurs when we use change tracking and it's not enabled on the source database
    /// </summary>
    public class MissingChangeTrackingException : DotmimBaseException
    {
        const string message = "Change Tracking is not activated for database {0}. Please execute this statement : Alter database {0} SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)";

        public MissingChangeTrackingException(string databaseName) : base(string.Format(message, databaseName)) { }
    }

    /// <summary>
    /// Occurs when we local orchestrator tries to update untracked rows, but no tracking table exists
    /// </summary>
    public class MissingTrackingTableException : DotmimBaseException
    {
        const string message = "No tracking table for table {0}. Please Provision your database before calling this method";

        public MissingTrackingTableException(string tableName) : base(string.Format(message, tableName)) { }
    }


    /// <summary>
    /// Occurs when we check database existence
    /// </summary>
    public class MissingDatabaseException : DotmimBaseException
    {

        const string message = "Database {0} does not exist";

        public MissingDatabaseException(string databaseName) : base(string.Format(message, databaseName)) { }
    }


    /// <summary>
    /// Occurs when we check database existence
    /// </summary>
    public class InvalidDatabaseVersionException : DotmimBaseException
    {

        const string message = "Engine {1} version {0} is not supported. Please upgrade your server to the last version.";

        public InvalidDatabaseVersionException(string version, string engine) : base(string.Format(message, version, engine)) { }
    }



    /// <summary>
    /// Occurs when a column is not supported by the Dotmim.Sync framework
    /// </summary>
    public class UnsupportedColumnTypeException : DotmimBaseException
    {
        const string message = "In table {0}, the Column {1} of type {2} from provider {3} is not currently supported.";

        public UnsupportedColumnTypeException(string tableName, string columnName, string columnType, string provider) : base(string.Format(message, tableName, columnName, columnType, provider)) { }
    }
    /// <summary>
    /// Occurs when a column name is not supported by the Dotmim.Sync framework
    /// </summary>
    public class UnsupportedColumnNameException : DotmimBaseException
    {
        const string message = "In table {0}, the Column name {1} is not allowed. Please consider to change the column name.";

        public UnsupportedColumnNameException(string tableName, string columnName, string columnType, string provider) :
            base(string.Format(message, tableName, columnName, columnType, provider))
        { }
    }

    /// <summary>
    /// Occurs when a column name is not supported by the Dotmim.Sync framework for a primary key
    /// </summary>
    public class UnsupportedPrimaryKeyColumnNameException : DotmimBaseException
    {
        const string message = "In table {0}, the Column name {1} is not allowed as a primary key. Please consider to change the column name or choose another primary key for your table.";

        public UnsupportedPrimaryKeyColumnNameException(string tableName, string columnName, string columnType, string provider)
            : base(string.Format(message, tableName, columnName, columnType, provider)) { }
    }


    /// <summary>
    /// Occurs when a provider not supported as a server provider is used with a RemoteOrchestrator.
    /// </summary>
    public class UnsupportedServerProviderException : DotmimBaseException
    {
        const string message = "The provider {0} can not be used as a server provider";

        public UnsupportedServerProviderException(string provider) : base(string.Format(message, provider)) { }
    }



    /// <summary>
    /// Occurs when sync metadatas are out of date
    /// </summary>
    public class OutOfDateException : DotmimBaseException
    {
        const string message = "Client database is out of date. Last client sync timestamp:{0}. Last server cleanup metadata:{1} Try to make a Reinitialize sync.";

        public OutOfDateException(long? timestampLimit, long? serverLastCleanTimestamp) : base(string.Format(message, timestampLimit, serverLastCleanTimestamp)) { }
    }

    /// <summary>
    /// Http empty response exception.
    /// </summary>
    public class HttpEmptyResponseContentException : DotmimBaseException
    {
        const string message = "The reponse has an empty body.";

        public HttpEmptyResponseContentException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a header is missing in the http request
    /// </summary>
    public class HttpHeaderMissingException : DotmimBaseException
    {
        const string message = "Header {0} is missing.";

        public HttpHeaderMissingException(string header) : base(string.Format(message, header)) { }
    }

    /// <summary>
    /// Occurs when a cache is not set on the server
    /// </summary>
    public class HttpCacheNotConfiguredException : DotmimBaseException
    {
        const string message = "Cache is not configured! Please add memory cache (distributed or not). See https://docs.microsoft.com/en-us/aspnet/core/performance/caching/response?view=aspnetcore-2.2).";

        public HttpCacheNotConfiguredException() : base(message) { }
    }

    /// <summary>
    /// Occurs when a Serializer is not available on the server side
    /// </summary>
    public class HttpSerializerNotConfiguredException : DotmimBaseException
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
    public class HttpConverterNotConfiguredException : DotmimBaseException
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
    public class HttpScopeNameInvalidException : DotmimBaseException
    {
        const string message = "The scope {0} does not exist on the server side. Please provider a correct scope name";

        public HttpScopeNameInvalidException(string scopeName) : base(string.Format(message, scopeName)) { }
    }

    /// <summary>
    /// Occurs when a parameter has been already added in a filter parameter list
    /// </summary>
    public class HttpScopeNameFromClientIsInvalidException : DotmimBaseException
    {
        const string message = "Scope name received from client {0} is different from the scope name specified in the web server agent {1}";

        public HttpScopeNameFromClientIsInvalidException(string scopeNameClientReceived, string scopeNameServerDeclared) 
            : base(string.Format(message, scopeNameClientReceived, scopeNameServerDeclared)) { }
    }

    /// <summary>
    /// Occurs when a session is lost during a sync session
    /// </summary>
    public class HttpSessionLostException : DotmimBaseException
    {
        const string message = "Session loss: No batchPartInfo could found for the current sessionId {0}. It seems the session was lost. Please try again.";

        public HttpSessionLostException(string sessionId) : base(string.Format(message, sessionId)) { }
    }



    /// <summary>
    /// Occurs when a parameter has been already added in a filter parameter list
    /// </summary>
    public class FilterParameterAlreadyExistsException : DotmimBaseException
    {
        const string message = "The parameter {0} has been already added for the {1} changes stored procedure";

        public FilterParameterAlreadyExistsException(string parameterName, string tableName) : base(string.Format(message, parameterName, tableName)) { }
    }

    /// <summary>
    /// Occurs when a filter already exists for a named table
    /// </summary>
    public class FilterAlreadyExistsException : DotmimBaseException
    {
        const string message = "The filter for the {0} changes stored procedure already exists";

        public FilterAlreadyExistsException(string tableName) : base(string.Format(message, tableName)) { }
    }


    /// <summary>
    /// Occurs when a filter column used as a filter for a tracking table, has not been added to the column parameters list
    /// </summary>
    public class FilterTrackingWhereException : DotmimBaseException
    {
        const string message = "The column {0} does not exist in the columns parameters list, so can't be add as a where filter clause to the tracking table";

        public FilterTrackingWhereException(string columName) : base(string.Format(message, columName)) { }
    }


    /// <summary>
    /// Occurs when a filter column used as a filter for a tracking table, but not exists
    /// </summary>
    public class FilterParamColumnNotExistsException : DotmimBaseException
    {
        const string message = "The parameter {0} does not exist as a column in the table {1}";

        public FilterParamColumnNotExistsException(string columName, string tableName) : base(string.Format(message, columName, tableName)) { }
    }

    /// <summary>
    /// Occurs when a filter column used as a filter for a tracking table, but not exists
    /// </summary>
    public class FilterParamTableNotExistsException : DotmimBaseException
    {
        const string message = "The table {0} does not exist";

        public FilterParamTableNotExistsException(string tableName) : base(string.Format(message, tableName)) { }
    }

    /// <summary>
    /// Occurs when a parameter has been already added to the parameter collection
    /// </summary>
    public class SyncParameterAlreadyExistsException : DotmimBaseException
    {
        const string message = "The parameter {0} already exists in the parameter list.";

        public SyncParameterAlreadyExistsException(string parameterName) : base(string.Format(message, parameterName)) { }
    }


    /// <summary>
    /// Occurs when trying to apply a snapshot that does not exists
    /// </summary>
    public class SnapshotNotExistsException : DotmimBaseException
    {
        const string message = "The snapshot {0} does not exists.";

        public SnapshotNotExistsException(string directoryName) : base(string.Format(message, directoryName)) { }
    }

    /// <summary>
    /// Occurs when trying to create a snapshot but no directory and size have been set in the options
    /// </summary>
    public class SnapshotMissingMandatariesOptionsException : DotmimBaseException
    {
        const string message = "To be able to create a snapshot, you need to precise SnapshotsDirectory and BatchSize in the SyncOptions from the RemoteOrchestrator";

        public SnapshotMissingMandatariesOptionsException() : base(message) { }
    }


    /// <summary>
    /// Occurs when options references are not the same
    /// </summary>
    public class OptionsReferencesAreNotSameExecption : DotmimBaseException
    {
        const string message = "Remote orchestrator options instance is different from Local orchestrator options instance. Please use the same instance.";

        public OptionsReferencesAreNotSameExecption() : base(message) { }
    }

    /// <summary>
    /// Occurs when setup references are not the same
    /// </summary>
    public class SetupReferencesAreNotSameExecption : DotmimBaseException
    {
        const string message = "Remote orchestrator setup instance is different from Local orchestrator setup instance. Please use the same instance.";

        public SetupReferencesAreNotSameExecption() : base(message) { }
    }


    /// <summary>
    /// Occurs when a hash from client or server is different from the hash recalculated from server or client
    /// </summary>
    public class SyncHashException : DotmimBaseException
    {
        const string message = "The batch file is corrupted. Hash is not valid";

        public SyncHashException(string hash1, string hash2) : base(string.Format(message, hash1, hash2)) { }
    }


    public class ApplyChangesException : DotmimBaseException
    {
        const string message = "Error on table [{0}]: {1}. Row:{2}. ApplyType:{3}";

        public ApplyChangesException(SyncRow errorRow, SyncTable schemaChangesTable, SyncRowState rowState, Exception innerException)
            : base(string.Format(message, schemaChangesTable.GetFullName(), innerException.Message, errorRow, rowState), innerException) { }
    }
}
