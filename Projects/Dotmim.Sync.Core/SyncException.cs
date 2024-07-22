using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Extensions;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;

namespace Dotmim.Sync
{
    /// <summary>
    /// Exception.
    /// </summary>
    public class SyncException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SyncException "/> class.
        /// </summary>
        public SyncException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncException "/>  class with a specified error message.
        /// </summary>
        public SyncException(string message)
            : base(message) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncException "/>  class with a specified error message and a reference to the inner exception that is the cause of this exception.
        /// </summary>
        public SyncException(string message, Exception innerException)
            : base(message, innerException) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncException "/>  class with a specified error message and a reference to current sync stage.
        /// </summary>
        public SyncException(string message, SyncStage stage = SyncStage.None)
            : base(message) => this.SyncStage = stage;

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncException "/>  class with a reference to the inner exception that is the cause of this exception and the current sync stage.
        /// </summary>
        public SyncException(Exception innerException, SyncStage stage = SyncStage.None)
            : this(innerException, innerException?.Message, stage) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncException "/>  class with a specified error message, a reference to the inner exception that is the cause of this exception and the current sync stage.
        /// </summary>
        public SyncException(Exception innerException, string message, SyncStage stage = SyncStage.None)
            : base(message, innerException)
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
        /// Gets or sets base message.
        /// </summary>
        public string BaseMessage { get; set; }

        /// <summary>
        /// Gets or Sets type name of exception.
        /// </summary>
        public string TypeName { get; set; }

        /// <summary>
        /// Gets or sets sync stage when exception occured.
        /// </summary>
        public SyncStage SyncStage { get; set; }

        /// <summary>
        /// Gets or sets data source error number if available.
        /// </summary>
        public int Number { get; set; }

        /// <summary>
        /// Gets or Sets data source if available.
        /// </summary>
        public string DataSource { get; set; }

        /// <summary>
        /// Gets or Sets initial catalog if available.
        /// </summary>
        public string InitialCatalog { get; set; }
    }

    /// <summary>
    /// Unknown Exception.
    /// </summary>
    public class UnknownException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="UnknownException"/> class.
        /// </summary>
        public UnknownException(string message)
            : base(message) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="UnknownException"/> class.
        /// </summary>
        public UnknownException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="UnknownException"/> class.
        /// </summary>
        public UnknownException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Rollback Exception.
    /// </summary>
    public class RollbackException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RollbackException"/> class.
        /// </summary>
        public RollbackException(string message)
            : base(message) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="RollbackException"/> class.
        /// </summary>
        public RollbackException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RollbackException"/> class.
        /// </summary>
        public RollbackException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when trying to launch another sync during an in progress sync.
    /// </summary>
    public class AlreadyInProgressException : Exception
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="AlreadyInProgressException"/> class.
        /// </summary>
        public AlreadyInProgressException()
            : base("Synchronization already in progress") { }
    }

    /// <summary>
    /// Occurs when trying to use a closed connection.
    /// </summary>
    public class ConnectionClosedException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionClosedException"/> class.
        /// </summary>
        public ConnectionClosedException(DbConnection connection)
            : base(string.Format(CultureInfo.InvariantCulture, "The connection to database {0} is closed.", connection?.Database)) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionClosedException"/> class.
        /// </summary>
        public ConnectionClosedException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionClosedException"/> class.
        /// </summary>
        public ConnectionClosedException(string message)
            : base(message) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConnectionClosedException"/> class.
        /// </summary>
        public ConnectionClosedException(string message, Exception innerException)
            : base(message, innerException) { }
    }

    /// <summary>
    /// Occurs when a type is not supported.
    /// </summary>
    public class FormatTypeException : Exception
    {
        private new const string Message = "The type {0} is not supported ";

        /// <inheritdoc cref="FormatTypeException"/>/>
        public FormatTypeException(Type type)
            : base(string.Format(Message, type.Name)) { }

        /// <inheritdoc cref="FormatTypeException"/>/>
        public FormatTypeException()
        {
        }

        /// <inheritdoc cref="FormatTypeException"/>/>
        public FormatTypeException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="FormatTypeException"/>/>
        public FormatTypeException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a DbType is not supported.
    /// </summary>
    public class FormatDbTypeException : Exception
    {
        private new const string Message = "The DbType {0} is not supported ";

        /// <inheritdoc cref="FormatDbTypeException"/>
        public FormatDbTypeException(DbType type)
            : base(string.Format(Message, type.ToString())) { }

        /// <inheritdoc cref="FormatDbTypeException"/>
        public FormatDbTypeException()
        {
        }

        /// <inheritdoc cref="FormatDbTypeException"/>
        public FormatDbTypeException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when a bad SyncProvision is provided to a local orchestrator.
    /// </summary>
    public class InvalidRemoteOrchestratorException : Exception
    {
        private new const string Message = "The remote orchestrator used here is not able to intercept the OnApplyChangedFailed event, since this event is occuring on the server side only";

        /// <inheritdoc cref="InvalidRemoteOrchestratorException"/>
        public InvalidRemoteOrchestratorException()
            : base(Message) { }

        /// <inheritdoc cref="InvalidRemoteOrchestratorException"/>
        public InvalidRemoteOrchestratorException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="InvalidRemoteOrchestratorException"/>
        public InvalidRemoteOrchestratorException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a bad SyncProvision is provided to a local orchestrator.
    /// </summary>
    public class InvalidProvisionForLocalOrchestratorException : Exception
    {
        private new const string Message = "A local database should not have a server scope table. Please provide a correct SyncProvision flag.";

        /// <inheritdoc cref="InvalidProvisionForLocalOrchestratorException"/>
        public InvalidProvisionForLocalOrchestratorException()
            : base(Message) { }

        /// <inheritdoc cref="InvalidProvisionForLocalOrchestratorException"/>
        public InvalidProvisionForLocalOrchestratorException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="InvalidProvisionForLocalOrchestratorException"/>
        public InvalidProvisionForLocalOrchestratorException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a bad SyncProvision is provided to a remote orchestrator.
    /// </summary>
    public class InvalidProvisionForRemoteOrchestratorException : Exception
    {
        private new const string Message = "A server database should not have a client scope table. Please provide a correct SyncProvision flag.";

        /// <inheritdoc cref="InvalidProvisionForRemoteOrchestratorException"/>
        public InvalidProvisionForRemoteOrchestratorException()
            : base(Message) { }

        /// <inheritdoc cref="InvalidProvisionForRemoteOrchestratorException"/>
        public InvalidProvisionForRemoteOrchestratorException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="InvalidProvisionForRemoteOrchestratorException"/>
        public InvalidProvisionForRemoteOrchestratorException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a connection is missing.
    /// </summary>
    public class MissingConnectionException : Exception
    {
        private new const string Message = "Connection is null";

        /// <inheritdoc cref="MissingConnectionException"/>
        public MissingConnectionException()
            : base(Message) { }

        /// <inheritdoc cref="MissingConnectionException"/>
        public MissingConnectionException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="MissingConnectionException"/>
        public MissingConnectionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a schema is needed, but does not exists.
    /// </summary>
    public class MissingLocalOrchestratorSchemaException : Exception
    {
        private new const string Message = "Schema does not exists yet in your local database. You must make a first sync with your server, to initialize everything required locally.";

        /// <inheritdoc cref="MissingLocalOrchestratorSchemaException"/>
        public MissingLocalOrchestratorSchemaException()
            : base(Message) { }

        /// <inheritdoc cref="MissingLocalOrchestratorSchemaException"/>
        public MissingLocalOrchestratorSchemaException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="MissingLocalOrchestratorSchemaException"/>
        public MissingLocalOrchestratorSchemaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a schema is needed, but does not exists.
    /// </summary>
    public class MissingRemoteOrchestratorSchemaException : Exception
    {
        private new const string Message = "Schema does not exists yet in your remote database. You must make a first sync with your server, to initialize everything required locally.";

        /// <inheritdoc cref="MissingRemoteOrchestratorSchemaException"/>
        public MissingRemoteOrchestratorSchemaException()
            : base(Message) { }

        /// <inheritdoc cref="MissingRemoteOrchestratorSchemaException"/>
        public MissingRemoteOrchestratorSchemaException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="MissingRemoteOrchestratorSchemaException"/>
        public MissingRemoteOrchestratorSchemaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a scope info is needed, but does not exists.
    /// </summary>
    public class MissingClientScopeInfoException : Exception
    {
        private new const string Message = "The client scope info is invalid. You need to make a first sync before.";

        /// <inheritdoc cref="MissingClientScopeInfoException"/>
        public MissingClientScopeInfoException()
            : base(Message) { }

        /// <inheritdoc cref="MissingClientScopeInfoException"/>
        public MissingClientScopeInfoException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="MissingClientScopeInfoException"/>
        public MissingClientScopeInfoException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a scope info is needed, but does not exists.
    /// </summary>
    public class MissingServerScopeInfoException : Exception
    {
        private new const string Message = "The server scope info is invalid. You need to make a first sync before.";

        /// <inheritdoc cref="MissingServerScopeInfoException"/>
        public MissingServerScopeInfoException()
            : base(Message) { }

        /// <inheritdoc cref="MissingServerScopeInfoException"/>
        public MissingServerScopeInfoException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="MissingServerScopeInfoException"/>
        public MissingServerScopeInfoException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a scope info is not good, conflicting with the one from the orchestrator.
    /// </summary>
    public class InvalidScopeInfoException : Exception
    {
        private new const string Message = "The scope name is invalid. Be sure to declare a scope name correctly.";

        /// <inheritdoc cref="InvalidScopeInfoException"/>
        public InvalidScopeInfoException()
            : base(Message) { }

        /// <inheritdoc cref="InvalidScopeInfoException"/>
        public InvalidScopeInfoException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="InvalidScopeInfoException"/>
        public InvalidScopeInfoException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a scope info is not good, conflicting with the one from the orchestrator.
    /// </summary>
    public class InvalidColumnAutoIncrementException : Exception
    {
        private new const string Message = "The column {0} is an auto increment column, but it's not used as a primary key for the table {1}. It's not allowed in DMS. Please consider to remove this column from your sync setup.";

        /// <inheritdoc cref="InvalidColumnAutoIncrementException"/>
        public InvalidColumnAutoIncrementException(string columnName, string sourceTableName)
            : base(string.Format(Message, columnName, sourceTableName)) { }

        /// <inheritdoc cref="InvalidColumnAutoIncrementException"/>
        public InvalidColumnAutoIncrementException()
        {
        }

        /// <inheritdoc cref="InvalidColumnAutoIncrementException"/>
        public InvalidColumnAutoIncrementException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when primary key is missing in the table schema.
    /// </summary>
    public class MissingPrimaryKeyException : Exception
    {
        private new const string Message = "Table {0} does not have any primary key.";

        /// <inheritdoc cref="MissingPrimaryKeyException"/>
        public MissingPrimaryKeyException(string tableName)
            : base(string.Format(Message, tableName)) { }

        /// <inheritdoc cref="MissingPrimaryKeyException"/>
        public MissingPrimaryKeyException()
        {
        }

        /// <inheritdoc cref="MissingPrimaryKeyException"/>
        public MissingPrimaryKeyException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Setup table exception. Used when a setup table is defined that does not exist in the data source.
    /// </summary>
    public class MissingTableException : Exception
    {
        private new const string Message = "Table {0} does not exists in database {1}.";

        /// <inheritdoc cref="MissingTableException"/>
        public MissingTableException(string tableName, string schemaName, string databaseName)
            : base(string.Format(Message, string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}", databaseName)) { }

        /// <inheritdoc cref="MissingTableException"/>
        public MissingTableException()
        {
        }

        /// <inheritdoc cref="MissingTableException"/>
        public MissingTableException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Setup Conflict, when setup provided by the user in code is different from the one in database.
    /// </summary>
    public class SetupConflictOnClientException : Exception
    {
        private new const string Message = "Seems you are trying another Setup that what is stored in your client scope database.\n" +
                               "You have already made a sync with a setup that has been stored in the client database.\n" +
                               "And you are trying now a new setup in your code, different from the one you have used before.\n" +
                               "If you want to use 2 differents setups, please use a different a scope name for each setup.\n" +
                               "If you want to replace the setup stored in database with a new one, make a migration (see docs).\n" +
                               "-----------------------------------------------------\n" +
                               "Setup you trying to use from your code: {0}\n" +
                               "-----------------------------------------------------\n" +
                               "Setup found in your database: {1}\n" +
                               "-----------------------------------------------------\n";

        private static ISerializer serializer = SerializersFactory.JsonSerializerFactory.GetSerializer();

        /// <inheritdoc cref="SetupConflictOnClientException"/>
        public SetupConflictOnClientException(SyncSetup inputSetup, SyncSetup clientScopeInfoSetup)
            : base(string.Format(CultureInfo.InvariantCulture, Message, serializer.Serialize(inputSetup).ToUtf8String(), serializer.Serialize(clientScopeInfoSetup).ToUtf8String())) { }

        /// <inheritdoc cref="SetupConflictOnClientException"/>
        public SetupConflictOnClientException()
        {
        }

        /// <inheritdoc cref="SetupConflictOnClientException"/>
        public SetupConflictOnClientException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Setup Conflict, when setup provided by the user in code is different from the one in database.
    /// </summary>
    public class SetupConflictOnServerException : Exception
    {
        private new const string Message = "Seems you are trying another Setup that what is stored in your server scope database.\n" +
                               "You have already made a sync with a setup that has been stored in the server (and client) database.\n" +
                               "And you are trying now a new setup in your code, different from the one you have used before.\n" +
                               "If you want to use 2 differents setups, please use a different a scope name for each setup.\n" +
                               "If you want to replace the setup stored in database with a new one, make a migration (see docs).\n" +
                               "-----------------------------------------------------\n" +
                               "Setup you trying to use from your code: {0}\n" +
                               "-----------------------------------------------------\n" +
                               "Setup found in your database: {1}\n" +
                               "-----------------------------------------------------\n";

        private static ISerializer serializer = SerializersFactory.JsonSerializerFactory.GetSerializer();

        /// <inheritdoc cref="SetupConflictOnServerException"/>
        public SetupConflictOnServerException(SyncSetup inputSetup, SyncSetup clientScopeInfoSetup)
            : base(string.Format(Message, serializer.Serialize(inputSetup).ToUtf8String(), serializer.Serialize(clientScopeInfoSetup).ToUtf8String())) { }

        /// <inheritdoc cref="SetupConflictOnServerException"/>
        public SetupConflictOnServerException()
        {
        }

        /// <inheritdoc cref="SetupConflictOnServerException"/>
        public SetupConflictOnServerException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Setup column exception. Used when a setup column  is defined that does not exist in the data source table.
    /// </summary>
    public class MissingColumnException : Exception
    {
        private new const string Message = "Column {0} does not exists in the table {1}.";

        /// <inheritdoc cref="MissingColumnException"/>
        public MissingColumnException(string columnName, string sourceTableName)
            : base(string.Format(Message, columnName, sourceTableName)) { }

        /// <inheritdoc cref="MissingColumnException"/>
        public MissingColumnException()
        {
        }

        /// <inheritdoc cref="MissingColumnException"/>
        public MissingColumnException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Setup columns exception. Used when a setup table has no columns during provisioning.
    /// </summary>
    public class MissingsColumnException : Exception
    {
        private new const string Message = "Table {0} has no columns.";

        /// <inheritdoc cref="MissingsColumnException"/>
        public MissingsColumnException(string sourceTableName)
            : base(string.Format(Message, sourceTableName)) { }

        /// <inheritdoc cref="MissingsColumnException"/>
        public MissingsColumnException()
        {
        }

        /// <inheritdoc cref="MissingsColumnException"/>
        public MissingsColumnException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Setup column exception. Used when a setup column  is defined that does not exist in the data source table.
    /// </summary>
    public class MissingPrimaryKeyColumnException : Exception
    {
        private new const string Message = "Primary key column {0} should be part of the columns list in your Setup table {1}.";

        /// <inheritdoc cref="MissingPrimaryKeyColumnException"/>
        public MissingPrimaryKeyColumnException(string columnName, string sourceTableName)
            : base(string.Format(Message, columnName, sourceTableName)) { }

        /// <inheritdoc cref="MissingPrimaryKeyColumnException"/>
        public MissingPrimaryKeyColumnException()
        {
        }

        /// <inheritdoc cref="MissingPrimaryKeyColumnException"/>
        public MissingPrimaryKeyColumnException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Setup table exception. Used when a your setup does not contains any table.
    /// </summary>
    public class MissingProviderException : Exception
    {
        private new const string Message = "You need a provider for {0}.";

        /// <inheritdoc cref="MissingProviderException"/>
        public MissingProviderException(string methodName)
            : base(string.Format(Message, methodName)) { }

        /// <inheritdoc cref="MissingProviderException"/>
        public MissingProviderException()
        {
        }

        /// <inheritdoc cref="MissingProviderException"/>
        public MissingProviderException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Setup table exception. Used when a your setup does not contains any table.
    /// </summary>
    public class MissingTablesException : Exception
    {
        private new const string Message = "Your setup does not contains any table.";

        /// <inheritdoc cref="MissingTablesException"/>
        public MissingTablesException()
            : base(Message) { }

        /// <inheritdoc cref="MissingTablesException"/>
        public MissingTablesException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="MissingTablesException"/>
        public MissingTablesException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Setup table exception. Used when a your setup does not contains any table.
    /// </summary>
    public class MissingServerScopeTablesException : Exception
    {
        private new const string Message = "Your server scope {0} is not existing on server, or you did not provide a setup with tables to provision on the server.";

        /// <inheritdoc cref="MissingServerScopeTablesException"/>
        public MissingServerScopeTablesException(string scopeName)
            : base(string.Format(Message, scopeName)) { }

        /// <inheritdoc cref="MissingServerScopeTablesException"/>
        public MissingServerScopeTablesException()
        {
        }

        /// <inheritdoc cref="MissingServerScopeTablesException"/>
        public MissingServerScopeTablesException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// No schema in the scope.
    /// </summary>
    public class MissingSchemaInScopeException : Exception
    {
        private new const string Message = "Your scope does not contains any schema.";

        /// <inheritdoc cref="MissingSchemaInScopeException"/>
        public MissingSchemaInScopeException()
            : base(Message) { }

        /// <inheritdoc cref="MissingSchemaInScopeException"/>
        public MissingSchemaInScopeException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="MissingSchemaInScopeException"/>
        public MissingSchemaInScopeException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Setup table exception. Used when a your setup does not contains any columns in table.
    /// </summary>
    public class MissingColumnsException : Exception
    {
        private new const string Message = "Your setup does not contains any column.";

        /// <inheritdoc cref="MissingColumnsException"/>
        public MissingColumnsException()
            : base(Message) { }

        /// <inheritdoc cref="MissingColumnsException"/>
        public MissingColumnsException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="MissingColumnsException"/>
        public MissingColumnsException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// During a migration, droping a table is not allowed.
    /// </summary>
    public class MigrationTableDropNotAllowedException : Exception
    {
        private new const string Message = "During a migration, droping a table is not allowed";

        /// <inheritdoc cref="MigrationTableDropNotAllowedException"/>
        public MigrationTableDropNotAllowedException()
            : base(Message) { }

        /// <inheritdoc cref="MigrationTableDropNotAllowedException"/>
        public MigrationTableDropNotAllowedException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="MigrationTableDropNotAllowedException"/>
        public MigrationTableDropNotAllowedException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Metadata exception.
    /// </summary>
    public class MetadataException : Exception
    {
        private new const string Message = "No metadatas rows found for table {0}.";

        /// <inheritdoc cref="MetadataException"/>
        public MetadataException(string tableName)
            : base(string.Format(Message, tableName)) { }

        /// <inheritdoc cref="MetadataException"/>
        public MetadataException()
        {
        }

        /// <inheritdoc cref="MetadataException"/>
        public MetadataException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a row is too big for download batch size.
    /// </summary>
    public class RowOverSizedException : Exception
    {
        private new const string Message = "Row is too big ({0} kb.) for the current DownloadBatchSizeInKB.";

        /// <inheritdoc cref="RowOverSizedException"/>
        public RowOverSizedException(string finalFieldSize)
            : base(string.Format(Message, finalFieldSize)) { }

        /// <inheritdoc cref="RowOverSizedException"/>
        public RowOverSizedException()
        {
        }

        /// <inheritdoc cref="RowOverSizedException"/>
        public RowOverSizedException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a command is missing.
    /// </summary>
    public class MissingCommandException : Exception
    {
        private new const string Message = "Missing command {0}.";

        /// <inheritdoc cref="MissingCommandException"/>
        public MissingCommandException(string commandType)
            : base(string.Format(Message, commandType)) { }

        /// <inheritdoc cref="MissingCommandException"/>
        public MissingCommandException()
        {
        }

        /// <inheritdoc cref="MissingCommandException"/>
        public MissingCommandException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when we use change tracking and it's not enabled on the source database.
    /// </summary>
    public class MissingChangeTrackingException : Exception
    {
        private new const string Message = "Change Tracking is not activated for database {0}. Please execute this statement : Alter database {0} SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)";

        /// <inheritdoc cref="MissingChangeTrackingException"/>
        public MissingChangeTrackingException(string databaseName)
            : base(string.Format(Message, databaseName)) { }

        /// <inheritdoc cref="MissingChangeTrackingException"/>
        public MissingChangeTrackingException()
        {
        }

        /// <inheritdoc cref="MissingChangeTrackingException"/>
        public MissingChangeTrackingException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when we local orchestrator tries to update untracked rows, but no tracking table exists.
    /// </summary>
    public class MissingTrackingTableException : Exception
    {
        private new const string Message = "No tracking table for table {0}. Please Provision your database before calling this method";

        /// <inheritdoc cref="MissingTrackingTableException"/>
        public MissingTrackingTableException(string tableName)
            : base(string.Format(Message, tableName)) { }

        /// <inheritdoc cref="MissingTrackingTableException"/>
        public MissingTrackingTableException()
        {
        }

        /// <inheritdoc cref="MissingTrackingTableException"/>
        public MissingTrackingTableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when we check database existence.
    /// </summary>
    public class MissingDatabaseException : Exception
    {

        private new const string Message = "Database {0} does not exist";

        /// <inheritdoc cref="MissingDatabaseException"/>
        public MissingDatabaseException(string databaseName)
            : base(string.Format(Message, databaseName)) { }

        /// <inheritdoc cref="MissingDatabaseException"/>
        public MissingDatabaseException()
        {
        }

        /// <inheritdoc cref="MissingDatabaseException"/>
        public MissingDatabaseException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when we check database existence.
    /// </summary>
    public class InvalidDatabaseVersionException : Exception
    {

        private new const string Message = "Engine {1} version {0} is not supported. Please upgrade your server to the last version.";

        /// <inheritdoc cref="InvalidDatabaseVersionException"/>
        public InvalidDatabaseVersionException(string version, string engine)
            : base(string.Format(Message, version, engine)) { }

        /// <inheritdoc cref="InvalidDatabaseVersionException"/>
        public InvalidDatabaseVersionException()
        {
        }

        /// <inheritdoc cref="InvalidDatabaseVersionException"/>
        public InvalidDatabaseVersionException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when a column is not supported by the Dotmim.Sync framework.
    /// </summary>
    public class UnsupportedColumnTypeException : Exception
    {
        private new const string Message = "In table {0}, the Column {1} of type {2} from provider {3} is not currently supported.";

        /// <inheritdoc cref="UnsupportedColumnTypeException"/>
        public UnsupportedColumnTypeException(string tableName, string columnName, string columnType, string provider)
            : base(string.Format(Message, tableName, columnName, columnType, provider)) { }

        /// <inheritdoc cref="UnsupportedColumnTypeException"/>
        public UnsupportedColumnTypeException()
        {
        }

        /// <inheritdoc cref="UnsupportedColumnTypeException"/>
        public UnsupportedColumnTypeException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when a column name is not supported by the Dotmim.Sync framework.
    /// </summary>
    public class UnsupportedColumnNameException : Exception
    {
        private new const string Message = "In table {0} (Provider {3}), the Column name {1} of type {2} is not allowed. Please consider to change the column name.";

        /// <inheritdoc cref="UnsupportedColumnNameException"/>
        public UnsupportedColumnNameException(string tableName, string columnName, string columnType, string provider)
            : base(string.Format(Message, tableName, columnName, columnType, provider))
        { }

        /// <inheritdoc cref="UnsupportedColumnNameException"/>
        public UnsupportedColumnNameException()
        {
        }

        /// <inheritdoc cref="UnsupportedColumnNameException"/>
        public UnsupportedColumnNameException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when a column name is not supported by the Dotmim.Sync framework for a primary key.
    /// </summary>
    public class UnsupportedPrimaryKeyColumnNameException : Exception
    {
        private new const string Message = "In table {0} (Provider {3}), the Column name {1} of type {2} is not allowed as a primary key. Please consider to change the column name or choose another primary key for your table.";

        /// <inheritdoc cref="UnsupportedPrimaryKeyColumnNameException"/>
        public UnsupportedPrimaryKeyColumnNameException(string tableName, string columnName, string columnType, string provider)
            : base(string.Format(Message, tableName, columnName, columnType, provider)) { }

        /// <inheritdoc cref="UnsupportedPrimaryKeyColumnNameException"/>
        public UnsupportedPrimaryKeyColumnNameException()
        {
        }

        /// <inheritdoc cref="UnsupportedPrimaryKeyColumnNameException"/>
        public UnsupportedPrimaryKeyColumnNameException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when a provider not supported as a server provider is used with a RemoteOrchestrator.
    /// </summary>
    public class UnsupportedServerProviderException : Exception
    {
        private new const string Message = "The provider {0} can not be used as a server provider";

        /// <inheritdoc cref="UnsupportedServerProviderException"/>
        public UnsupportedServerProviderException(string provider)
            : base(string.Format(Message, provider)) { }

        /// <inheritdoc cref="UnsupportedServerProviderException"/>
        public UnsupportedServerProviderException()
        {
        }

        /// <inheritdoc cref="UnsupportedServerProviderException"/>
        public UnsupportedServerProviderException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when sync metadatas are out of date.
    /// </summary>
    public class OutOfDateException : Exception
    {
        private new const string Message = "Client database is out of date. Last client sync timestamp:{0}. Last server cleanup metadata:{1} Try to make a Reinitialize sync.";

        /// <inheritdoc cref="OutOfDateException"/>
        public OutOfDateException(long? timestampLimit, long? serverLastCleanTimestamp)
            : base(string.Format(Message, timestampLimit, serverLastCleanTimestamp)) { }

        /// <inheritdoc cref="OutOfDateException"/>
        public OutOfDateException()
        {
        }

        /// <inheritdoc cref="OutOfDateException"/>
        public OutOfDateException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Http empty response exception.
    /// </summary>
    public class HttpEmptyResponseContentException : Exception
    {
        private new const string Message = "The reponse has an empty body.";

        /// <inheritdoc cref="HttpEmptyResponseContentException"/>
        public HttpEmptyResponseContentException()
            : base(Message) { }

        /// <inheritdoc cref="HttpEmptyResponseContentException"/>
        public HttpEmptyResponseContentException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="HttpEmptyResponseContentException"/>
        public HttpEmptyResponseContentException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a header is missing in the http request.
    /// </summary>
    public class HttpHeaderMissingException : Exception
    {
        private new const string Message = "Header {0} is missing.";

        /// <inheritdoc cref="HttpHeaderMissingException"/>
        public HttpHeaderMissingException(string header)
            : base(string.Format(Message, header)) { }

        /// <inheritdoc cref="HttpHeaderMissingException"/>
        public HttpHeaderMissingException()
        {
        }

        /// <inheritdoc cref="HttpHeaderMissingException"/>
        public HttpHeaderMissingException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a cache is not set on the server.
    /// </summary>
    public class HttpCacheNotConfiguredException : Exception
    {
        private new const string Message = "Cache is not configured! Please add memory cache (distributed or not). See https://docs.microsoft.com/en-us/aspnet/core/performance/caching/response?view=aspnetcore-2.2).";

        /// <inheritdoc cref="HttpCacheNotConfiguredException"/>
        public HttpCacheNotConfiguredException()
            : base(Message) { }

        /// <inheritdoc cref="HttpCacheNotConfiguredException"/>
        public HttpCacheNotConfiguredException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="HttpCacheNotConfiguredException"/>
        public HttpCacheNotConfiguredException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a Serializer is not available on the server side.
    /// </summary>
    public class HttpSerializerNotConfiguredException : Exception
    {
        private new const string Message = "Unexpected value for serializer. Available serializers on the server: {0}";
        private const string MessageEmpty = "Unexpected value for serializer. Server has not any serializer registered";

        /// <inheritdoc cref="HttpSerializerNotConfiguredException"/>
        public HttpSerializerNotConfiguredException(IEnumerable<string> serializers)
            : base(
                serializers.Any() ?
                    string.Format(Message, string.Join(".", serializers))
                    : MessageEmpty)
        { }

        /// <inheritdoc cref="HttpSerializerNotConfiguredException"/>
        public HttpSerializerNotConfiguredException()
        {
        }

        /// <inheritdoc cref="HttpSerializerNotConfiguredException"/>
        public HttpSerializerNotConfiguredException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when a Converter is not available.
    /// </summary>
    public class HttpConverterNotConfiguredException : Exception
    {
        private new const string Message = "Unexpected value for converter. Available converters on the server: {0}";
        private const string MessageEmpty = "Unexpected value for converter. Server has not any converter registered";

        /// <inheritdoc cref="HttpConverterNotConfiguredException"/>
        public HttpConverterNotConfiguredException(IEnumerable<string> converters)
            : base(
                converters.Any() ?
                    string.Format(Message, string.Join(".", converters))
                    : MessageEmpty)
        { }

        /// <inheritdoc cref="HttpConverterNotConfiguredException"/>
        public HttpConverterNotConfiguredException()
        {
        }

        /// <inheritdoc cref="HttpConverterNotConfiguredException"/>
        public HttpConverterNotConfiguredException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when a parameter has been already added in a filter parameter list.
    /// </summary>
    public class HttpScopeNameInvalidException : Exception
    {
        private new const string Message = "The scope {0} does not exist on the server side. Please provider a correct scope name";

        /// <inheritdoc cref="HttpScopeNameInvalidException"/>
        public HttpScopeNameInvalidException(string scopeName)
            : base(string.Format(Message, scopeName)) { }

        /// <inheritdoc cref="HttpScopeNameInvalidException"/>
        public HttpScopeNameInvalidException()
        {
        }

        /// <inheritdoc cref="HttpScopeNameInvalidException"/>
        public HttpScopeNameInvalidException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a parameter has been already added in a filter parameter list.
    /// </summary>
    public class HttpScopeNameFromClientIsInvalidException : Exception
    {
        private new const string Message = "Scope name received from client {0} is different from the scope name specified in the web server agent {1}";

        /// <inheritdoc cref="HttpScopeNameFromClientIsInvalidException"/>
        public HttpScopeNameFromClientIsInvalidException(string scopeNameClientReceived, string scopeNameServerDeclared)
            : base(string.Format(Message, scopeNameClientReceived, scopeNameServerDeclared)) { }

        /// <inheritdoc cref="HttpScopeNameFromClientIsInvalidException"/>
        public HttpScopeNameFromClientIsInvalidException()
        {
        }

        /// <inheritdoc cref="HttpScopeNameFromClientIsInvalidException"/>
        public HttpScopeNameFromClientIsInvalidException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when a session is lost during a sync session.
    /// </summary>
    public class HttpSessionLostException : Exception
    {
        private new const string Message = "Session loss: No batchPartInfo could found for the current sessionId {0}. It seems the session was lost. Please try again.";

        /// <inheritdoc cref="HttpSessionLostException"/>
        public HttpSessionLostException(string sessionId)
            : base(string.Format(Message, sessionId)) { }

        /// <inheritdoc cref="HttpSessionLostException"/>
        public HttpSessionLostException()
        {
        }

        /// <inheritdoc cref="HttpSessionLostException"/>
        public HttpSessionLostException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a parameter has been already added in a filter parameter list.
    /// </summary>
    public class FilterParameterAlreadyExistsException : Exception
    {
        private new const string Message = "The parameter {0} has been already added for the {1} changes stored procedure";

        /// <inheritdoc cref="FilterParameterAlreadyExistsException"/>
        public FilterParameterAlreadyExistsException(string parameterName, string tableName)
            : base(string.Format(Message, parameterName, tableName)) { }

        /// <inheritdoc cref="FilterParameterAlreadyExistsException"/>
        public FilterParameterAlreadyExistsException()
        {
        }

        /// <inheritdoc cref="FilterParameterAlreadyExistsException"/>
        public FilterParameterAlreadyExistsException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when a filter already exists for a named table.
    /// </summary>
    public class FilterAlreadyExistsException : Exception
    {
        private new const string Message = "The filter for the {0} changes stored procedure already exists";

        /// <inheritdoc cref="FilterAlreadyExistsException"/>
        public FilterAlreadyExistsException(string tableName)
            : base(string.Format(Message, tableName)) { }

        /// <inheritdoc cref="FilterAlreadyExistsException"/>
        public FilterAlreadyExistsException()
        {
        }

        /// <inheritdoc cref="FilterAlreadyExistsException"/>
        public FilterAlreadyExistsException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a filter column used as a filter for a tracking table, has not been added to the column parameters list.
    /// </summary>
    public class FilterTrackingWhereException : Exception
    {
        private new const string Message = "The column {0} does not exist in the columns parameters list, so can't be add as a where filter clause to the tracking table";

        /// <inheritdoc cref="FilterTrackingWhereException"/>
        public FilterTrackingWhereException(string columName)
            : base(string.Format(Message, columName)) { }

        /// <inheritdoc cref="FilterTrackingWhereException"/>
        public FilterTrackingWhereException()
        {
        }

        /// <inheritdoc cref="FilterTrackingWhereException"/>
        public FilterTrackingWhereException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a filter column used as a filter for a tracking table, but not exists.
    /// </summary>
    public class FilterParamColumnNotExistsException : Exception
    {
        private new const string Message = "The parameter {0} does not exist as a column in the table {1}";

        /// <inheritdoc cref="FilterParamColumnNotExistsException"/>
        public FilterParamColumnNotExistsException(string columName, string tableName)
            : base(string.Format(Message, columName, tableName)) { }

        /// <inheritdoc cref="FilterParamColumnNotExistsException"/>
        public FilterParamColumnNotExistsException()
        {
        }

        /// <inheritdoc cref="FilterParamColumnNotExistsException"/>
        public FilterParamColumnNotExistsException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Occurs when a filter column used as a filter for a tracking table, but not exists.
    /// </summary>
    public class FilterParamTableNotExistsException : Exception
    {
        private new const string Message = "The table {0} does not exist";

        /// <inheritdoc cref="FilterParamTableNotExistsException"/>
        public FilterParamTableNotExistsException(string tableName)
            : base(string.Format(Message, tableName)) { }

        /// <inheritdoc cref="FilterParamTableNotExistsException"/>
        public FilterParamTableNotExistsException()
        {
        }

        /// <inheritdoc cref="FilterParamTableNotExistsException"/>
        public FilterParamTableNotExistsException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a parameter has been already added to the parameter collection.
    /// </summary>
    public class SyncParameterAlreadyExistsException : Exception
    {
        private new const string Message = "The parameter {0} already exists in the parameter list.";

        /// <inheritdoc cref="SyncParameterAlreadyExistsException"/>
        public SyncParameterAlreadyExistsException(string parameterName)
            : base(string.Format(Message, parameterName)) { }

        /// <inheritdoc cref="SyncParameterAlreadyExistsException"/>
        public SyncParameterAlreadyExistsException()
        {
        }

        /// <inheritdoc cref="SyncParameterAlreadyExistsException"/>
        public SyncParameterAlreadyExistsException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when trying to apply a snapshot that does not exists.
    /// </summary>
    public class SnapshotNotExistsException : Exception
    {
        private new const string Message = "The snapshot {0} does not exists.";

        /// <inheritdoc cref="SnapshotNotExistsException"/>
        public SnapshotNotExistsException(string directoryName)
            : base(string.Format(Message, directoryName)) { }

        /// <inheritdoc cref="SnapshotNotExistsException"/>
        public SnapshotNotExistsException()
        {
        }

        /// <inheritdoc cref="SnapshotNotExistsException"/>
        public SnapshotNotExistsException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when trying to create a snapshot but no directory and size have been set in the options.
    /// </summary>
    public class SnapshotMissingMandatariesOptionsException : Exception
    {
        private new const string Message = "To be able to create a snapshot, you need to precise SnapshotsDirectory and BatchSize in the SyncOptions from the RemoteOrchestrator";

        /// <inheritdoc cref="SnapshotMissingMandatariesOptionsException"/>
        public SnapshotMissingMandatariesOptionsException()
            : base(Message) { }

        /// <inheritdoc cref="SnapshotMissingMandatariesOptionsException"/>
        public SnapshotMissingMandatariesOptionsException(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="SnapshotMissingMandatariesOptionsException"/>
        public SnapshotMissingMandatariesOptionsException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when options references are not the same.
    /// </summary>
    public class OptionsReferencesAreNotSameExecption : Exception
    {
        private new const string Message = "Remote orchestrator options instance is different from Local orchestrator options instance. Please use the same instance.";

        /// <inheritdoc cref="OptionsReferencesAreNotSameExecption"/>
        public OptionsReferencesAreNotSameExecption()
            : base(Message) { }

        /// <inheritdoc cref="OptionsReferencesAreNotSameExecption"/>
        public OptionsReferencesAreNotSameExecption(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="OptionsReferencesAreNotSameExecption"/>
        public OptionsReferencesAreNotSameExecption(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when setup references are not the same.
    /// </summary>
    public class SetupReferencesAreNotSameExecption : Exception
    {
        private new const string Message = "Remote orchestrator setup instance is different from Local orchestrator setup instance. Please use the same instance.";

        /// <inheritdoc cref="SetupReferencesAreNotSameExecption"/>
        public SetupReferencesAreNotSameExecption()
            : base(Message) { }

        /// <inheritdoc cref="SetupReferencesAreNotSameExecption"/>
        public SetupReferencesAreNotSameExecption(string message)
            : base(message)
        {
        }

        /// <inheritdoc cref="SetupReferencesAreNotSameExecption"/>
        public SetupReferencesAreNotSameExecption(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Occurs when a hash from client or server is different from the hash recalculated from server or client.
    /// </summary>
    public class SyncHashException : Exception
    {
        private new const string Message = "The batch file is corrupted. Hash is not valid ({0} compared to {1}";

        /// <inheritdoc cref="SyncHashException"/>
        public SyncHashException(string hash1, string hash2)
            : base(string.Format(Message, hash1, hash2)) { }

        /// <inheritdoc cref="SyncHashException"/>
        public SyncHashException()
        {
        }

        /// <inheritdoc cref="SyncHashException"/>
        public SyncHashException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// Apply changes exception.
    /// </summary>
    public class ApplyChangesException : Exception
    {
        private new const string Message = "Error on table [{0}]: {1}. Row:{2}. ApplyType:{3}";

        /// <inheritdoc cref="ApplyChangesException"/>
        public ApplyChangesException(SyncRow errorRow, SyncTable schemaChangesTable, SyncRowState rowState, Exception innerException)
            : base(string.Format(Message, schemaChangesTable.GetFullName(), innerException.Message, errorRow, rowState), innerException) { }

        /// <inheritdoc cref="ApplyChangesException"/>
        public ApplyChangesException()
        {
        }

        /// <inheritdoc cref="ApplyChangesException"/>
        public ApplyChangesException(string message)
            : base(message)
        {
        }
    }
}