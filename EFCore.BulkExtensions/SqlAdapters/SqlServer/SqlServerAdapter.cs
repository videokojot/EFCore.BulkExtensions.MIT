﻿using EFCore.BulkExtensions.Helpers;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

/// <inheritdoc/>
public sealed class SqlOperationsServerAdapter: ISqlOperationsAdapter
{
    internal static string CreateTableCopy(string existingTableName, string newTableName, TableInfo tableInfo, bool isOutputTable = false)
    {
        // TODO: (optionaly) if CalculateStats = True but SetOutputIdentity = False then Columns could be ommited from Create and from MergeOutput
        List<string> columnsNames = (isOutputTable ? tableInfo.OutputPropertyColumnNamesDict : tableInfo.PropertyColumnNamesDict).Values.ToList();
        if (tableInfo.TimeStampColumnName != null)
        {
            columnsNames.Remove(tableInfo.TimeStampColumnName);
        }
        
        string statsColumn = (tableInfo.BulkConfig.OutputTableHasSqlActionColumn && isOutputTable) ? $", CAST('' AS char(1)) AS [{tableInfo.SqlActionIUD}] " : "";
        string indexMappingColumn = (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn) ? $", CAST(-1 AS int) AS [{tableInfo.OriginalIndexColumnName}] " : "";
        var q = $"SELECT TOP 0 {SqlQueryBuilder.GetCommaSeparatedColumns(columnsNames, "T")} " + statsColumn + indexMappingColumn +
                $"INTO {newTableName} FROM {existingTableName} AS T " +
                $"LEFT JOIN {existingTableName} AS Source ON 1 = 0;"; // removes Identity constraint
        return q;
    }
    
    #region Methods

    /// <inheritdoc/>
    public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken)
    {
        await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }
    
    private static string CheckTableExist(string fullTableName, bool isTempTable)
    {
        string q;
        if (isTempTable)
        {
            q = $"IF OBJECT_ID ('tempdb..[#{fullTableName.Split('#')[1]}', 'U') IS NOT NULL SELECT 1 AS res ELSE SELECT 0 AS res;";
        }
        else
        {
            q = $"IF OBJECT_ID ('{fullTableName}', 'U') IS NOT NULL SELECT 1 AS res ELSE SELECT 0 AS res;";
        }
        return q;
    }
    
    private static string TruncateTable(string tableName) => $"TRUNCATE TABLE {tableName};";

    private static async Task<bool> CheckTableExistAsync(DbContext context, TableInfo tableInfo, bool isAsync, CancellationToken cancellationToken)
    {
        if (isAsync)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            context.Database.OpenConnection();
        }

        bool tableExist = false;
        try
        {
            var sqlConnection = context.Database.GetDbConnection();
            var currentTransaction = context.Database.CurrentTransaction;

            using var command = sqlConnection.CreateCommand();
            if (currentTransaction != null)
                command.Transaction = currentTransaction.GetDbTransaction();
            command.CommandText = CheckTableExist(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);

            if (isAsync)
            {
                using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (reader.HasRows)
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        tableExist = (int)reader[0] == 1;
                    }
                }
            }
            else
            {
                using var reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        tableExist = (int)reader[0] == 1;
                    }
                }
            }
        }
        finally
        {
            if (isAsync)
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
            else
            {
                context.Database.CloseConnection();
            }
        }
        return tableExist;
    }

    
    private static async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
    {
        tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities);
        if (isAsync)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            context.Database.OpenConnection();
        }
        var connection = context.GetUnderlyingConnection(tableInfo.BulkConfig);

        try
        {
            var transaction = context.Database.CurrentTransaction;

            using var sqlBulkCopy = GetSqlBulkCopy((SqlConnection)connection, transaction, tableInfo.BulkConfig);
            bool setColumnMapping = false;
            SetSqlBulkCopyConfig(sqlBulkCopy, tableInfo, entities, setColumnMapping, progress);
            try
            {
                var dataTable = GetDataTable(context, type, entities, sqlBulkCopy, tableInfo);
                if (isAsync)
                {
                    await sqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    sqlBulkCopy.WriteToServer(dataTable);
                }
            }
            catch (InvalidOperationException ex)
            {
                if (ex.Message.Contains(BulkExceptionMessage.ColumnMappingNotMatch))
                {
                    bool tableExist = isAsync ? await CheckTableExistAsync(context, tableInfo, isAsync: true, cancellationToken).ConfigureAwait(false)
                                                    : CheckTableExistAsync(context, tableInfo, isAsync: false, cancellationToken).GetAwaiter().GetResult();

                    if (!tableExist)
                    {
                        var sqlCreateTableCopy = CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
                        var sqlDropTable = SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);

                        if (isAsync)
                        {
                            await context.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
                            await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            context.Database.ExecuteSqlRaw(sqlCreateTableCopy);
                            context.Database.ExecuteSqlRaw(sqlDropTable);
                        }
                    }
                }
                throw;
            }
        }
        finally
        {
            if (isAsync)
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
            else
            {
                context.Database.CloseConnection();
            }
        }
        
        if (!tableInfo.CreatedOutputTable)
        {
            tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities, reset: true);
        }
    }

    /// <inheritdoc/>
    public void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress) where T : class
    {
        MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Generates SQL query to alter table columns to nullables
    /// </summary>
    private static string AlterTableColumnsToNullable(string tableName, TableInfo tableInfo)
    {
        string q = "";
        foreach (var column in tableInfo.ColumnNamesTypesDict)
        {
            string columnName = column.Key;
            string columnType = column.Value;
            if (columnName == tableInfo.TimeStampColumnName)
                columnType = TableInfo.TimeStampOutColumnType;
            q += $"ALTER TABLE {tableName} ALTER COLUMN [{columnName}] {columnType}; ";
        }
        return q;
    }
    
    private async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        var entityPropertyWithDefaultValue = entities.GetPropertiesWithDefaultValue(type, tableInfo);

        if (tableInfo.BulkConfig.CustomSourceTableName == null)
        {
            tableInfo.InsertToTempTable = true;

            var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

            if (dropTempTableIfExists)
            {
                var sqlDropTable = SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlDropTable);
                }
            }

            var sqlCreateTableCopy = CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
            
            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlCreateTableCopy);
            }

            if (tableInfo.TimeStampColumnName != null)
            {
                var sqlAddColumn = AddColumn(tableInfo.FullTempTableName, tableInfo.TimeStampColumnName, TableInfo.TimeStampOutColumnType);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlAddColumn, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlAddColumn);
                }
            }
        }

        if (tableInfo.CreatedOutputTable)
        {
            var sqlCreateOutputTableCopy = CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true);
            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlCreateOutputTableCopy, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlCreateOutputTableCopy);
            }

            if (tableInfo.TimeStampColumnName != null)
            {
                var sqlAddColumn = AddColumn(tableInfo.FullTempOutputTableName, tableInfo.TimeStampColumnName, TableInfo.TimeStampOutColumnType);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlAddColumn, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlAddColumn);
                }
            }

            if (operationType == OperationType.InsertOrUpdateOrDelete)
            {
                // Output returns all changes including Deleted rows with all NULL values, so if TempOutput.Id col not Nullable it breaks
                var sqlAlterTableColumnsToNullable = AlterTableColumnsToNullable(tableInfo.FullTempOutputTableName, tableInfo);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlAlterTableColumnsToNullable, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlAlterTableColumnsToNullable);
                }
            }
        }

        bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
        try
        {
            if (tableInfo.BulkConfig.CustomSourceTableName == null)
            {
                if (isAsync)
                {
                    await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    Insert(context, type, entities, tableInfo, progress);
                }
            }

            if (keepIdentity && tableInfo.HasIdentity)
            {
                var sqlSetIdentityInsertTrue = SetIdentityInsert(tableInfo.FullTableName, true);
                if (isAsync)
                {
                    await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                    await context.Database.ExecuteSqlRawAsync(sqlSetIdentityInsertTrue, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.OpenConnection();
                    context.Database.ExecuteSqlRaw(sqlSetIdentityInsertTrue);
                }
            }

            var (sql, parameters) = SqlQueryBuilder.MergeTable<T>(context, tableInfo, operationType, entityPropertyWithDefaultValue);
            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sql, parameters, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sql, parameters);
            }

            if (tableInfo.CreatedOutputTable)
            {
                if (isAsync)
                {
                    await tableInfo.LoadOutputDataAsync(context, type, entities, tableInfo, isAsync: true, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    tableInfo.LoadOutputDataAsync(context, type, entities, tableInfo, isAsync: false, cancellationToken).GetAwaiter().GetResult();
                }
            }
        }
        finally
        {
            if (!tableInfo.BulkConfig.UseTempDB)
            {
                if (tableInfo.CreatedOutputTable)
                {
                    var sqlDropOutputTable = SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName, tableInfo.BulkConfig.UseTempDB);
                    if (isAsync)
                    {
                        await context.Database.ExecuteSqlRawAsync(sqlDropOutputTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        context.Database.ExecuteSqlRaw(sqlDropOutputTable);
                    }

                }
                if (tableInfo.BulkConfig.CustomSourceTableName == null)
                {
                    var sqlDropTable = SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
                    if (isAsync)
                    {
                        await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        context.Database.ExecuteSqlRaw(sqlDropTable);
                    }
                }
            }

            if (keepIdentity && tableInfo.HasIdentity)
            {
                var sqlSetIdentityInsertFalse = SetIdentityInsert(tableInfo.FullTableName, false);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlSetIdentityInsertFalse, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlSetIdentityInsertFalse);
                }
                context.Database.CloseConnection();
            }
        }
    }

    /// <inheritdoc/>
    public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        ReadAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await ReadAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }

    private async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        Dictionary<string, string> previousPropertyColumnNamesDict = tableInfo.ConfigureBulkReadTableInfo();

        var sqlCreateTableCopy = CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
        if (isAsync)
        {
            await context.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            context.Database.ExecuteSqlRaw(sqlCreateTableCopy);
        }

        try
        {
            if (isAsync)
            {
                await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, cancellationToken).GetAwaiter().GetResult();
            }

            tableInfo.PropertyColumnNamesDict = tableInfo.OutputPropertyColumnNamesDict;

            var sqlSelectJoinTable = SqlQueryBuilder.SelectJoinTable(tableInfo);

            tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict; // TODO Consider refactor and integrate with TimeStampPropertyName, also check for Calculated props.
                                                                                 // Output only PropertisToInclude and for getting Id with SetOutputIdentity
            if (tableInfo.TimeStampPropertyName != null && !tableInfo.PropertyColumnNamesDict.ContainsKey(tableInfo.TimeStampPropertyName) && tableInfo.TimeStampColumnName is not null)
            {
                tableInfo.PropertyColumnNamesDict.Add(tableInfo.TimeStampPropertyName, tableInfo.TimeStampColumnName);
            }

            List<T> existingEntities = tableInfo.LoadOutputEntities<T>(context, type, sqlSelectJoinTable);

            if (tableInfo.BulkConfig.ReplaceReadEntities)
            {
                tableInfo.ReplaceReadEntities(entities, existingEntities);
            }
            else
            {
                tableInfo.UpdateReadEntities(entities, existingEntities, context);
            }

            if (tableInfo.TimeStampPropertyName != null && !tableInfo.PropertyColumnNamesDict.ContainsKey(tableInfo.TimeStampPropertyName))
            {
                tableInfo.PropertyColumnNamesDict.Remove(tableInfo.TimeStampPropertyName);
            }
        }
        finally
        {
            if (!tableInfo.BulkConfig.UseTempDB)
            {
                var sqlDropTable = SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlDropTable);
                }
            }
        }
    }
    
    private static string SetIdentityInsert(string tableName, bool identityInsert)
    {
        string ON_OFF = identityInsert ? "ON" : "OFF";
        var q = $"SET IDENTITY_INSERT {tableName} {ON_OFF};";
        return q;
    }
    
    private static string AddColumn(string fullTableName, string columnName, string columnType) => $"ALTER TABLE {fullTableName} ADD [{columnName}] {columnType};";

    /// <inheritdoc/>
    public void Truncate(DbContext context, TableInfo tableInfo) => context.Database.ExecuteSqlRaw(TruncateTable(tableInfo.FullTableName));

    /// <inheritdoc/>
    public async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken) => await context.Database.ExecuteSqlRawAsync(TruncateTable(tableInfo.FullTableName), cancellationToken).ConfigureAwait(false);

    #endregion

    #region Connection
    private static SqlBulkCopy GetSqlBulkCopy(SqlConnection sqlConnection, IDbContextTransaction? transaction, BulkConfig config)
    {
        var sqlTransaction = transaction == null ? null : (SqlTransaction)transaction.GetUnderlyingTransaction(config);
        var sqlBulkCopy = new SqlBulkCopy(sqlConnection, config.SqlBulkCopyOptions, sqlTransaction);
        if (config.SqlBulkCopyColumnOrderHints != null)
        {
            foreach(var hint in config.SqlBulkCopyColumnOrderHints)
                sqlBulkCopy.ColumnOrderHints.Add(hint);
        }
        return sqlBulkCopy;
    }
  
    private static void SetSqlBulkCopyConfig<T>(SqlBulkCopy sqlBulkCopy, TableInfo tableInfo, IList<T> entities, bool setColumnMapping, Action<decimal>? progress)
    {
        sqlBulkCopy.DestinationTableName = tableInfo.InsertToTempTable ? tableInfo.FullTempTableName : tableInfo.FullTableName;
        sqlBulkCopy.BatchSize = tableInfo.BulkConfig.BatchSize;
        sqlBulkCopy.NotifyAfter = tableInfo.BulkConfig.NotifyAfter ?? tableInfo.BulkConfig.BatchSize;
        sqlBulkCopy.SqlRowsCopied += (_, e) =>
        {
            progress?.Invoke(ProgressHelper.GetProgress(entities.Count, e.RowsCopied)); // round to 4 decimal places
        };
        sqlBulkCopy.BulkCopyTimeout = tableInfo.BulkConfig.BulkCopyTimeout ?? sqlBulkCopy.BulkCopyTimeout;
        sqlBulkCopy.EnableStreaming = tableInfo.BulkConfig.EnableStreaming;

        if (setColumnMapping)
        {
            foreach (var element in tableInfo.PropertyColumnNamesDict)
            {
                sqlBulkCopy.ColumnMappings.Add(element.Key, element.Value);
            }
        }
    }

    #endregion

    #region DataTable
    /// <summary>
    /// Supports <see cref="SqlBulkCopy"/>
    /// </summary>
    internal static DataTable GetDataTable<T>(DbContext context, Type type, IList<T> entities, SqlBulkCopy sqlBulkCopy, TableInfo tableInfo)
    {
        DataTable dataTable = InnerGetDataTable(context, ref type, entities, tableInfo);

        foreach (DataColumn item in dataTable.Columns)  // Add mapping
        {
            sqlBulkCopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
        }
        
        return dataTable;
    }

    /// <summary>
    /// Common logic for two versions of GetDataTable
    /// </summary>
    private static DataTable InnerGetDataTable<T>(DbContext context, ref Type type, IList<T> entities, TableInfo tableInfo)
    {
        var dataTable = new DataTable();
        var columnsDict = new Dictionary<string, object?>();
        var ownedEntitiesMappedProperties = new HashSet<string>();

        var databaseType = SqlAdaptersMapping.GetDatabaseType(context);
        var isSqlServer = databaseType == DbServerType.SQLServer;
        var sqlServerBytesWriter = new SqlServerBytesWriter();

        var objectIdentifier = tableInfo.ObjectIdentifier;
        type = tableInfo.HasAbstractList ? entities[0]!.GetType() : type;
        var entityType = context.Model.FindEntityType(type) ?? throw new ArgumentException($"Unable to determine entity type from given type - {type.Name}");
        var entityTypeProperties = entityType.GetProperties().ToList();
        var entityPropertiesDict = entityTypeProperties.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name) ||
                                                                   (tableInfo.BulkConfig.OperationType != OperationType.Read && a.Name == tableInfo.TimeStampPropertyName))
                                                       .ToDictionary(a => a.Name, a => a);
        var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.TargetEntityType.IsOwned()).ToDictionary(a => a.Name, a => a);
        var entityShadowFkPropertiesDict = entityTypeProperties.Where(a => a.IsShadowProperty() &&
                                                                           a.IsForeignKey() &&
                                                                           a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                                                                     .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);

        var entityShadowFkPropertyColumnNamesDict = entityShadowFkPropertiesDict
            .ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));
        var shadowPropertyColumnNamesDict = entityPropertiesDict
            .Where(a => a.Value.IsShadowProperty()).ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));

        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        var discriminatorColumn = GetDiscriminatorColumn(tableInfo);

        foreach (var property in properties)
        {
            var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                && !tableInfo.BulkConfig.SetOutputIdentity
                && tableInfo.DefaultValueProperties.Contains(property.Name);

            if (entityPropertiesDict.TryGetValue(property.Name, out var propertyEntityType))
            {
                string columnName = propertyEntityType.GetColumnName(objectIdentifier) ?? string.Empty;

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName].ProviderClrType : property.PropertyType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (isSqlServer && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
                {
                    propertyType = typeof(byte[]);
                    tableInfo.HasSpatialType = true;
                    if (tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null || tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null)
                    {
                        throw new InvalidOperationException("OnCompare properties Config can not be set for Entity with Spatial types like 'Geometry'");
                    }
                }
                if (isSqlServer && (propertyType == typeof(HierarchyId) || propertyType.IsSubclassOf(typeof(HierarchyId))))
                {
                    propertyType = typeof(byte[]);
                }

                if (!columnsDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(property.Name, null);
                }
            }
            else if (entityShadowFkPropertiesDict.TryGetValue(property.Name, out var fk))
            {
                entityPropertiesDict.TryGetValue(fk.GetColumnName(objectIdentifier) ?? string.Empty, out var entityProperty);
                if (entityProperty == null) // BulkRead
                    continue;

                var columnName = entityProperty.GetColumnName(objectIdentifier);

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName ?? string.Empty);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName ?? string.Empty].ProviderClrType : entityProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (propertyType == typeof(Geometry) && isSqlServer)
                {
                    propertyType = typeof(byte[]);
                }

                if (propertyType == typeof(HierarchyId) && isSqlServer)
                {
                    propertyType = typeof(byte[]);
                }

                if (columnName is not null && !(columnsDict.ContainsKey(columnName)) && !hasDefaultVauleOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(columnName, null);
                }
            }
            else if (entityNavigationOwnedDict.ContainsKey(property.Name)) // isOWned
            {
                //Type? navOwnedType = type.Assembly.GetType(property.PropertyType.FullName!); // was not used

                var ownedEntityType = context.Model.FindEntityType(property.PropertyType);
                if (ownedEntityType == null)
                {
                    ownedEntityType = context.Model.GetEntityTypes().SingleOrDefault(x => x.ClrType == property.PropertyType && x.Name.StartsWith(entityType.Name + "." + property.Name + "#"));
                }

                var ownedEntityProperties = ownedEntityType?.GetProperties().ToList() ?? new();
                var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                foreach (var ownedEntityProperty in ownedEntityProperties)
                {
                    if (!ownedEntityProperty.IsPrimaryKey())
                    {
                        string? columnName = ownedEntityProperty.GetColumnName(objectIdentifier);
                        if (columnName is not null && tableInfo.PropertyColumnNamesDict.ContainsValue(columnName))
                        {
                            ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                            ownedEntitiesMappedProperties.Add(property.Name + "_" + ownedEntityProperty.Name);
                        }
                    }
                }

                var innerProperties = property.PropertyType.GetProperties();
                if (!tableInfo.LoadOnlyPKColumn)
                {
                    foreach (var innerProperty in innerProperties)
                    {
                        if (ownedEntityPropertyNameColumnNameDict.TryGetValue(innerProperty.Name, out var columnName))
                        {
                            var propertyName = $"{property.Name}_{innerProperty.Name}";

                            if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(propertyName, out var convertor))
                            {
                                var underlyingType = Nullable.GetUnderlyingType(convertor.ProviderClrType) ?? convertor.ProviderClrType;
          
                                dataTable.Columns.Add(columnName, underlyingType);
                            }
                            else
                            {
                                var ownedPropertyType = Nullable.GetUnderlyingType(innerProperty.PropertyType) ?? innerProperty.PropertyType;
                                
                                if (isSqlServer && (ownedPropertyType == typeof(Geometry) || ownedPropertyType.IsSubclassOf(typeof(Geometry))))
                                {
                                    ownedPropertyType = typeof(byte[]);
                                    tableInfo.HasSpatialType = true;
                                    if (tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null || tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null)
                                    {
                                        throw new InvalidOperationException("OnCompare properties Config can not be set for Entity with Spatial types like 'Geometry'");
                                    }
                                }

                                if (isSqlServer && (ownedPropertyType == typeof(HierarchyId) || ownedPropertyType.IsSubclassOf(typeof(HierarchyId))))
                                {
                                    ownedPropertyType = typeof(byte[]);
                                }

                                dataTable.Columns.Add(columnName, ownedPropertyType);
                            }
                            
                            columnsDict.Add(property.Name + "_" + innerProperty.Name, null);
                        }
                    }
                }
            }
        }

        if (tableInfo.BulkConfig.EnableShadowProperties)
        {
            foreach (var shadowProperty in entityPropertiesDict.Values.Where(a => a.IsShadowProperty()))
            {
                string? columnName = shadowProperty.GetColumnName(objectIdentifier);

                // If a model has an entity which has a relationship without an explicity defined FK, the data table will already contain the foreign key shadow property
                if (columnName is not null && dataTable.Columns.Contains(columnName))
                    continue;

                var isConvertible = columnName is not null && tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);

                var propertyType = isConvertible
                    ? tableInfo.ConvertibleColumnConverterDict[columnName!].ProviderClrType
                    : shadowProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (isSqlServer && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
                {
                    propertyType = typeof(byte[]);
                }

                if (isSqlServer && (propertyType == typeof(HierarchyId) || propertyType.IsSubclassOf(typeof(HierarchyId))))
                {
                    propertyType = typeof(byte[]);
                }

                dataTable.Columns.Add(columnName, propertyType);
                columnsDict.Add(shadowProperty.Name, null);
            }
        }

        if (discriminatorColumn != null)
        {
            var discriminatorProperty = entityPropertiesDict[discriminatorColumn];

            dataTable.Columns.Add(discriminatorColumn, discriminatorProperty.ClrType);
            columnsDict.Add(discriminatorColumn, entityType.GetDiscriminatorValue());
        }
        
        bool hasConverterProperties = tableInfo.ConvertiblePropertyColumnDict.Count > 0;

        if (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn)
        {
            dataTable.Columns.Add(tableInfo.OriginalIndexColumnName, typeof(int));
            columnsDict.Add(tableInfo.OriginalIndexColumnName, -1);
        }

        var index = 0;
        foreach (T entity in entities)
        {
            var propertiesToLoad = properties
                .Where(a => !tableInfo.AllNavigationsDictionary.ContainsKey(a.Name)
                            || entityShadowFkPropertiesDict.ContainsKey(a.Name)
                            || tableInfo.OwnedTypesDict.ContainsKey(a.Name)); // omit virtual Navigation (except Owned and ShadowNavig.) since it's Getter can cause unwanted Select-s from Db

            foreach (var property in propertiesToLoad)
            {
                object? propertyValue = tableInfo.FastPropertyDict.ContainsKey(property.Name)
                    ? tableInfo.FastPropertyDict[property.Name].Get(entity!)
                    : null;

                var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                    && !tableInfo.BulkConfig.SetOutputIdentity
                    && tableInfo.DefaultValueProperties.Contains(property.Name);

                if (tableInfo.BulkConfig.DateTime2PrecisionForceRound
                    && isSqlServer
                    && tableInfo.DateTime2PropertiesPrecisionLessThen7Dict.TryGetValue(property.Name, out var precision))
                {
                    DateTime? dateTimePropertyValue = (DateTime?)propertyValue;

                    if (dateTimePropertyValue is not null)
                    {
                        int digitsToRemove = 7 - precision;
                        int powerOf10 = (int)Math.Pow(10, digitsToRemove);

                        long subsecondTicks = dateTimePropertyValue.Value.Ticks % 10000000;
                        long ticksToRound = subsecondTicks + (subsecondTicks % 10 == 0 ? 1 : 0); // if ends with 0 add 1 tick to make sure rounding of value .5_zeros is rounded to Upper like SqlServer is doing, not to Even as Math.Round works
                        int roundedTicks = Convert.ToInt32(Math.Round((decimal)ticksToRound / powerOf10, 0)) * powerOf10;
                        dateTimePropertyValue = dateTimePropertyValue.Value.AddTicks(-subsecondTicks).AddTicks(roundedTicks);

                        propertyValue = dateTimePropertyValue;
                    }
                }

                if (hasConverterProperties && tableInfo.ConvertiblePropertyColumnDict.TryGetValue(property.Name, out var convertibleColumnName))
                {
                    propertyValue = tableInfo.ConvertibleColumnConverterDict[convertibleColumnName].ConvertToProvider.Invoke(propertyValue);
                }

                if (tableInfo.HasSpatialType && propertyValue is Geometry geometryValue)
                {
                    geometryValue.SRID = tableInfo.BulkConfig.SRID;

                    if (tableInfo.PropertyColumnNamesDict.TryGetValue(property.Name, out var spatialColumnName))
                    {
                        sqlServerBytesWriter.IsGeography = tableInfo.ColumnNamesTypesDict[spatialColumnName] == "geography"; // "geography" type is default, otherwise it's "geometry" type
                    }

                    propertyValue = sqlServerBytesWriter.Write(geometryValue);
                }

                if (propertyValue is HierarchyId hierarchyValue && isSqlServer)
                {
                    using MemoryStream memStream = new();
                    using BinaryWriter binWriter = new(memStream);
#if !NET8_0
                    hierarchyValue.Write(binWriter);
#endif
                    propertyValue = memStream.ToArray();
                }

                if (entityPropertiesDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    columnsDict[property.Name] = propertyValue;
                }
                else if (entityShadowFkPropertiesDict.TryGetValue(property.Name, out var foreignKeyShadowProperty))
                {
                    var columnName = entityShadowFkPropertyColumnNamesDict[property.Name] ?? string.Empty;
                    if (!entityPropertiesDict.TryGetValue(columnName, out var entityProperty) || entityProperty is null)
                    {
                        continue; // BulkRead
                    }

                    columnsDict[columnName] = propertyValue == null
                        ? null
                        : foreignKeyShadowProperty.FindFirstPrincipal()?.PropertyInfo?.GetValue(propertyValue); // TODO Check if can be optimized
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                {
                    var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                    foreach (var ownedProperty in ownedProperties)
                    {
                        var columnName = $"{property.Name}_{ownedProperty.Name}";
                        var ownedPropertyValue = propertyValue == null ? null : tableInfo.FastPropertyDict[columnName].Get(propertyValue);

                        if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(columnName, out var converter))
                        {
                            columnsDict[columnName] = ownedPropertyValue == null ? null : converter.ConvertToProvider.Invoke(ownedPropertyValue);
                        }
                        else if(tableInfo.HasSpatialType && ownedPropertyValue is Geometry ownedGeometryValue)
                        {
                            ownedGeometryValue.SRID = tableInfo.BulkConfig.SRID;

                            if (tableInfo.PropertyColumnNamesDict.TryGetValue(property.Name, out var ownedSpatialColumnName))
                            {
                                sqlServerBytesWriter.IsGeography = tableInfo.ColumnNamesTypesDict[ownedSpatialColumnName] == "geography"; // "geography" type is default, otherwise it's "geometry" type
                            }

                            columnsDict[columnName] = sqlServerBytesWriter.Write(ownedGeometryValue);
                        }
                        else
                        {
                            columnsDict[columnName] = ownedPropertyValue;
                        }
                    }
                }
            }

            if (tableInfo.BulkConfig.EnableShadowProperties)
            {
                foreach (var shadowPropertyName in shadowPropertyColumnNamesDict.Keys)
                {
                    var columnName = shadowPropertyColumnNamesDict[shadowPropertyName] ?? string.Empty;

                    var propertyValue = default(object);

                    if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                    {
                        propertyValue = context.Entry(entity!).Property(shadowPropertyName).CurrentValue;
                    }
                    else
                    {
                        propertyValue = tableInfo.BulkConfig.ShadowPropertyValue(entity!, shadowPropertyName);
                    }

                    if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(columnName, out var converter))
                    {
                        propertyValue = converter.ConvertToProvider.Invoke(propertyValue);
                    }

                    columnsDict[shadowPropertyName] = propertyValue;
                }
            }

            if (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn)
            {
                columnsDict[tableInfo.OriginalIndexColumnName] = index;
            }
            
            var record = columnsDict.Values.ToArray();

            dataTable.Rows.Add(record);
            index++;
        }

        return dataTable;
    }

    private static string? GetDiscriminatorColumn(TableInfo tableInfo)
    {
        string? discriminatorColumn = null;
        if (!tableInfo.BulkConfig.EnableShadowProperties && tableInfo.ShadowProperties.Count > 0)
        {
            var stringColumns = tableInfo.ColumnNamesTypesDict.Where(a => a.Value.Contains("char")).Select(a => a.Key).ToList();
            discriminatorColumn = tableInfo.ShadowProperties.Where(a => stringColumns.Contains(a)).ElementAt(0);
        }
        return discriminatorColumn;
    }
    #endregion
}
