﻿using EFCore.BulkExtensions.Helpers;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters.PostgreSql;

/// <inheritdoc/>
public class PostgreSqlAdapter : ISqlOperationsAdapter
{
    /// <inheritdoc/>
    #region Methods
    // Insert
    public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }
    
    /// <inheritdoc/>
    public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken)
    {
        await InsertAsync(context, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }
    
    /// <inheritdoc/>
    protected static async Task InsertAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
    {
        NpgsqlConnection? connection = (NpgsqlConnection?)SqlAdaptersMapping.DbServer(context).DbConnection;
        bool closeConnectionInternally = false;
        if (connection == null)
        {
            (connection, closeConnectionInternally) = isAsync ? await OpenAndGetNpgsqlConnectionAsync(context, cancellationToken).ConfigureAwait(false)
                                                              : OpenAndGetNpgsqlConnection(context);
        }

        try
        {
            var operationType = tableInfo.InsertToTempTable ? OperationType.InsertOrUpdate : OperationType.Insert;
            string sqlCopy = SqlQueryBuilderPostgreSql.InsertIntoTable(tableInfo, operationType);

            using var writer = isAsync ? await connection.BeginBinaryImportAsync(sqlCopy, cancellationToken).ConfigureAwait(false)
                                       : connection.BeginBinaryImport(sqlCopy);

            var uniqueColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList().FirstOrDefault();

            var doKeepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions == SqlBulkCopyOptions.KeepIdentity;
            var propertiesColumnDict = ((tableInfo.InsertToTempTable || doKeepIdentity) && tableInfo.IdentityColumnName == uniqueColumnName)
                ? tableInfo.PropertyColumnNamesDict
                : tableInfo.PropertyColumnNamesDict.Where(a => a.Value != tableInfo.IdentityColumnName);

            var propertiesNames = propertiesColumnDict.Select(a => a.Key).ToList();
            var entitiesCopiedCount = 0;
            foreach (var entity in entities)
            {
                if (isAsync)
                {
                    await writer.StartRowAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    writer.StartRow();
                }

                foreach (var propertyName in propertiesNames)
                {
                    if (operationType == OperationType.Insert
                        && tableInfo.DefaultValueProperties.Contains(propertyName) 
                        && !tableInfo.PrimaryKeysPropertyColumnNameDict.ContainsKey(propertyName))
                    {
                        continue;
                    }

                    var propertyValue = GetPropertyValue(tableInfo, propertyName, entity);
                    var propertyColumnName = tableInfo.PropertyColumnNamesDict.ContainsKey(propertyName) ? tableInfo.PropertyColumnNamesDict[propertyName] : string.Empty;
                    var columnType = tableInfo.ColumnNamesTypesDict[propertyColumnName];

                    // string is 'text' which works fine
                    if (columnType.StartsWith("character")) // when MaxLength is defined: 'character(1)' or 'character varying'
                        columnType = "character"; // 'character' is like 'string'
                    else if (columnType.StartsWith("varchar"))
                        columnType = "varchar";
                    else if (columnType.StartsWith("numeric"))
                        columnType = "numeric";

                    var convertibleDict = tableInfo.ConvertibleColumnConverterDict;
                    if (convertibleDict.TryGetValue(propertyColumnName, out var converter))
                    {
                        if (propertyValue != null)
                        {
                            if (converter.ModelClrType.IsEnum)
                            {
                                var clrType = converter.ProviderClrType;
                                if (clrType == typeof(byte)) // columnType == "smallint"
                                    propertyValue = (byte)propertyValue;
                                if (clrType == typeof(short))
                                    propertyValue = (short)propertyValue;
                                if (clrType == typeof(int))
                                    propertyValue = (int)propertyValue;
                                if (clrType == typeof(long))
                                    propertyValue = (long)propertyValue;
                                if (clrType == typeof(string))
                                    propertyValue = propertyValue.ToString();
                            }
                            else
                            {
                                propertyValue = converter.ConvertToProvider.Invoke(propertyValue);
                            }
                        }
                    }

                    if (isAsync)
                    {
                        await writer.WriteAsync(propertyValue, columnType, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        writer.Write(propertyValue, columnType);
                    }
                }
                entitiesCopiedCount++;
                if (progress != null && entitiesCopiedCount % tableInfo.BulkConfig.NotifyAfter == 0)
                {
                    progress?.Invoke(ProgressHelper.GetProgress(entities.Count, entitiesCopiedCount));
                }
            }
            if (isAsync)
            {
                await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                writer.Complete();
            }
        }
        finally
        {
            if (closeConnectionInternally)
            {
                if (isAsync)
                {
                    await connection.CloseAsync().ConfigureAwait(false);
                }
                else
                {
                    connection.Close();
                }
            }
        }
    }

    static object? GetPropertyValue<T>(TableInfo tableInfo, string propertyName, T entity)
    {
        if (!tableInfo.FastPropertyDict.ContainsKey(propertyName.Replace('.', '_')) || entity is null)
        {
            return null;
        }

        object? propertyValue = entity;
        string fullPropertyName = string.Empty;
        foreach (var entry in propertyName.AsSpan().Split("."))
        {
            if (propertyValue == null)
            {
                return null;
            }

            if (fullPropertyName.Length > 0)
            {
                fullPropertyName += $"_{entry.Token}";
            }
            else
            {
                fullPropertyName = new string(entry.Token);
            }
            
            propertyValue = tableInfo.FastPropertyDict[fullPropertyName].Get(propertyValue);
        }

        return propertyValue;
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

    /// <inheritdoc/>
    protected async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        if (tableInfo.BulkConfig.CustomSourceTableName == null)
        {
            tableInfo.InsertToTempTable = true;

            var sqlCreateTableCopy = SqlQueryBuilderPostgreSql.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, false); //  tableInfo.BulkConfig.UseTempDB
            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlCreateTableCopy);
            }
        }

        bool hasUniqueConstrain = await CheckHasExplicitUniqueConstrainAsync(context, tableInfo, isAsync,  cancellationToken).ConfigureAwait(false);
        if (hasUniqueConstrain == false)
        {
            if (tableInfo.EntityPKPropertyColumnNameDict == tableInfo.PrimaryKeysPropertyColumnNameDict)
            {
                hasUniqueConstrain = true; // ExplicitUniqueConstrain not required for PK
            }
        }
        bool doDropUniqueConstrain = false;

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

            if (!hasUniqueConstrain)
            {
                string createUniqueIndex = SqlQueryBuilderPostgreSql.CreateUniqueIndex(tableInfo);
                string createUniqueConstrain = SqlQueryBuilderPostgreSql.CreateUniqueConstrain(tableInfo);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(createUniqueIndex, cancellationToken).ConfigureAwait(false);
                    await context.Database.ExecuteSqlRawAsync(createUniqueConstrain, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(createUniqueIndex);
                    context.Database.ExecuteSqlRaw(createUniqueConstrain);
                }
                doDropUniqueConstrain = true;
            }

            var sqlMergeTable = SqlQueryBuilderPostgreSql.MergeTable<T>(tableInfo, operationType);
            if (operationType != OperationType.Read && (!tableInfo.BulkConfig.SetOutputIdentity || operationType == OperationType.Delete))
            {
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlMergeTable, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlMergeTable);
                }
            }
            else
            {
                var sqlMergeTableOutput = sqlMergeTable.TrimEnd(';'); // when ends with ';' Test Atypical - OwnedTypes throws from LoadOutputEntities exception: postgresql '42601: syntax error at or near ";"
                List<T> outputEntities = tableInfo.LoadOutputEntities<T>(context, type, sqlMergeTableOutput);
                tableInfo.UpdateReadEntities(entities, outputEntities, context);
            }
        }
        finally
        {
            try
            {
                if (doDropUniqueConstrain)
                {
                    string dropUniqueConstrain = SqlQueryBuilderPostgreSql.DropUniqueConstrain(tableInfo);
                    if (isAsync)
                    {
                        await context.Database.ExecuteSqlRawAsync(dropUniqueConstrain, cancellationToken)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        context.Database.ExecuteSqlRaw(dropUniqueConstrain);
                    }
                }

                if (!tableInfo.BulkConfig.UseTempDB)
                {
                    if (tableInfo.BulkConfig.CustomSourceTableName == null)
                    {
                        var sqlDropTable = SqlQueryBuilderPostgreSql.DropTable(tableInfo.FullTempTableName);
                        if (isAsync)
                        {
                            await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken)
                                .ConfigureAwait(false);
                        }
                        else
                        {
                            context.Database.ExecuteSqlRaw(sqlDropTable);
                        }
                    }
                }
            }
            catch (PostgresException ex) when (ex.SqlState == "25P02")
            {
                // ignore "current transaction is aborted" exception as it hides the real exception that caused it
            }
        }
    }

    /// <inheritdoc/>
    public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
        => ReadAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
        =>  await ReadAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    protected async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
        =>  await MergeAsync(context, type, entities, tableInfo, OperationType.Read, progress, isAsync, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public void Truncate(DbContext context, TableInfo tableInfo)
    {
        var sqlTruncateTable = SqlQueryBuilderPostgreSql.TruncateTable(tableInfo.FullTableName);
        context.Database.ExecuteSqlRaw(sqlTruncateTable);
    }

    /// <inheritdoc/>
    public async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        var sqlTruncateTable = SqlQueryBuilderPostgreSql.TruncateTable(tableInfo.FullTableName);
        await context.Database.ExecuteSqlRawAsync(sqlTruncateTable, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region Connection
    internal static async Task<(NpgsqlConnection, bool)> OpenAndGetNpgsqlConnectionAsync(DbContext context, CancellationToken cancellationToken)
    {
        bool closeConnectionInternally = false;
        var npgsqlConnection = (NpgsqlConnection)context.Database.GetDbConnection();
        if (npgsqlConnection.State != ConnectionState.Open)
        {
            await npgsqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            closeConnectionInternally = true;
        }
        return (npgsqlConnection, closeConnectionInternally);

        //await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        //return (NpgsqlConnection)context.Database.GetDbConnection();
    }

    internal static (NpgsqlConnection, bool) OpenAndGetNpgsqlConnection(DbContext context)
    {
        bool closeConnectionInternally = false;
        var npgsqlConnection = (NpgsqlConnection)context.Database.GetDbConnection();
        if (npgsqlConnection.State != ConnectionState.Open)
        {
            npgsqlConnection.Open();
            closeConnectionInternally = true;
        }
        return (npgsqlConnection, closeConnectionInternally);

        //context.Database.OpenConnection();
        //return (NpgsqlConnection)context.Database.GetDbConnection();

    }
    #endregion

    internal static async Task<bool> CheckHasExplicitUniqueConstrainAsync(DbContext context, TableInfo tableInfo, bool isAsync, CancellationToken cancellationToken)
    {
        string countUniqueConstrain = SqlQueryBuilderPostgreSql.CountUniqueConstrain(tableInfo);

        bool hasUniqueConstrain = false;
        using (var command = context.Database.GetDbConnection().CreateCommand())
        {
            //command.CommandText = @"SELECT COUNT(*) FROM ""Item""";
            //var count = command.ExecuteScalar();

            command.CommandText = countUniqueConstrain;
            context.Database.OpenConnection();

            if (isAsync)
            {
                using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (reader.HasRows)
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        hasUniqueConstrain = (long)reader[0] == 1;
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
                        hasUniqueConstrain = (long)reader[0] == 1;
                    }
                }
            }
        }
        return hasUniqueConstrain;
    }
}
