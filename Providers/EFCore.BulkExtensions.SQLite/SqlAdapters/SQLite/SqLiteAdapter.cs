using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters.SQLite;

/// <inheritdoc/>
public sealed class SqliteOperationsAdapter : ISqlOperationsAdapter
{
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

    /// <inheritdoc/>
    public static async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
    {
        SqliteConnection? connection = (SqliteConnection?)SqlAdaptersMapping.DbServer(context).DbConnection;

        if (connection == null)
        {
            connection = isAsync
                             ? await OpenAndGetSqliteConnectionAsync(context, cancellationToken).ConfigureAwait(false)
                             : OpenAndGetSqliteConnection(context);
        }

        bool doExplicitCommit = false;

        try
        {
            if (context.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }

            SqliteTransaction? transaction = (SqliteTransaction?)tableInfo.DbTransaction;

            if (transaction == null)
            {
                DbTransaction? dbTransaction = doExplicitCommit
                                        ? connection.BeginTransaction()
                                        : context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);

                transaction = (SqliteTransaction?)dbTransaction;
            }
            else
            {
                doExplicitCommit = false;
            }

            SqliteCommand command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

            type = tableInfo.HasAbstractList ? entities[0]!.GetType() : type;
            int rowsCopied = 0;

            foreach (T item in entities)
            {
                LoadSqliteValues(tableInfo, item, command, context);

                if (isAsync)
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    command.ExecuteNonQuery();
                }

                ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();
            }
        }
        finally
        {
            if (doExplicitCommit)
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
        }
    }

    // Merge
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
    protected static async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
        where T : class
    {
        SqliteConnection connection = isAsync
                                          ? await OpenAndGetSqliteConnectionAsync(context, cancellationToken).ConfigureAwait(false)
                                          : OpenAndGetSqliteConnection(context);
        bool doExplicitCommit = false;

        try
        {
            if (context.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }

            DbTransaction? dbTransaction = doExplicitCommit
                                    ? connection.BeginTransaction()
                                    : context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);
            SqliteTransaction? transaction = (SqliteTransaction?)dbTransaction;

            SqliteCommand command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

            type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
            int rowsCopied = 0;

            foreach (T item in entities)
            {
                LoadSqliteValues(tableInfo, item, command, context);

                if (isAsync)
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    command.ExecuteNonQuery();
                }

                ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
            }

            if (operationType == OperationType.Insert && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null) // For Sqlite Identity can be set by Db only with pure Insert method
            {
                command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();

                object? lastRowIdScalar = isAsync
                                              ? await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)
                                              : command.ExecuteScalar();

                SetIdentityForOutput(entities, tableInfo, lastRowIdScalar);
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();
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
    }

    // Read
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

    /// <inheritdoc/>
    protected static async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        SqliteConnection connection = isAsync
                                          ? await OpenAndGetSqliteConnectionAsync(context, cancellationToken).ConfigureAwait(false)
                                          : OpenAndGetSqliteConnection(context);
        bool doExplicitCommit = false;
        SqliteTransaction? transaction = null;

        try
        {
            if (context.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }

            transaction = doExplicitCommit
                              ? connection.BeginTransaction()
                              : (SqliteTransaction?)context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);

            SqliteCommand command = connection.CreateCommand();
            command.Transaction = transaction;

            // CREATE
            command.CommandText = SqlQueryBuilderSqlite.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName);

            if (isAsync)
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                command.ExecuteNonQuery();
            }

            tableInfo.BulkConfig.OperationType = OperationType.Insert;
            tableInfo.InsertToTempTable = true;
            tableInfo.DbTransaction = transaction;

            // INSERT
            if (isAsync)
            {
                await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, cancellationToken).GetAwaiter().GetResult();
            }

            // JOIN
            List<T> existingEntities;
            string sqlSelectJoinTable = SqlQueryBuilder.SelectJoinTable(tableInfo);
            Expression<Func<DbContext, IQueryable<T>>> expression = tableInfo.GetQueryExpression<T>(sqlSelectJoinTable, false);
            var compiled = EF.CompileQuery(expression); // instead using Compiled queries
            existingEntities = compiled(context).ToList();

            if (tableInfo.BulkConfig.ReplaceReadEntities)
            {
                tableInfo.ReplaceReadEntities(entities, existingEntities);
            }
            else
            {
                tableInfo.UpdateReadEntities(entities, existingEntities, context);
            }

            // DROP
            command.CommandText = SqlQueryBuilderSqlite.DropTable(tableInfo.FullTempTableName);

            if (isAsync)
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                command.ExecuteNonQuery();
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();
            }
        }
        finally
        {
            if (doExplicitCommit)
            {
                if (isAsync)
                {
                    if (transaction is not null)
                    {
                        await transaction.DisposeAsync().ConfigureAwait(false);
                    }

                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    transaction?.Dispose();
                    context.Database.CloseConnection();
                }
            }
        }
    }
    
    private static string DeleteTable(string tableName) => $"DELETE FROM {tableName};VACUUM;";

    /// <inheritdoc/>
    public void Truncate(DbContext context, TableInfo tableInfo) => context.Database.ExecuteSqlRaw(DeleteTable(tableInfo.FullTableName));

    /// <inheritdoc/>
    public async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken) => await context.Database.ExecuteSqlRawAsync(DeleteTable(tableInfo.FullTableName), cancellationToken).ConfigureAwait(false);

    #endregion

    #region Connection

    internal static async Task<SqliteConnection> OpenAndGetSqliteConnectionAsync(DbContext context, CancellationToken cancellationToken)
    {
        await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        SqliteConnection sqliteConnection = (SqliteConnection)context.Database.GetDbConnection();

        return sqliteConnection;
    }

    internal static SqliteConnection OpenAndGetSqliteConnection(DbContext context)
    {
        context.Database.OpenConnection();

        SqliteConnection sqliteConnection = (SqliteConnection)context.Database.GetDbConnection();

        return sqliteConnection;
    }

    #endregion

    #region SqliteData

    internal static SqliteCommand GetSqliteCommand<T>(DbContext context, Type? type, IList<T> entities, TableInfo tableInfo, SqliteConnection connection, SqliteTransaction? transaction)
    {
        SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;

        OperationType operationType = tableInfo.BulkConfig.OperationType;

        switch (operationType)
        {
            case OperationType.Insert:
                command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.Insert);

                break;
            case OperationType.InsertOrUpdate:
                command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);

                break;
            case OperationType.InsertOrUpdateOrDelete:
                throw new NotSupportedException("'BulkInsertOrUpdateDelete' not supported for Sqlite. Sqlite has only UPSERT statement (analog for MERGE WHEN MATCHED) but no functionality for: 'WHEN NOT MATCHED BY SOURCE THEN DELETE'."
                                                + " Another way to achieve this is to BulkRead existing data from DB, split list into sublists and call separately Bulk methods for Insert, Update, Delete.");
            case OperationType.Update:
                command.CommandText = SqlQueryBuilderSqlite.UpdateSetTable(tableInfo);

                break;
            case OperationType.Delete:
                command.CommandText = SqlQueryBuilderSqlite.DeleteFromTable(tableInfo);

                break;
        }

        type = tableInfo.HasAbstractList ? entities[0]?.GetType() : type;

        if (type is null)
        {
            throw new ArgumentException("Unable to determine entity type");
        }

        IEntityType? entityType = context.Model.FindEntityType(type);
        Dictionary<string, IProperty>? entityPropertiesDict = entityType?.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
        PropertyInfo[] properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

        Dictionary<string, IProperty>? entityShadowFkPropertiesDict = entityType?.GetProperties()
                                                     .Where(a => a.IsShadowProperty()
                                                                 && a.IsForeignKey()
                                                                 && a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                                                     .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);

        foreach (PropertyInfo property in properties)
        {
            IProperty? propertyEntityType = null;

            if (entityPropertiesDict?.ContainsKey(property.Name) ?? false)
            {
                propertyEntityType = entityPropertiesDict[property.Name];
            }
            else if (entityShadowFkPropertiesDict?.ContainsKey(property.Name) ?? false)
            {
                propertyEntityType = entityShadowFkPropertiesDict[property.Name];
            }

            if (propertyEntityType != null)
            {
                string columnName = propertyEntityType.Name;
                Type propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                //SqliteType(CpropertyType.Name): Text(String, Decimal, DateTime); Integer(Int16, Int32, Int64) Real(Float, Double) Blob(Guid)
                SqliteParameter parameter = new SqliteParameter($"@{columnName}", propertyType); // ,sqliteType // ,null //()
                command.Parameters.Add(parameter);
            }
        }

        HashSet<string> shadowProperties = tableInfo.ShadowProperties;

        foreach (string shadowProperty in shadowProperties)
        {
            SqliteParameter parameter = new SqliteParameter($"@{shadowProperty}", typeof(string));
            command.Parameters.Add(parameter);
        }

        command.Prepare(); // Not Required but called for efficiency (prepared should be little faster)

        return command;
    }

    internal static void LoadSqliteValues<T>(TableInfo tableInfo, T? entity, SqliteCommand command, DbContext dbContext)
    {
        Dictionary<string, string> propertyColumnsDict = tableInfo.PropertyColumnNamesDict;

        foreach (KeyValuePair<string, string> propertyColumn in propertyColumnsDict)
        {
            bool isShadowProperty = tableInfo.ShadowProperties.Contains(propertyColumn.Key);
            string parameterName = propertyColumn.Key.Replace(".", "_");
            object? value = null;

            if (!isShadowProperty)
            {
                if (propertyColumn.Key.Contains('.')) // ToDo: change IF clause to check for NavigationProperties, optimise, integrate with same code segment from LoadData method
                {
                    string[] ownedPropertyNameList = propertyColumn.Key.Split('.');
                    string ownedPropertyName = ownedPropertyNameList[0];
                    string subPropertyName = ownedPropertyNameList[1];
                    FastProperty ownedFastProperty = tableInfo.FastPropertyDict[ownedPropertyName];
                    PropertyInfo ownedProperty = ownedFastProperty.Property;

                    Type propertyType = Nullable.GetUnderlyingType(ownedProperty.GetType()) ?? ownedProperty.GetType();

                    if (!command.Parameters.Contains("@" + parameterName))
                    {
                        SqliteParameter parameter = new SqliteParameter($"@{parameterName}", propertyType);
                        command.Parameters.Add(parameter);
                    }

                    if (ownedProperty == null)
                    {
                        value = null;
                    }
                    else
                    {
                        object? ownedPropertyValue = entity == null ? null : tableInfo.FastPropertyDict[ownedPropertyName].Get(entity);
                        string subPropertyFullName = $"{ownedPropertyName}_{subPropertyName}";
                        value = ownedPropertyValue == null ? null : tableInfo.FastPropertyDict[subPropertyFullName]?.Get(ownedPropertyValue);
                    }
                }
                else if (tableInfo.FastPropertyDict.ContainsKey(propertyColumn.Key))
                {
                    value = entity is null ? null : tableInfo.FastPropertyDict[propertyColumn.Key].Get(entity);
                }
                else if (tableInfo.ColumnToPropertyDictionary.ContainsKey(propertyColumn.Key))
                {
                    IProperty property = tableInfo.ColumnToPropertyDictionary[propertyColumn.Key];

                    if (property.IsShadowProperty() && property.IsForeignKey())
                    {
                        IForeignKey? foreignKey = property.GetContainingForeignKeys().FirstOrDefault();
                        INavigation? principalNavigation = foreignKey?.DependentToPrincipal;
                        string? pkPropertyName = foreignKey?.PrincipalKey.Properties.FirstOrDefault()?.Name;

                        if (principalNavigation is not null && pkPropertyName is not null)
                        {
                            pkPropertyName = principalNavigation.Name + "_" + pkPropertyName;
                            object? fkPropertyValue = entity == null ? null : tableInfo.FastPropertyDict[principalNavigation.Name].Get(entity);
                            value = fkPropertyValue == null ? null : tableInfo.FastPropertyDict[pkPropertyName]?.Get(fkPropertyValue);
                        }
                    }
                }
            }
            else
            {
                if (tableInfo.BulkConfig.EnableShadowProperties)
                {
                    if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                    {
                        value = entity is null ? null : dbContext.Entry(entity).Property(propertyColumn.Key).CurrentValue; // Get the shadow property value
                    }
                    else
                    {
                        value = entity is null ? null : tableInfo.BulkConfig.ShadowPropertyValue(entity, propertyColumn.Key);
                    }
                }
                else
                {
                    value = entity is null ? null : dbContext.Entry(entity).Metadata.GetDiscriminatorValue(); // Set the value for the discriminator column
                }
            }

            if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(propertyColumn.Value) && value != DBNull.Value)
            {
                value = tableInfo.ConvertibleColumnConverterDict[propertyColumn.Value].ConvertToProvider.Invoke(value);
            }

            command.Parameters[$"@{parameterName}"].Value = value ?? DBNull.Value;
        }
    }

    /// <inheritdoc/>
    public static void SetIdentityForOutput<T>(IList<T> entities, TableInfo tableInfo, object? lastRowIdScalar)
    {
        long counter = (long?)lastRowIdScalar ?? 0;

        string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
        FastProperty identityFastProperty = tableInfo.FastPropertyDict[identityPropertyName];

        string idTypeName = identityFastProperty.Property.PropertyType.Name;
        object? idValue = null;

        for (int i = entities.Count - 1; i >= 0; i--)
        {
            idValue = idTypeName switch
            {
                "Int64" => counter, // long is default
                "UInt64" => (ulong)counter,
                "Int32" => (int)counter,
                "UInt32" => (uint)counter,
                "Int16" => (short)counter,
                "UInt16" => (ushort)counter,
                "Byte" => (byte)counter,
                "SByte" => (sbyte)counter,
                _ => counter,
            };

            if (entities[i] is not null)
            {
                identityFastProperty.Set(entities[i]!, idValue);
            }

            counter--;
        }
    }

    #endregion
}
