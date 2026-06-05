using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using MySqlConnector;
using NetTopologySuite.Geometries;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

/// <inheritdoc/>
public sealed class MySqlAdapter : ISqlOperationsAdapter
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
        await InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected static async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
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

        DbConnection connection = context.GetUnderlyingConnection(tableInfo.BulkConfig);

        try
        {
            IDbContextTransaction? transaction = context.Database.CurrentTransaction;
            MySqlBulkCopy mySqlBulkCopy = GetMySqlBulkCopy((MySqlConnection)connection, transaction, tableInfo.BulkConfig);

            SetMySqlBulkCopyConfig(mySqlBulkCopy, tableInfo);

            DataTable dataTable = GetDataTable(context, type, entities, mySqlBulkCopy, tableInfo);

            if (isAsync)
            {
                await mySqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                mySqlBulkCopy.WriteToServer(dataTable);
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
        await MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: true, CancellationToken.None).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
        where T : class
    {
        //Because of using temp table in case of update, we need to access created temp table in Insert method.
        bool hasExistingTransaction = context.Database.CurrentTransaction != null;
        IDbContextTransaction transaction = context.Database.CurrentTransaction ?? (isAsync ? await context.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false) : context.Database.BeginTransaction());

        if (tableInfo.BulkConfig.CustomSourceTableName == null)
        {
            tableInfo.InsertToTempTable = true;

            string sqlCreateTableCopy = SqlQueryBuilderMySql.CreateTableCopy(tableInfo, tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo.InsertToTempTable);

            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlCreateTableCopy);
            }
        }

        bool doDropUniqueConstrain = false;
        bool hasUniqueConstrain = false;

        if (string.Join("_", tableInfo.EntityPKPropertyColumnNameDict.Keys.ToList()) == string.Join("_", tableInfo.PrimaryKeysPropertyColumnNameDict.Keys.ToList()))
        {
            hasUniqueConstrain = true; // ExplicitUniqueConstrain not required for PK
        }

        if (!hasUniqueConstrain)
        {
            // TODO 
            //hasUniqueConstrain = await CheckHasExplicitUniqueConstrainAsync(context, connection, tableInfo, isAsync, cancellationToken);
            // throws "The transaction associated with this command is not the connection’s active transaction";
            // https://mysqlconnector.net/troubleshooting/transaction-usage/
        }

        if (!hasUniqueConstrain)
        {
            string createUniqueConstrain = SqlQueryBuilderMySql.CreateUniqueConstrain(tableInfo);

            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(createUniqueConstrain, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(createUniqueConstrain);
            }

            doDropUniqueConstrain = true;
        }

        if (tableInfo.CreatedOutputTable)
        {
            tableInfo.InsertToTempTable = true;
            string sqlCreateOutputTableCopy = SqlQueryBuilderMySql.CreateTableCopy(tableInfo, tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo.InsertToTempTable);

            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlCreateOutputTableCopy, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlCreateOutputTableCopy);
            }
        }

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

        try
        {
            string sqlMergeTable = SqlQueryBuilderMySql.MergeTable<T>(tableInfo, operationType);

            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlMergeTable, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlMergeTable);
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

            if (hasExistingTransaction == false && !tableInfo.BulkConfig.IncludeGraph)
            {
                if (isAsync)
                {
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    transaction.Commit();
                }
            }
        }
        finally
        {
            if (doDropUniqueConstrain)
            {
                string dropUniqueConstrain = SqlQueryBuilderMySql.DropUniqueConstrain(tableInfo);
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
                if (tableInfo.CreatedOutputTable)
                {
                    string sqlDropOutputTable = SqlQueryBuilderMySql.DropTable(tableInfo.FullTempOutputTableName, tableInfo.InsertToTempTable);
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
                    string sqlDropTable = SqlQueryBuilderMySql.DropTable(tableInfo.FullTempTableName, tableInfo.InsertToTempTable);
                    if (isAsync)
                    {
                        await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        context.Database.ExecuteSqlRaw(sqlDropTable);
                    }
                }
                if (hasExistingTransaction == false && !tableInfo.BulkConfig.IncludeGraph)
                {
                    if (isAsync)
                    {
                        await transaction.DisposeAsync().ConfigureAwait(false);
                    }
                    else
                    {
                        transaction.Dispose();
                    }
                }
            }
        }
    }

    /// <inheritdoc/>
    public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Truncate(DbContext context, TableInfo tableInfo)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    #endregion

    #region Connection

    private static MySqlBulkCopy GetMySqlBulkCopy(MySqlConnection mySqlConnection, IDbContextTransaction? transaction, BulkConfig config)
    {
        MySqlTransaction? mySqlTransaction = transaction == null ? null : (MySqlTransaction)transaction.GetUnderlyingTransaction(config);
        MySqlBulkCopy mySqlBulkCopy = new MySqlBulkCopy(mySqlConnection, mySqlTransaction);

        return mySqlBulkCopy;
    }

    /// <summary> Supports <see cref="MySqlConnector.MySqlBulkCopy"/> </summary>
    private static void SetMySqlBulkCopyConfig(MySqlBulkCopy mySqlBulkCopy, TableInfo tableInfo)
    {
        mySqlBulkCopy.DestinationTableName = tableInfo.InsertToTempTable ? tableInfo.FullTempTableName : tableInfo.FullTableName;
        mySqlBulkCopy.DestinationTableName = mySqlBulkCopy.DestinationTableName.Replace("[", "").Replace("]", "");
        mySqlBulkCopy.NotifyAfter = tableInfo.BulkConfig.NotifyAfter ?? tableInfo.BulkConfig.BatchSize;
        mySqlBulkCopy.BulkCopyTimeout = tableInfo.BulkConfig.BulkCopyTimeout ?? mySqlBulkCopy.BulkCopyTimeout;
    }

    #endregion

    #region DataTable

    public static DataTable GetDataTable<T>(DbContext context, Type type, IList<T> entities, MySqlBulkCopy mySqlBulkCopy, TableInfo tableInfo)
    {
        DataTable dataTable = InnerGetDataTable(context, ref type, entities, tableInfo);

        int sourceOrdinal = 0;

        foreach (DataColumn item in dataTable.Columns)  // Add mapping
        {
            mySqlBulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(sourceOrdinal, item.ColumnName));
            sourceOrdinal++;
        }

        return dataTable;
    }

    /// <summary> Common logic for two versions of GetDataTable </summary>
    private static DataTable InnerGetDataTable<T>(DbContext context, ref Type type, IList<T> entities, TableInfo tableInfo)
    {
        var dataTable = new DataTable();
        var columnsDict = new Dictionary<string, object?>();
        var ownedEntitiesMappedProperties = new HashSet<string>();

        StoreObjectIdentifier objectIdentifier = tableInfo.ObjectIdentifier;
        type = tableInfo.HasAbstractList ? entities[0]!.GetType() : type;
        IEntityType entityType = context.Model.FindEntityType(type) ?? throw new ArgumentException($"Unable to determine entity type from given type - {type.Name}");
        IEnumerable<IProperty> entityTypeProperties = entityType.GetProperties();
        Dictionary<string, IProperty> entityPropertiesDict = entityTypeProperties.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name) ||
                                                                   (tableInfo.BulkConfig.OperationType != OperationType.Read && a.Name == tableInfo.TimeStampPropertyName))
                                                       .ToDictionary(a => a.Name, a => a);
        Dictionary<string, INavigation> entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.TargetEntityType.IsOwned()).ToDictionary(a => a.Name, a => a);
        Dictionary<string, IProperty> entityShadowFkPropertiesDict = entityTypeProperties.Where(a => a.IsShadowProperty() &&
                                                                           a.IsForeignKey() &&
                                                                           a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                                                                     .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);

        PropertyInfo[] properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

        foreach (PropertyInfo property in properties)
        {
            bool hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                                           && !tableInfo.BulkConfig.SetOutputIdentity
                                           && tableInfo.DefaultValueProperties.Contains(property.Name);

            if (entityPropertiesDict.ContainsKey(property.Name))
            {
                IProperty propertyEntityType = entityPropertiesDict[property.Name];
                string columnName = propertyEntityType.GetColumnName(objectIdentifier) ?? string.Empty;

                bool isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);
                Type propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName].ProviderClrType : property.PropertyType;

                Type? underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry)))
                {
                    propertyType = typeof(byte[]);
                    tableInfo.HasSpatialType = true;
                    if (tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null || tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null)
                    {
                        throw new InvalidOperationException("OnCompare properties Config can not be set for Entity with Spatial types like 'Geometry'");
                    }
                }
                if (propertyType == typeof(HierarchyId) || propertyType.IsSubclassOf(typeof(HierarchyId)))
                {
                    propertyType = typeof(byte[]);
                }

                if (!columnsDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(property.Name, null);
                }
            }
            else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
            {
                IProperty fk = entityShadowFkPropertiesDict[property.Name];

                entityPropertiesDict.TryGetValue(fk.GetColumnName(objectIdentifier) ?? string.Empty, out IProperty? entityProperty);

                if (entityProperty == null) // BulkRead
                {
                    continue;
                }

                string? columnName = entityProperty.GetColumnName(objectIdentifier);

                bool isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName ?? string.Empty);
                Type propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName ?? string.Empty].ProviderClrType : entityProperty.ClrType;

                Type? underlyingType = Nullable.GetUnderlyingType(propertyType);

                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (propertyType == typeof(Geometry))
                {
                    propertyType = typeof(byte[]);
                }

                if (propertyType == typeof(HierarchyId))
                {
                    propertyType = typeof(byte[]);
                }

                if (columnName is not null && !(columnsDict.ContainsKey(columnName)) && !hasDefaultVauleOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(columnName, null);
                }
            }
            else
            {
                if (entityNavigationOwnedDict.ContainsKey(property.Name)) // isOWned
                {
                    IEntityType? ownedEntityType = context.Model.FindEntityType(property.PropertyType);

                    if (ownedEntityType == null)
                    {
                        ownedEntityType = context.Model.GetEntityTypes().SingleOrDefault(x => x.ClrType == property.PropertyType && x.Name.StartsWith(entityType.Name + "." + property.Name + "#"));
                    }

                    List<IProperty> ownedEntityProperties = ownedEntityType?.GetProperties().ToList() ?? new();
                    var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                    foreach (IProperty ownedEntityProperty in ownedEntityProperties)
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

                    PropertyInfo[] innerProperties = property.PropertyType.GetProperties();
                    if (!tableInfo.LoadOnlyPKColumn)
                    {
                        foreach (PropertyInfo innerProperty in innerProperties)
                        {
                            if (ownedEntityPropertyNameColumnNameDict.ContainsKey(innerProperty.Name))
                            {
                                string columnName = ownedEntityPropertyNameColumnNameDict[innerProperty.Name];
                                string propertyName = $"{property.Name}_{innerProperty.Name}";

                                if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(propertyName))
                                {
                                    ValueConverter convertor = tableInfo.ConvertibleColumnConverterDict[propertyName];
                                    Type underlyingType = Nullable.GetUnderlyingType(convertor.ProviderClrType) ?? convertor.ProviderClrType;
                                    dataTable.Columns.Add(columnName, underlyingType);
                                }
                                else
                                {
                                    Type ownedPropertyType = Nullable.GetUnderlyingType(innerProperty.PropertyType) ?? innerProperty.PropertyType;
                                    dataTable.Columns.Add(columnName, ownedPropertyType);
                                }

                                columnsDict.Add(property.Name + "_" + innerProperty.Name, null);
                            }
                        }
                    }
                }
            }
        }

        if (tableInfo.BulkConfig.EnableShadowProperties)
        {
            foreach (IProperty shadowProperty in entityPropertiesDict.Values.Where(a => a.IsShadowProperty()))
            {
                string? columnName = shadowProperty.GetColumnName(objectIdentifier);

                // If a model has an entity which has a relationship without an explicity defined FK, the data table will already contain the foreign key shadow property
                if (columnName is not null && dataTable.Columns.Contains(columnName))
                {
                    continue;
                }

                bool isConvertible = columnName is not null && tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);

                Type propertyType = isConvertible
                    ? tableInfo.ConvertibleColumnConverterDict[columnName!].ProviderClrType
                    : shadowProperty.ClrType;

                Type? underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry)))
                {
                    propertyType = typeof(byte[]);
                }

                if (propertyType == typeof(HierarchyId) || propertyType.IsSubclassOf(typeof(HierarchyId)))
                {
                    propertyType = typeof(byte[]);
                }

                dataTable.Columns.Add(columnName, propertyType);
                columnsDict.Add(shadowProperty.Name, null);
            }
        }

        string? discriminatorColumn = GetDiscriminatorColumn(tableInfo);

        if (discriminatorColumn != null)
        {
            IProperty discriminatorProperty = entityPropertiesDict[discriminatorColumn];

            dataTable.Columns.Add(discriminatorColumn, discriminatorProperty.ClrType);
            columnsDict.Add(discriminatorColumn, entityType.GetDiscriminatorValue());
        }

        bool hasConverterProperties = tableInfo.ConvertiblePropertyColumnDict.Count > 0;

        if (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn)
        {
            dataTable.Columns.Add(tableInfo.OriginalIndexColumnName, typeof(int));
            columnsDict.Add(tableInfo.OriginalIndexColumnName, -1);
        }

        Dictionary<string, string?> entityShadowFkPropertyColumnNamesDict = entityShadowFkPropertiesDict
            .ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));
        Dictionary<string, string?> shadowPropertyColumnNamesDict = entityPropertiesDict
            .Where(a => a.Value.IsShadowProperty())
            .ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));

        int index = 0;

        foreach (T entity in entities)
        {
            IEnumerable<PropertyInfo> propertiesToLoad = properties
                .Where(a => !tableInfo.AllNavigationsDictionary.ContainsKey(a.Name)
                            || entityShadowFkPropertiesDict.ContainsKey(a.Name)
                            || tableInfo.OwnedTypesDict.ContainsKey(a.Name)); // omit virtual Navigation (except Owned and ShadowNavig.) since it's Getter can cause unwanted Select-s from Db

            foreach (PropertyInfo property in propertiesToLoad)
            {
                object? propertyValue = tableInfo.FastPropertyDict.ContainsKey(property.Name)
                    ? tableInfo.FastPropertyDict[property.Name].Get(entity!)
                    : null;

                bool hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                                               && !tableInfo.BulkConfig.SetOutputIdentity
                                               && tableInfo.DefaultValueProperties.Contains(property.Name);

                if (tableInfo.BulkConfig.DateTime2PrecisionForceRound
                    && tableInfo.DateTime2PropertiesPrecisionLessThen7Dict.ContainsKey(property.Name))
                {
                    DateTime? dateTimePropertyValue = (DateTime?)propertyValue;

                    if (dateTimePropertyValue is not null)
                    {
                        int precision = tableInfo.DateTime2PropertiesPrecisionLessThen7Dict[property.Name];
                        int digitsToRemove = 7 - precision;
                        int powerOf10 = (int)Math.Pow(10, digitsToRemove);

                        long subsecondTicks = dateTimePropertyValue.Value!.Ticks % 10000000;
                        long ticksToRound = subsecondTicks + (subsecondTicks % 10 == 0 ? 1 : 0); // if ends with 0 add 1 tick to make sure rounding of value .5_zeros is rounded to Upper like SqlServer is doing, not to Even as Math.Round works
                        int roundedTicks = Convert.ToInt32(Math.Round((decimal)ticksToRound / powerOf10, 0)) * powerOf10;
                        dateTimePropertyValue = dateTimePropertyValue.Value!.AddTicks(-subsecondTicks).AddTicks(roundedTicks);

                        propertyValue = dateTimePropertyValue;
                    }
                }

                if (hasConverterProperties && tableInfo.ConvertiblePropertyColumnDict.ContainsKey(property.Name))
                {
                    string columnName = tableInfo.ConvertiblePropertyColumnDict[property.Name];
                    propertyValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyValue);
                }
                //TODO: Hamdling special types

                if (propertyValue is HierarchyId hierarchyValue)
                {
                    using MemoryStream memStream = new();
                    using BinaryWriter binWriter = new(memStream);
#if !NET8_0 && !NET9_0 && !NET10_0
                    hierarchyValue.Write(binWriter);
#endif
                    propertyValue = memStream.ToArray();
                }

                if (entityPropertiesDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    columnsDict[property.Name] = propertyValue;
                }
                else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
                {
                    IProperty foreignKeyShadowProperty = entityShadowFkPropertiesDict[property.Name];
                    string columnName = entityShadowFkPropertyColumnNamesDict[property.Name] ?? string.Empty;
                    if (!entityPropertiesDict.TryGetValue(columnName, out IProperty? entityProperty) || entityProperty is null)
                    {
                        continue; // BulkRead
                    }
                    ;

                    columnsDict[columnName] = propertyValue == null
                        ? null
                        : foreignKeyShadowProperty.FindFirstPrincipal()?.PropertyInfo?.GetValue(propertyValue); // TODO Check if can be optimized
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                {
                    IEnumerable<PropertyInfo> ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                    foreach (PropertyInfo ownedProperty in ownedProperties)
                    {
                        string columnName = $"{property.Name}_{ownedProperty.Name}";
                        object? ownedPropertyValue = propertyValue == null ? null : tableInfo.FastPropertyDict[columnName].Get(propertyValue);

                        if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName))
                        {
                            ValueConverter converter = tableInfo.ConvertibleColumnConverterDict[columnName];
                            columnsDict[columnName] = ownedPropertyValue == null ? null : converter.ConvertToProvider.Invoke(ownedPropertyValue);
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
                foreach (string shadowPropertyName in shadowPropertyColumnNamesDict.Keys)
                {
                    string columnName = shadowPropertyColumnNamesDict[shadowPropertyName] ?? string.Empty;

                    object? propertyValue = default(object);

                    if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                    {
                        propertyValue = context.Entry(entity!).Property(shadowPropertyName).CurrentValue;
                    }
                    else
                    {
                        propertyValue = tableInfo.BulkConfig.ShadowPropertyValue(entity!, shadowPropertyName);
                    }

                    if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName))
                    {
                        propertyValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyValue);
                    }

                    columnsDict[shadowPropertyName] = propertyValue;
                }
            }

            if (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn)
            {
                columnsDict[tableInfo.OriginalIndexColumnName] = index;
            }

            object?[] record = columnsDict.Values.ToArray();

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
            List<string> stringColumns = tableInfo.ColumnNamesTypesDict.Where(a => a.Value.Contains("char")).Select(a => a.Key).ToList();
            discriminatorColumn = tableInfo.ShadowProperties.Where(a => stringColumns.Contains(a)).ElementAt(0);
        }

        return discriminatorColumn;
    }

    #endregion
}
