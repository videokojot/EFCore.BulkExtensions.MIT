using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions;

/// <summary>
/// Provides a list of information for EFCore.BulkExtensions that is used internally to know what to do with the data source received
/// </summary>
public class TableInfo
{
    public string? Schema { get; set; }
    public string SchemaFormated => Schema != null ? $"[{Schema}]." : "";
    public string? TempSchema { get; set; }
    public string TempSchemaFormated => TempSchema != null ? $"[{TempSchema}]." : "";
    public string? TableName { get; set; }
    public string FullTableName => $"{SchemaFormated}[{TableName}]";
    public Dictionary<string, string> PrimaryKeysPropertyColumnNameDict { get; set; } = null!;
    public Dictionary<string, string> EntityPKPropertyColumnNameDict { get; set; } = null!;
    public bool HasSinglePrimaryKey { get; set; }
    public bool UpdateByPropertiesAreNullable { get; set; }

    protected string TempDBPrefix => BulkConfig.UseTempDB ? "#" : "";
    public string? TempTableSufix { get; set; }
    public string? TempTableName { get; set; }
    public string FullTempTableName => $"{TempSchemaFormated}[{TempDBPrefix}{TempTableName}]";
    public string FullTempOutputTableName => $"{SchemaFormated}[{TempDBPrefix}{TempTableName}Output]";

    public bool CreatedOutputTable => BulkConfig.SetOutputIdentity || BulkConfig.CalculateStats;

    public bool InsertToTempTable { get; set; }
    public string? IdentityColumnName { get; set; }
    public bool HasIdentity => IdentityColumnName != null;
    public ValueConverter? IdentityColumnConverter { get; set; }
    public bool HasOwnedTypes { get; set; }
    public bool HasAbstractList { get; set; }
    public bool LoadOnlyPKColumn { get; set; }
    public bool HasSpatialType { get; set; }
    public bool HasTemporalColumns { get; set; }
    public int NumberOfEntities { get; set; }

    public BulkConfig BulkConfig { get; set; } = null!;
    public Dictionary<string, string> OutputPropertyColumnNamesDict { get; set; } = new();
    public Dictionary<string, string> PropertyColumnNamesDict { get; set; } = new();
    public Dictionary<string, string> ColumnNamesTypesDict { get; set; } = new();
    public Dictionary<string, IProperty> ColumnToPropertyDictionary { get; set; } = new();
    public Dictionary<string, string> PropertyColumnNamesCompareDict { get; set; } = new();
    public Dictionary<string, string> PropertyColumnNamesUpdateDict { get; set; } = new();
    public Dictionary<string, FastProperty> FastPropertyDict { get; set; } = new();
    public Dictionary<string, INavigation> AllNavigationsDictionary { get; private set; } = null!;
    public Dictionary<string, INavigation> OwnedTypesDict { get; set; } = new();
    public HashSet<string> ShadowProperties { get; set; } = new HashSet<string>();
    public HashSet<string> DefaultValueProperties { get; set; } = new HashSet<string>();

    public Dictionary<string, string> ConvertiblePropertyColumnDict { get; set; } = new Dictionary<string, string>();
    public Dictionary<string, ValueConverter> ConvertibleColumnConverterDict { get; set; } = new Dictionary<string, ValueConverter>();
    public Dictionary<string, int> DateTime2PropertiesPrecisionLessThen7Dict { get; set; } = new Dictionary<string, int>();

    public static string TimeStampOutColumnType => "varbinary(8)";
    public string? TimeStampPropertyName { get; set; }
    public string? TimeStampColumnName { get; set; }

    protected IList<object>? EntitiesSortedReference { get; set; } // Operation Merge writes In Output table first Existing that were Updated then for new that were Inserted so this makes sure order is same in list when need to set Output

    public StoreObjectIdentifier ObjectIdentifier { get; set; }

    public DbTransaction? DbTransaction { get; set; }
    
    public string SqlActionIUD => "EFCore_BulkExtensions_MIT_MergeActionIUD";

    public string OriginalIndexColumnName => "EFCore_BulkExtensions_MIT_OriginalIndex";

 
#pragma warning restore CS1591 // No XML comments required here.

    /// <summary>
    /// Creates an instance of TableInfo
    /// </summary>
    public static TableInfo CreateInstance<T>(DbContext context, Type? type, IList<T> entities, OperationType operationType, BulkConfig? bulkConfig)
    {
        var tableInfo = new TableInfo
        {
            NumberOfEntities = entities.Count,
            BulkConfig = bulkConfig ?? new BulkConfig(),
        };
        tableInfo.BulkConfig.OperationType = operationType;

        bool isExplicitTransaction = context.Database.GetDbConnection().State == ConnectionState.Open;
        if (tableInfo.BulkConfig.UseTempDB && !isExplicitTransaction && (operationType != OperationType.Insert || tableInfo.BulkConfig.SetOutputIdentity))
        {
            throw new InvalidOperationException("When 'UseTempDB' is set then BulkOperation has to be inside Transaction. " +
                                                "Otherwise destination table gets dropped too early because transaction ends before operation is finished."); // throws: 'Cannot access destination table'
        }

        var isDeleteOperation = operationType == OperationType.Delete;
        tableInfo.LoadData(context, type, entities, isDeleteOperation);
        return tableInfo;
    }

    #region Main
    /// <summary>
    /// Configures the table info based on entity data 
    /// </summary>
    public void LoadData<T>(DbContext context, Type? type, IList<T> entities, bool loadOnlyPKColumn)

    {
        LoadOnlyPKColumn = loadOnlyPKColumn;
        var entityType = type is null ? null : context.Model.FindEntityType(type);
        if (entityType == null)
        {
            type = entities[0]?.GetType() ?? throw new ArgumentNullException(nameof(type));
            entityType = context.Model.FindEntityType(type);
            HasAbstractList = true;
        }
        if (entityType == null)
        {
            throw new InvalidOperationException($"DbContext does not contain EntitySet for Type: {type?.Name}");
        }

        //var relationalData = entityType.Relational(); relationalData.Schema relationalData.TableName // DEPRECATED in Core3.0
        string? providerName = context.Database.ProviderName?.ToLower();
        bool isSqlServer = providerName?.EndsWith(DbServerType.SQLServer.ToString().ToLower()) ?? false;
        bool isNpgsql = providerName?.EndsWith(DbServerType.PostgreSQL.ToString().ToLower()) ?? false;
        bool isSqlite = providerName?.EndsWith(DbServerType.SQLite.ToString().ToLower()) ?? false;
        bool isMySql = providerName?.EndsWith(DbServerType.MySQL.ToString().ToLower()) ?? false;

        string? defaultSchema = isSqlServer ? "dbo" : null;

        string? customSchema = null;
        string? customTableName = null;
        if (BulkConfig.CustomDestinationTableName != null)
        {
            customTableName = BulkConfig.CustomDestinationTableName;
            if (customTableName.Contains('.'))
            {
                var tableNameSplitList = customTableName.Split('.');
                customSchema = tableNameSplitList[0];
                customTableName = tableNameSplitList[1];
            }
        }
        Schema = customSchema ?? entityType.GetSchema() ?? defaultSchema;
        var entityTableName = entityType.GetTableName();
        TableName = customTableName ?? entityTableName;

        string? sourceSchema = null;
        string? sourceTableName = null;
        if (BulkConfig.CustomSourceTableName != null)
        {
            sourceTableName = BulkConfig.CustomSourceTableName;
            if (sourceTableName.Contains('.'))
            {
                var tableNameSplitList = sourceTableName.Split('.');
                sourceSchema = tableNameSplitList[0];
                sourceTableName = tableNameSplitList[1];
            }
            BulkConfig.UseTempDB = false;
        }

        TempSchema = sourceSchema ?? Schema;
        TempTableSufix = sourceTableName != null ? "" : "Temp";
        if (BulkConfig.UniqueTableNameTempDb)
        {
            // 8 chars of Guid as tableNameSufix to avoid same name collision with other tables
            TempTableSufix += Guid.NewGuid().ToString()[..8];
            // TODO Consider Hash                                                             
        }
        TempTableName = sourceTableName ?? $"{TableName}{TempTableSufix}";

        if (entityTableName is null)
        {
            throw new ArgumentException("Entity does not contain a table name");
        }

        ObjectIdentifier = StoreObjectIdentifier.Table(entityTableName, entityType.GetSchema());

        var allProperties = new List<IProperty>();
        foreach (var entityProperty in entityType.GetProperties())
        {
            var columnName = entityProperty.GetColumnName(ObjectIdentifier);
            bool isTemporalColumn = columnName is not null
                && entityProperty.IsShadowProperty()
                && entityProperty.ClrType == typeof(DateTime)
                && BulkConfig.TemporalColumns.Contains(columnName);

            HasTemporalColumns = HasTemporalColumns || isTemporalColumn;

            if (columnName == null || isTemporalColumn)
                continue;

            allProperties.Add(entityProperty);
            ColumnNamesTypesDict.Add(columnName, entityProperty.GetColumnType());
            ColumnToPropertyDictionary.Add(columnName, entityProperty);

            if (BulkConfig.DateTime2PrecisionForceRound)
            {
                var columnMappings = entityProperty.GetTableColumnMappings();
                var firstMapping = columnMappings.FirstOrDefault();
                var columnType = firstMapping?.Column.StoreType;
                if ((columnType?.StartsWith("datetime2(") ?? false) && (!columnType?.EndsWith("7)") ?? false))
                {
                    string precisionText = columnType!.Substring(10, 1);
                    int precision = int.Parse(precisionText);
                    DateTime2PropertiesPrecisionLessThen7Dict.Add(firstMapping!.Property.Name, precision); // SqlBulkCopy does Floor instead of Round so Rounding done in memory
                }
            }
        }

        bool areSpecifiedUpdateByProperties = BulkConfig.UpdateByProperties?.Count > 0;
        var primaryKeys = entityType.FindPrimaryKey()?.Properties?.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier) ?? string.Empty);
        EntityPKPropertyColumnNameDict = primaryKeys ?? new Dictionary<string, string>();

        HasSinglePrimaryKey = primaryKeys?.Count == 1;
        PrimaryKeysPropertyColumnNameDict = areSpecifiedUpdateByProperties ? BulkConfig.UpdateByProperties?.ToDictionary(a => a, b => allProperties.First(p => p.Name == b).GetColumnName(ObjectIdentifier) ?? string.Empty) ?? new()
                                                                           : (primaryKeys ?? new Dictionary<string, string>());

        // load all derived type properties
        if (entityType.IsAbstract())
        {
            foreach (var derivedType in entityType.GetDirectlyDerivedTypes())
            {
                foreach (var derivedProperty in derivedType.GetProperties())
                {
                    if (!allProperties.Contains(derivedProperty))
                        allProperties.Add(derivedProperty);
                }
            }
        }

        var navigations = entityType.GetNavigations();
        AllNavigationsDictionary = navigations.ToDictionary(nav => nav.Name, nav => nav);

        var ownedTypes = navigations.Where(a => a.TargetEntityType.IsOwned());
        HasOwnedTypes = ownedTypes.Any();
        OwnedTypesDict = ownedTypes.ToDictionary(a => a.Name, a => a);

        if (isSqlServer || isNpgsql || isMySql)
        {
            var strategyName = SqlAdaptersMapping.DbServer(context).ValueGenerationStrategy;
            if (!strategyName.Contains(":Value"))
            {
                strategyName = strategyName.Replace("Value", ":Value"); //example 'SqlServer:ValueGenerationStrategy'
            }

            foreach (var property in allProperties)
            {
                var annotation = property.FindAnnotation(strategyName);
                bool hasIdentity = false;
                if (annotation != null)
                {
                    hasIdentity = SqlAdaptersMapping.DbServer(context).PropertyHasIdentity(annotation);
                }
                if (hasIdentity)
                {
                    IdentityColumnName = property.GetColumnName(ObjectIdentifier);
                    break;
                }
            }
        }
        if (isSqlite) // SQLite no ValueGenerationStrategy
        {
            // for HiLo on SqlServer was returning True when should be False
            IdentityColumnName = allProperties.SingleOrDefault(a => a.IsPrimaryKey() &&
                                                    a.ValueGenerated == ValueGenerated.OnAdd && // ValueGenerated equals OnAdd for nonIdentity column like Guid so take only number types
                                                    (a.ClrType.Name.StartsWith("Byte") ||
                                                     a.ClrType.Name.StartsWith("SByte") ||
                                                     a.ClrType.Name.StartsWith("Int") ||
                                                     a.ClrType.Name.StartsWith("UInt") ||
                                                     (isSqlServer && a.ClrType.Name.StartsWith("Decimal")))
                                              )?.GetColumnName(ObjectIdentifier);
        }

        // timestamp/row version properties are only set by the Db, the property has a [Timestamp] Attribute or is configured in FluentAPI with .IsRowVersion()
        // They can be identified by the columne type "timestamp" or .IsConcurrencyToken in combination with .ValueGenerated == ValueGenerated.OnAddOrUpdate
        string timestampDbTypeName = nameof(TimestampAttribute).Replace("Attribute", "").ToLower(); // = "timestamp";
        IEnumerable<IProperty> timeStampProperties;
        if (BulkConfig.IgnoreRowVersion)
            timeStampProperties = new List<IProperty>();
        else
            timeStampProperties = allProperties.Where(a => a.IsConcurrencyToken && a.ValueGenerated == ValueGenerated.OnAddOrUpdate); // || a.GetColumnType() == timestampDbTypeName // removed as unnecessary and might not be correct

        TimeStampColumnName = timeStampProperties.FirstOrDefault()?.GetColumnName(ObjectIdentifier); // can be only One
        TimeStampPropertyName = timeStampProperties.FirstOrDefault()?.Name; // can be only One
        var allPropertiesExceptTimeStamp = allProperties.Except(timeStampProperties);
        var properties = allPropertiesExceptTimeStamp.Where(a => a.GetComputedColumnSql() == null);

        var propertiesWithDefaultValues = allPropertiesExceptTimeStamp.Where(a =>
            !a.IsShadowProperty() &&
            (a.GetDefaultValueSql() != null ||
             (a.GetDefaultValue() != null &&
              a.ValueGenerated != ValueGenerated.Never &&
              a.ClrType != typeof(Guid)) // Since .Net_6.0 in EF 'Guid' type has DefaultValue even when not explicitly defined with Annotation or FluentApi
            ));
        foreach (var propertyWithDefaultValue in propertiesWithDefaultValues)
        {
            var propertyType = propertyWithDefaultValue.ClrType;
            var instance = propertyType.IsValueType || propertyType.GetConstructor(Type.EmptyTypes) != null
                              ? Activator.CreateInstance(propertyType)
                              : null; // when type does not have parameterless constructor, like String for example, then default value is 'null'

            bool listHasAllDefaultValues = !entities.Any(a => a?.GetType().GetProperty(propertyWithDefaultValue.Name)?.GetValue(a, null)?.ToString() != instance?.ToString());
            // it is not feasible to have in same list simultaneously both entities groups With and Without default values, they are omitted OnInsert only if all have default values or if it is PK (like Guid DbGenerated)
            if (listHasAllDefaultValues || (PrimaryKeysPropertyColumnNameDict.ContainsKey(propertyWithDefaultValue.Name) && propertyType == typeof(Guid)))
            {
                DefaultValueProperties.Add(propertyWithDefaultValue.Name);
            }
        }

        var propertiesOnCompare = allPropertiesExceptTimeStamp.Where(a => a.GetComputedColumnSql() == null);
        var propertiesOnUpdate = allPropertiesExceptTimeStamp.Where(a => a.GetComputedColumnSql() == null);

        // TimeStamp prop. is last column in OutputTable since it is added later with varbinary(8) type in which Output can be inserted
        var outputProperties = allPropertiesExceptTimeStamp.Where(a => a.GetColumnName(ObjectIdentifier) != null).Concat(timeStampProperties);
        OutputPropertyColumnNamesDict = outputProperties.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier)?.Replace("]", "]]") ?? string.Empty); // square brackets have to be escaped

        bool AreSpecifiedPropertiesToInclude = BulkConfig.PropertiesToInclude?.Count > 0;
        bool AreSpecifiedPropertiesToExclude = BulkConfig.PropertiesToExclude?.Count > 0;

        bool AreSpecifiedPropertiesToIncludeOnCompare = BulkConfig.PropertiesToIncludeOnCompare?.Count > 0;
        bool AreSpecifiedPropertiesToExcludeOnCompare = BulkConfig.PropertiesToExcludeOnCompare?.Count > 0;

        bool AreSpecifiedPropertiesToIncludeOnUpdate = BulkConfig.PropertiesToIncludeOnUpdate?.Count > 0;
        bool AreSpecifiedPropertiesToExcludeOnUpdate = BulkConfig.PropertiesToExcludeOnUpdate?.Count > 0;

        if (AreSpecifiedPropertiesToInclude)
        {
            if (areSpecifiedUpdateByProperties) // Adds UpdateByProperties to PropertyToInclude if they are not already explicitly listed
            {
                if (BulkConfig.UpdateByProperties is not null)
                {
                    foreach (var updateByProperty in BulkConfig.UpdateByProperties)
                    {
                        if (!BulkConfig.PropertiesToInclude?.Contains(updateByProperty) ?? false)
                        {
                            BulkConfig.PropertiesToInclude?.Add(updateByProperty);
                        }
                    }
                }
            }
            else // Adds PrimaryKeys to PropertyToInclude if they are not already explicitly listed
            {
                foreach (var primaryKey in PrimaryKeysPropertyColumnNameDict)
                {
                    if (!BulkConfig.PropertiesToInclude?.Contains(primaryKey.Key) ?? false)
                    {
                        BulkConfig.PropertiesToInclude?.Add(primaryKey.Key);
                    }
                }
            }
        }

        foreach (var property in allProperties)
        {
            if (property.PropertyInfo != null) // skip Shadow Property
            {
                FastPropertyDict.Add(property.Name, FastProperty.GetOrCreate(property.PropertyInfo));
            }

            if (property.IsShadowProperty() && property.IsForeignKey())
            {
                // TODO: Does Shadow ForeignKey Property aways contain only one ForgeignKey? 
                var navigationProperty = property.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.PropertyInfo;
                if (navigationProperty is not null)
                {
                    var navigationEntityType = context.Model.FindEntityType(navigationProperty.PropertyType);
                    var navigationProperties = navigationEntityType?.GetProperties().Where(p => p.IsPrimaryKey()).ToList() ?? new();

                    foreach (var navEntityProperty in navigationProperties)
                    {
                        var fullName = navigationProperty.Name + "_" + navEntityProperty.Name;
                        if (!FastPropertyDict.ContainsKey(fullName) && navEntityProperty.PropertyInfo is not null)
                        {
                            FastPropertyDict.Add(fullName, FastProperty.GetOrCreate(navEntityProperty.PropertyInfo));
                        }
                    }
                }
            }

            var converter = property.GetTypeMapping().Converter;
            if (converter is not null)
            {
                var columnName = property.GetColumnName(ObjectIdentifier) ?? string.Empty;
                ConvertiblePropertyColumnDict.Add(property.Name, columnName);
                ConvertibleColumnConverterDict.Add(columnName, converter);

                if (columnName == IdentityColumnName)
                    IdentityColumnConverter = converter;
            }
        }

        UpdateByPropertiesAreNullable = properties.Any(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Name) && a.IsNullable);

        if (AreSpecifiedPropertiesToInclude || AreSpecifiedPropertiesToExclude)
        {
            if (AreSpecifiedPropertiesToInclude && AreSpecifiedPropertiesToExclude)
            {
                throw new MultiplePropertyListSetException(nameof(BulkConfig.PropertiesToInclude), nameof(BulkConfig.PropertiesToExclude));
            }
            if (AreSpecifiedPropertiesToInclude)
            {
                properties = properties.Where(a => BulkConfig.PropertiesToInclude?.Contains(a.Name) ?? false);
                ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToInclude, nameof(BulkConfig.PropertiesToInclude));
            }
            if (AreSpecifiedPropertiesToExclude)
            {
                properties = properties.Where(a => !BulkConfig.PropertiesToExclude?.Contains(a.Name) ?? false);
                ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToExclude, nameof(BulkConfig.PropertiesToExclude));
            }
        }

        if (AreSpecifiedPropertiesToIncludeOnCompare || AreSpecifiedPropertiesToExcludeOnCompare)
        {
            if (AreSpecifiedPropertiesToIncludeOnCompare && AreSpecifiedPropertiesToExcludeOnCompare)
            {
                throw new MultiplePropertyListSetException(nameof(BulkConfig.PropertiesToIncludeOnCompare), nameof(BulkConfig.PropertiesToExcludeOnCompare));
            }
            if (AreSpecifiedPropertiesToIncludeOnCompare)
            {
                propertiesOnCompare = propertiesOnCompare.Where(a => BulkConfig.PropertiesToIncludeOnCompare?.Contains(a.Name) ?? false);
                ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToIncludeOnCompare, nameof(BulkConfig.PropertiesToIncludeOnCompare));
            }
            if (AreSpecifiedPropertiesToExcludeOnCompare)
            {
                propertiesOnCompare = propertiesOnCompare.Where(a => !BulkConfig.PropertiesToExcludeOnCompare?.Contains(a.Name) ?? false);
                ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToExcludeOnCompare, nameof(BulkConfig.PropertiesToExcludeOnCompare));
            }
        }
        else
        {
            propertiesOnCompare = properties;
        }
        if (AreSpecifiedPropertiesToIncludeOnUpdate || AreSpecifiedPropertiesToExcludeOnUpdate)
        {
            if (AreSpecifiedPropertiesToIncludeOnUpdate && AreSpecifiedPropertiesToExcludeOnUpdate)
            {
                throw new MultiplePropertyListSetException(nameof(BulkConfig.PropertiesToIncludeOnUpdate), nameof(BulkConfig.PropertiesToExcludeOnUpdate));
            }
            if (AreSpecifiedPropertiesToIncludeOnUpdate)
            {
                propertiesOnUpdate = propertiesOnUpdate.Where(a => BulkConfig.PropertiesToIncludeOnUpdate?.Contains(a.Name) ?? false);
                ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToIncludeOnUpdate, nameof(BulkConfig.PropertiesToIncludeOnUpdate));
            }
            if (AreSpecifiedPropertiesToExcludeOnUpdate)
            {
                propertiesOnUpdate = propertiesOnUpdate.Where(a => !BulkConfig.PropertiesToExcludeOnUpdate?.Contains(a.Name) ?? false);
                ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToExcludeOnUpdate, nameof(BulkConfig.PropertiesToExcludeOnUpdate));
            }
        }
        else
        {
            propertiesOnUpdate = properties;

            if (BulkConfig.UpdateByProperties != null) // to remove NonIdentity PK like Guid from SET ID = ID, ...
            {
                propertiesOnUpdate = propertiesOnUpdate.Where(a => !BulkConfig.UpdateByProperties.Contains(a.Name));
            }
            else if (primaryKeys != null)
            {
                propertiesOnUpdate = propertiesOnUpdate.Where(a => !primaryKeys.ContainsKey(a.Name));
            }
        }

        PropertyColumnNamesCompareDict = propertiesOnCompare.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier)?.Replace("]", "]]") ?? string.Empty);
        PropertyColumnNamesUpdateDict = propertiesOnUpdate.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier)?.Replace("]", "]]") ?? string.Empty);

        if (loadOnlyPKColumn)
        {
            if (PrimaryKeysPropertyColumnNameDict.Count == 0)
                throw new InvalidBulkConfigException("If no PrimaryKey is defined operation requres bulkConfig set with 'UpdatedByProperties'.");
            PropertyColumnNamesDict = properties.Where(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier)?.Replace("]", "]]") ?? string.Empty);
        }
        else
        {
            PropertyColumnNamesDict = properties.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier)?.Replace("]", "]]") ?? string.Empty);
            ShadowProperties = new HashSet<string>(properties.Where(p => p.IsShadowProperty() && !p.IsForeignKey()).Select(p => p.GetColumnName(ObjectIdentifier) ?? string.Empty));

            foreach (var navigation in entityType.GetNavigations().Where(a => !a.IsCollection && !a.TargetEntityType.IsOwned()))
            {
                if (navigation.PropertyInfo is not null)
                {
                    FastPropertyDict.Add(navigation.Name, FastProperty.GetOrCreate(navigation.PropertyInfo));
                }
            }

            if (HasOwnedTypes)  // Support owned entity property update. TODO: Optimize
            {
                foreach (var navigationProperty in ownedTypes)
                {
                    var property = navigationProperty.PropertyInfo;
                    FastPropertyDict.Add(property!.Name, FastProperty.GetOrCreate(property));

                    // If the OwnedType is mapped to the separate table, don't try merge it into its owner
                    if (OwnedTypeUtil.IsOwnedInSameTableAsOwner(navigationProperty) == false)
                        continue;

                    //Type navOwnedType = type?.Assembly.GetType(property.PropertyType.FullName!) ?? throw new ArgumentException("Unable to determine Type"); // was not used
                    var ownedEntityType = context.Model.FindEntityType(property.PropertyType);
                    if (ownedEntityType == null) // when entity has more then one ownedType (e.g. Address HomeAddress, Address WorkAddress) or one ownedType is in multiple Entities like Audit is usually.
                    {
                        ownedEntityType = context.Model.GetEntityTypes().SingleOrDefault(x => x.ClrType == property.PropertyType && x.Name.StartsWith(entityType.Name + "." + property.Name + "#"));
                    }
                    var ownedEntityProperties = ownedEntityType?.GetProperties().ToList() ?? new();
                    var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                    foreach (var ownedEntityProperty in ownedEntityProperties)
                    {
                        string columnName = ownedEntityProperty.GetColumnName(ObjectIdentifier) ?? string.Empty;

                        if (!ownedEntityProperty.IsPrimaryKey())
                        {
                            ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                            var ownedEntityPropertyFullName = property.Name + "_" + ownedEntityProperty.Name;
                            if (!FastPropertyDict.ContainsKey(ownedEntityPropertyFullName) && ownedEntityProperty.PropertyInfo is not null)
                            {
                                FastPropertyDict.Add(ownedEntityPropertyFullName, FastProperty.GetOrCreate(ownedEntityProperty.PropertyInfo));
                            }
                        }

                        var converter = ownedEntityProperty.GetValueConverter();
                        if (converter != null)
                        {
                            ConvertibleColumnConverterDict.Add($"{navigationProperty.Name}_{ownedEntityProperty.Name}", converter);
                        }

                        ColumnNamesTypesDict[columnName] = ownedEntityProperty.GetColumnType();
                    }
                    var ownedProperties = property.PropertyType.GetProperties();
                    foreach (var ownedProperty in ownedProperties)
                    {
                        if (ownedEntityPropertyNameColumnNameDict.TryGetValue(ownedProperty.Name, out var columnName))
                        {
                            string ownedPropertyFullName = property.Name + "." + ownedProperty.Name;
                            var ownedPropertyType = Nullable.GetUnderlyingType(ownedProperty.PropertyType) ?? ownedProperty.PropertyType;

                            bool doAddProperty = true;
                            
                            if (AreSpecifiedPropertiesToInclude && !(BulkConfig.PropertiesToInclude?.Contains(ownedPropertyFullName) ?? false))
                            {
                                doAddProperty = false;
                            }
                            
                            if (AreSpecifiedPropertiesToExclude && (BulkConfig.PropertiesToExclude?.Contains(ownedPropertyFullName) ?? false))
                            {
                                doAddProperty = false;
                            }

                            if (doAddProperty)
                            {
                                PropertyColumnNamesDict.Add(ownedPropertyFullName, columnName);
                                PropertyColumnNamesCompareDict.Add(ownedPropertyFullName, columnName);
                                PropertyColumnNamesUpdateDict.Add(ownedPropertyFullName, columnName);
                                OutputPropertyColumnNamesDict.Add(ownedPropertyFullName, columnName);
                            }
                        }
                    }
                }
            }
        }
    }


    /// <summary>
    /// Validates the specified property list
    /// </summary>
    protected void ValidateSpecifiedPropertiesList(List<string>? specifiedPropertiesList, string specifiedPropertiesListName)

    {
        if (specifiedPropertiesList is not null)
        {
            foreach (var configSpecifiedPropertyName in specifiedPropertiesList)
            {

                if (!FastPropertyDict.Any(a => a.Key == configSpecifiedPropertyName) &&
                    !configSpecifiedPropertyName.Contains('.') && // Those with dot "." skiped from validating for now since FastPropertyDict here does not contain them
                    !(specifiedPropertiesListName == nameof(BulkConfig.PropertiesToIncludeOnUpdate) && configSpecifiedPropertyName == "") && // In PropsToIncludeOnUpdate empty is allowed as config for skipping Update
                    !BulkConfig.TemporalColumns.Contains(configSpecifiedPropertyName)
                    )
                {
                    throw new InvalidOperationException($"PropertyName '{configSpecifiedPropertyName}' specified in '{specifiedPropertiesListName}' not found in Properties.");
                }
            }
        }
    }

    #endregion

    #region SqlCommands

    public record struct MergeActionCounts(int Inserted, int Updated, int Deleted);

    public async Task<MergeActionCounts> GetMergeActionCounts(DbContext context, bool isAsync, CancellationToken cancellationToken)
    {
        var commandText = $"SELECT COUNT (*) FROM {FullTempOutputTableName} WHERE [{SqlActionIUD}] = 'I';\n"
                          + $"SELECT COUNT (*) FROM {FullTempOutputTableName} WHERE [{SqlActionIUD}] = 'U' ;\n"
                          + $"SELECT COUNT (*) FROM {FullTempOutputTableName} WHERE [{SqlActionIUD}] = 'D';";

        var inserted = -1;
        var updated = -1;
        var deleted = -1;

        if (isAsync)
        {
            await GetMergeActionCountsInternalAsync().ConfigureAwait(false);
        }
        else
        {
            GetMergeActionCountsInternal();
        }

        return new MergeActionCounts(inserted, updated, deleted);

        async Task GetMergeActionCountsInternalAsync()
        {
            #pragma warning disable CA2007

            await using var command = context.Database.GetDbConnection().CreateCommand();

            command.CommandText = commandText;
            
            if (command.Connection!.State != ConnectionState.Open)
            {
               await command.Connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
            
            if (context.Database.CurrentTransaction != null)
            {
                command.Transaction = context.Database.CurrentTransaction.GetDbTransaction();
            }
            
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            inserted = reader.GetInt32(0);
            await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            updated = reader.GetInt32(0);
            await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            deleted = reader.GetInt32(0);
            #pragma warning restore CA2007
        }

        void GetMergeActionCountsInternal()
        {
            using var command = context.Database.GetDbConnection().CreateCommand();

            command.CommandText = commandText;
            
            if (command.Connection!.State != ConnectionState.Open)
            {
                command.Connection.Open();
            }
            
            if (context.Database.CurrentTransaction != null)
            {
                command.Transaction = context.Database.CurrentTransaction.GetDbTransaction();
            }
            
            using var reader = command.ExecuteReader();

            reader.Read();
            inserted = reader.GetInt32(0);
            reader.NextResult();

            reader.Read();
            updated = reader.GetInt32(0);
            reader.NextResult();

            reader.Read();
            deleted = reader.GetInt32(0);
        }
    }

    #endregion

    /// <summary>
    /// Returns the unique property values
    /// </summary>
    public static string GetUniquePropertyValues(object entity, List<string> propertiesNames, Dictionary<string, FastProperty> fastPropertyDict)
    {
        StringBuilder uniqueBuilder = new(1024);
        string delimiter = "_"; // TODO: Consider making it Config-urable
        foreach (var propertyName in propertiesNames)
        {
            var property = fastPropertyDict[propertyName].Get(entity);
            if (property is Array propertyArray)
            {
                foreach (var element in propertyArray)
                {
                    uniqueBuilder.Append(element?.ToString() ?? "null");
                }
            }
            else
            {
                uniqueBuilder.Append(property?.ToString() ?? "null");
            }

            uniqueBuilder.Append(delimiter);
        }
        string result = uniqueBuilder.ToString();
        result = result[0..^1]; // removes last delimiter
        return result;
    }

    #region ReadProcedures
    /// <summary>
    /// Configures the bulk read column names for the table info
    /// </summary>
    /// <returns></returns>
    public Dictionary<string, string> ConfigureBulkReadTableInfo()
    {
        InsertToTempTable = true;

        var previousPropertyColumnNamesDict = PropertyColumnNamesDict;
        BulkConfig.PropertiesToInclude = PrimaryKeysPropertyColumnNameDict.Select(a => a.Key).ToList();
        PropertyColumnNamesDict = PropertyColumnNamesDict.Where(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Key)).ToDictionary(a => a.Key, a => a.Value);
        return previousPropertyColumnNamesDict;
    }

    internal void UpdateReadEntities<T>(IList<T> entities, IList<T> existingEntities, DbContext context)
    {
        List<string> propertyNames = PropertyColumnNamesDict.Keys.ToList();
        if (HasOwnedTypes)
        {
            foreach (string ownedTypeName in OwnedTypesDict.Keys)
            {
                var ownedTypeProperties = OwnedTypesDict[ownedTypeName].ClrType.GetProperties();
                foreach (var ownedTypeProperty in ownedTypeProperties)
                {
                    propertyNames.Remove(ownedTypeName + "." + ownedTypeProperty.Name);
                }
                propertyNames.Add(ownedTypeName);
            }
        }

        List<string> selectByPropertyNames = PropertyColumnNamesDict.Keys
            .Where(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a)).ToList();

        Dictionary<string, T> existingEntitiesDict = new();
        foreach (var existingEntity in existingEntities)
        {
            string uniqueProperyValues = GetUniquePropertyValues(existingEntity!, selectByPropertyNames, FastPropertyDict);
            existingEntitiesDict.TryAdd(uniqueProperyValues, existingEntity);
        }

        for (int i = 0; i < NumberOfEntities; i++)
        {
            T entity = entities[i];
            string uniqueProperyValues = GetUniquePropertyValues(entity!, selectByPropertyNames, FastPropertyDict);

            existingEntitiesDict.TryGetValue(uniqueProperyValues, out T? existingEntity);
            bool isPostgreSQL = context.Database.ProviderName?.EndsWith(DbServerType.PostgreSQL.ToString()) ?? false;
            if (existingEntity == null && isPostgreSQL && i < existingEntities.Count)
            {
                existingEntity = existingEntities[i]; // TODO check if BinaryImport with COPY on Postgres preserves order
            }
            if (existingEntity != null)
            {
                foreach (var propertyName in propertyNames)
                {
                    if (FastPropertyDict.ContainsKey(propertyName))
                    {
                        var propertyValue = FastPropertyDict[propertyName].Get(existingEntity);
                        FastPropertyDict[propertyName].Set(entity!, propertyValue);
                    }
                    else
                    {
                       //TODO: Shadow FK property update
                    }
                    
                }
            }
        }
    }

    internal void ReplaceReadEntities<T>(IList<T> entities, IList<T> existingEntities)
    {
        entities.Clear();

        foreach (var existingEntity in existingEntities)
        {
            entities.Add(existingEntity);
        }
    }
    #endregion
    
    /// <summary>
    /// Sets the identity preserve order
    /// </summary>
    public void CheckToSetIdentityForPreserveOrder<T>(TableInfo tableInfo, IList<T> entities, bool reset = false)
    {
        string identityPropertyName = PropertyColumnNamesDict.SingleOrDefault(a => a.Value == IdentityColumnName).Key;

        bool doSetIdentityColumnsForInsertOrder = BulkConfig.PreserveInsertOrder &&
                                                  entities.Count > 1 &&
                                                  PrimaryKeysPropertyColumnNameDict?.Count == 1 &&
                                                  PrimaryKeysPropertyColumnNameDict?.Select(a => a.Value).First() == IdentityColumnName;

        var operationType = tableInfo.BulkConfig.OperationType;
        if (doSetIdentityColumnsForInsertOrder)
        {
            if (operationType == OperationType.Insert) // Insert should either have all zeros for automatic order, or they can be manually set
            {
                var propertyValue = FastPropertyDict[identityPropertyName].Get(entities[0]!);
                var identityValue = Convert.ToInt64(IdentityColumnConverter != null ? IdentityColumnConverter.ConvertToProvider(propertyValue) : propertyValue);

                if (identityValue != 0) // (to check it fast, condition for all 0s is only done on first one)
                {
                    doSetIdentityColumnsForInsertOrder = false;
                }
            }
        }

        if (doSetIdentityColumnsForInsertOrder)
        {
            bool sortEntities = !reset && BulkConfig.SetOutputIdentity &&
                                (operationType == OperationType.Update || operationType == OperationType.InsertOrUpdate || operationType == OperationType.InsertOrUpdateOrDelete);
            var entitiesExistingDict = new Dictionary<long, T>();
            var entitiesNew = new List<T>();

            long i = -entities.Count;
            foreach (var entity in entities)
            {
                var identityFastProperty = FastPropertyDict[identityPropertyName];
                var propertyValue = identityFastProperty.Get(entity!);
                long identityValue = Convert.ToInt64(IdentityColumnConverter != null ? IdentityColumnConverter.ConvertToProvider(propertyValue) : propertyValue);

                if (identityValue == 0 ||         // set only zero(0) values
                    (identityValue < 0 && reset)) // set only negative(-N) values if reset
                {
                    long value = reset ? 0 : i;
                    object idValue;
                    var idType = identityFastProperty.Property.PropertyType;
                    if (idType == typeof(ushort))
                        idValue = (ushort)value;
                    if (idType == typeof(short))
                        idValue = (short)value;
                    else if (idType == typeof(uint))
                        idValue = (uint)value;
                    else if (idType == typeof(int))
                        idValue = (int)value;
                    else if (idType == typeof(ulong))
                        idValue = (ulong)value;
                    else if (idType == typeof(decimal))
                        idValue = (decimal)value;
                    else
                        idValue = value; // type 'long' left as default

                    identityFastProperty.Set(entity!, IdentityColumnConverter != null ? IdentityColumnConverter.ConvertFromProvider(idValue) : idValue);
                    i++;
                }
                if (sortEntities)
                {
                    if (identityValue != 0)
                        entitiesExistingDict.Add(identityValue, entity); // first load existing ones
                    else
                        entitiesNew.Add(entity);
                }
            }
            if (sortEntities)
            {
                List<T> entitiesSorted = entitiesExistingDict.OrderBy(a => a.Key).Select(a => a.Value).ToList();
                entitiesSorted.AddRange(entitiesNew); // then append new ones
                tableInfo.EntitiesSortedReference = entitiesSorted.Cast<object>().ToList();
            }
        }
    }

    /// <summary>
    /// Loads the output entities
    /// </summary>
    public List<T> LoadOutputEntities<T>(DbContext context, Type type, string sqlSelect) where T : class
    {
        List<T> existingEntities;
        if (typeof(T) == type)
        {
            Expression<Func<DbContext, IQueryable<T>>> expression = GetQueryExpression<T>(sqlSelect, false);
            var compiled = EF.CompileQuery(expression); // instead using Compiled queries
            existingEntities = compiled(context).ToList();
        }
        else // TODO: Consider removing
        {
            Expression<Func<DbContext, IEnumerable>> expression = GetQueryExpression(type, sqlSelect, false);
            var compiled = EF.CompileQuery(expression); // instead using Compiled queries
            existingEntities = compiled(context).Cast<T>().ToList();
        }
        return existingEntities;
    }

    /// <summary>
    /// Updates the entities' identity field
    /// </summary>
    internal void UpdateEntitiesIdentity<T>(TableInfo tableInfo, IList<T> entities, IList<object> entitiesWithOutputIdentity)
    {
        var identifierPropertyName = IdentityColumnName != null ? OutputPropertyColumnNamesDict.SingleOrDefault(a => a.Value == IdentityColumnName).Key // it Identity autoincrement 
                                                                : PrimaryKeysPropertyColumnNameDict.FirstOrDefault().Key;                               // or PK with default sql value

        if (BulkConfig.PreserveInsertOrder) // Updates Db changed Columns in entityList
        {
            int countDiff = entities.Count - entitiesWithOutputIdentity.Count;
            if (countDiff > 0) // When some ommited from Merge because of TimeStamp conflict then changes are not loaded but output is set in TimeStampInfo
            {
                tableInfo.BulkConfig.TimeStampInfo = new TimeStampInfo
                {
                    NumberOfSkippedForUpdate = countDiff,
                    EntitiesOutput = entitiesWithOutputIdentity.ToList()
                };
                return;
            }
            
            
            var customPK = tableInfo.PrimaryKeysPropertyColumnNameDict.Keys;

            if (countDiff < 0)
            {
                // This might happen in case of BulkInsertOrUpdate with custom UpdateBy properties, when there are multiple matching rows in table which is updated
                // for more see: https://github.com/borisdj/EFCore.BulkExtensions/issues/1251
                // In case of setting output identity, we cannot decide which id we should use (as there might be multiple rows in output table, which 'belong' to only one row in source table).

                var nonUniqueKeys = entitiesWithOutputIdentity.GroupBy(x => new PrimaryKeysPropertyColumnNameValues(customPK.Select(c => FastPropertyDict[c].Get(x)))).Where(x => x.Count() > 1).Select(x => x.Key).ToList();

                throw new BulkExtensionsException(BulkExtensionsExceptionType.CannotSetOutputIdentityForNonUniqueUpdateByProperties,
                                                  "Items were Inserted/Updated successfully in db, but we cannot set output identity correctly since single source row(s) matched multiple rows in db. "
                                                  + "Keys which matched more rows: "
                                                  + string.Join("\n", nonUniqueKeys.Select(x => x.ToLogString())));
            }

            if (tableInfo.EntitiesSortedReference != null)
            {
                entities = tableInfo.EntitiesSortedReference.Cast<T>().ToList();
            }
            
            
            // (UpsertOrderTest) fix for BulkInsertOrUpdate assigns wrong output IDs when PreserveInsertOrder = true and SetOutputIdentity = true
            var setByDictionary = !(customPK.Count == 1 && customPK.First() == identifierPropertyName)
                                  && (tableInfo.BulkConfig.OperationType == OperationType.Update
                                      || tableInfo.BulkConfig.OperationType == OperationType.InsertOrUpdate
                                      || tableInfo.BulkConfig.OperationType == OperationType.InsertOrUpdateOrDelete);

            Dictionary<PrimaryKeysPropertyColumnNameValues, T> entitiesDict;
            
            if (setByDictionary)
            {
                entitiesDict = new Dictionary<PrimaryKeysPropertyColumnNameValues, T>();
                foreach (var entity in entities)
                {
                    PrimaryKeysPropertyColumnNameValues customPKValue = new(customPK.Select(c => FastPropertyDict[c].Get(entity!)));
                    entitiesDict.Add(customPKValue, entity);
                }
            }
            else
            {
                // we will not be using the dictionary in the loop below.
                entitiesDict = null!;
            }
            
            for (int i = 0; i < NumberOfEntities; i++)
            {
                T entityToBeFilled;
                object elementFromOutputTable = entitiesWithOutputIdentity.ElementAt(i);

                if (setByDictionary)
                {
                    PrimaryKeysPropertyColumnNameValues customPKOutputValue = new(customPK.Select(c => FastPropertyDict[c].Get(elementFromOutputTable)));
                    entityToBeFilled = entitiesDict[customPKOutputValue]!;
                }
                else
                {
                    // We rely on the order:
                    entityToBeFilled = entities.ElementAt(i)!;
                }
                
                if (identifierPropertyName != null)
                {
                    var selectOnlyIdentityColumn = false;
                    var identityPropertyValue = selectOnlyIdentityColumn ? elementFromOutputTable : FastPropertyDict[identifierPropertyName].Get(elementFromOutputTable);
                    FastPropertyDict[identifierPropertyName].Set(entityToBeFilled, identityPropertyValue);
                }

                if (TimeStampColumnName != null) // timestamp/rowversion is also generated by the SqlServer so if exist should be updated as well
                {
                    string timeStampPropertyName = OutputPropertyColumnNamesDict.SingleOrDefault(a => a.Value == TimeStampColumnName).Key;
                    var timeStampPropertyValue = FastPropertyDict[timeStampPropertyName].Get(elementFromOutputTable);
                    FastPropertyDict[timeStampPropertyName].Set(entityToBeFilled, timeStampPropertyValue);
                }

                var propertiesToLoad = tableInfo.OutputPropertyColumnNamesDict.Keys.Where(a => a != identifierPropertyName && a != TimeStampColumnName && // already loaded in segmet above
                                                                                               (tableInfo.DefaultValueProperties.Contains(a) ||           // add Computed and DefaultValues
                                                                                                !tableInfo.PropertyColumnNamesDict.ContainsKey(a)));      // remove others since already have same have (could be omited)
                foreach (var outputPropertyName in propertiesToLoad)
                {
                    var propertyValue = FastPropertyDict[outputPropertyName].Get(elementFromOutputTable);
                    FastPropertyDict[outputPropertyName].Set(entityToBeFilled, propertyValue);
                }
            }
        }
        else // Clears entityList and then refills it with loaded entites from Db
        {
            entities.Clear();
            if (typeof(T) == entitiesWithOutputIdentity.FirstOrDefault()?.GetType())
            {
                ((List<T>)entities).AddRange(entitiesWithOutputIdentity.Cast<T>().ToList());
            }
            else
            {
                var entitiesObjects = entities.Cast<object>().ToList();
                entitiesObjects.AddRange(entitiesWithOutputIdentity);
            }
        }
    }
    
    internal void UpdateEntitiesIdentityByMap<T>(TableInfo tableInfo, IList<T> entities, List<IndexToGeneratedId> indexToGeneratedIds)
    {
        var identifierPropertyName = IdentityColumnName != null ? OutputPropertyColumnNamesDict.SingleOrDefault(a => a.Value == IdentityColumnName).Key // it Identity autoincrement 
                                                                : PrimaryKeysPropertyColumnNameDict.FirstOrDefault().Key;                               // or PK with default sql value

        if (BulkConfig.PreserveInsertOrder)
        {
            int countDiff = entities.Count - entitiesWithOutputIdentity.Count;
            if (countDiff > 0) // When some ommited from Merge because of TimeStamp conflict then changes are not loaded but output is set in TimeStampInfo
            {
                tableInfo.BulkConfig.TimeStampInfo = new TimeStampInfo
                {
                    NumberOfSkippedForUpdate = countDiff,
                    EntitiesOutput = entitiesWithOutputIdentity.ToList()
                };
                return;
            }

            var mappingDictionary = indexToGeneratedIds.GroupBy(x => x.OriginalIndex).ToDictionary(x => x.Key, x => x.ToList());

            for (int index = 0; index < NumberOfEntities; index++)
            {
                T entityToBeFilled = entities[index]!;
                var mapping = mappingDictionary[index];

                if (mapping.Count > 1)
                {
                    // This might happen in case of BulkInsertOrUpdate with custom UpdateBy properties, when there are multiple matching rows in table which is updated
                    // for more see: https://github.com/borisdj/EFCore.BulkExtensions/issues/1251
                    // In case of setting output identity, we cannot decide which id we should use (as there might be multiple rows in output table, which 'belong' to only one row in source table).

                       throw new BulkExtensionsException(BulkExtensionsExceptionType.CannotSetOutputIdentityForNonUniqueUpdateByProperties,
                    "Items were Inserted/Updated successfully in db, but we cannot set output identity correctly since single source row(s) matched multiple rows in db. "
                    + "Keys which matched more rows: "
                    + string.Join("\n", nonUniqueKeys.Select(x => x.ToLogString())));
                }

                
                if (identifierPropertyName != null)
                {
                    var generatedId = mapping.Single().GeneratedId;
                     FastPropertyDict[identifierPropertyName].Set(entityToBeFilled, generatedId);
                }

                if (TimeStampColumnName != null) // timestamp/rowversion is also generated by the SqlServer so if exist should be updated as well
                {
                    // TODO Fill also timestamp
                    string timeStampPropertyName = OutputPropertyColumnNamesDict.SingleOrDefault(a => a.Value == TimeStampColumnName).Key;
                    var timeStampPropertyValue = FastPropertyDict[timeStampPropertyName].Get(elementFromOutputTable);
                    FastPropertyDict[timeStampPropertyName].Set(entityToBeFilled, timeStampPropertyValue);
                }
            }
        }
    }

    // Compiled queries created manually to avoid EF Memory leak bug when using EF with dynamic SQL:
    // https://github.com/borisdj/EFCore.BulkExtensions/issues/73
    // Once the following Issue gets fixed(expected in EF 3.0) this can be replaced with code segment: DirectQuery
    // https://github.com/aspnet/EntityFrameworkCore/issues/12905
    #region CompiledQuery

    public async Task LoadOutputDataAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        bool hasIdentity = OutputPropertyColumnNamesDict.Any(a => a.Value == IdentityColumnName) ||
                           (tableInfo.HasSinglePrimaryKey && tableInfo.DefaultValueProperties.Contains(tableInfo.PrimaryKeysPropertyColumnNameDict.FirstOrDefault().Key));

        if (BulkConfig.SetOutputIdentity && hasIdentity)
        {
            if (BulkConfig.UseOriginalIndexToIdentityMappingColumn)
            {
                var map = isAsync ? await QueryOutputTableForIndexToIdMapping(context, isAsync, cancellationToken).ConfigureAwait(false)
                              : QueryOutputTableForIndexToIdMapping(context, isAsync, cancellationToken).GetAwaiter().GetResult();
                
                UpdateEntitiesIdentityByMap(tableInfo, entities, map);
            }
            else
            {
                var databaseType = SqlAdaptersMapping.GetDatabaseType(context);
                string sqlQuery = databaseType == DbServerType.SQLServer ? SqlQueryBuilder.SelectFromOutputTable(this) : SqlAdaptersMapping.DbServer(context).QueryBuilder.SelectFromOutputTable(this);
                //var entitiesWithOutputIdentity = await QueryOutputTableAsync<T>(context, sqlQuery).ToListAsync(cancellationToken).ConfigureAwait(false); // TempFIX
                var entitiesWithOutputIdentity = QueryOutputTable(context, type, sqlQuery).Cast<object>().ToList();
                //var entitiesWithOutputIdentity = (typeof(T) == type) ? QueryOutputTable<object>(context, sqlQuery).ToList() : QueryOutputTable(context, type, sqlQuery).Cast<object>().ToList();

                UpdateEntitiesIdentity(tableInfo, entities, entitiesWithOutputIdentity);
            }
        }
        
        if (BulkConfig.CalculateStats)
        {
            var mergeCounts = isAsync ? await GetMergeActionCounts(context, isAsync: true, cancellationToken).ConfigureAwait(false)
                                  : GetMergeActionCounts(context, isAsync: false, cancellationToken).GetAwaiter().GetResult();
            BulkConfig.StatsInfo = new StatsInfo
            {
                StatsNumberUpdated = mergeCounts.Updated,
                StatsNumberDeleted = mergeCounts.Deleted,
                StatsNumberInserted = mergeCounts.Inserted,
            };
        }
    }

    internal record struct IndexToGeneratedId(int OriginalIndex, object GeneratedId);
    
    internal async Task<List<IndexToGeneratedId>> QueryOutputTableForIndexToIdMapping(DbContext context, bool isAsync, CancellationToken cancellationToken)
    {
        var sql = $"SELECT [{OriginalIndexColumnName}],[{IdentityColumnName}] FROM {FullTempOutputTableName} WHERE [{OriginalIndexColumnName}] is not null;";

        return isAsync ? await GetInternalAsync().ConfigureAwait(false) : GetInternal();

        async Task<List<IndexToGeneratedId>> GetInternalAsync()
        {
            await using var command = context.Database.GetDbConnection().CreateCommand();
            command.CommandText = sql;

            if (command.Connection!.State != ConnectionState.Open)
            {
                await command.Connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }

            if (context.Database.CurrentTransaction != null)
            {
                command.Transaction = context.Database.CurrentTransaction.GetDbTransaction();
            }

            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            var results = new List<IndexToGeneratedId>();

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var index = reader.GetInt32(0);
                var id = reader.GetValue(1);
                results.Add(new IndexToGeneratedId(index, id));
            }

            return results;
        }

        List<IndexToGeneratedId> GetInternal()
        {
            using var command = context.Database.GetDbConnection().CreateCommand();
            command.CommandText = sql;

            if (command.Connection!.State != ConnectionState.Open)
            {
                command.Connection.Open();
            }

            if (context.Database.CurrentTransaction != null)
            {
                command.Transaction = context.Database.CurrentTransaction.GetDbTransaction();
            }

            using var reader = command.ExecuteReader();

            var results = new List<IndexToGeneratedId>();

            while (reader.Read())
            {
                var index = reader.GetInt32(0);
                var id = reader.GetValue(1);
                results.Add(new IndexToGeneratedId(index, id));
            }

            return results;
        }
    }

    /// <summary>
    /// Queries the output table data
    /// </summary>
    protected IEnumerable QueryOutputTable(DbContext context, Type type, string sqlQuery)
    {
        var compiled = EF.CompileQuery(GetQueryExpression(type, sqlQuery));
        var result = compiled(context);
        return result;
    }

    /*protected IEnumerable<T> QueryOutputTable<T>(DbContext context, string sqlQuery) where T : class
    {
        var compiled = EF.CompileQuery(GetQueryExpression<T>(sqlQuery));
        var result = compiled(context);
        return result;
    }*/

    /*protected IAsyncEnumerable<T> QueryOutputTableAsync<T>(DbContext context, string sqlQuery) where T : class
    {
        var compiled = EF.CompileAsyncQuery(GetQueryExpression<T>(sqlQuery));
        var result = compiled(context);
        return result;
    }*/

    /// <summary>
    /// Returns an expression for the SQL query
    /// </summary>
    public Expression<Func<DbContext, IQueryable<T>>> GetQueryExpression<T>(string sqlQuery, bool ordered = true) where T : class
    {
        Expression<Func<DbContext, IQueryable<T>>>? expression;
        if (BulkConfig.TrackingEntities) // If Else can not be replaced with Ternary operator for Expression
        {
            expression = BulkConfig.IgnoreGlobalQueryFilters ?
                (ctx) => ctx.Set<T>().FromSqlRaw(sqlQuery).IgnoreQueryFilters() :
                (ctx) => ctx.Set<T>().FromSqlRaw(sqlQuery);
        }
        else
        {
            expression = BulkConfig.IgnoreGlobalQueryFilters ?
                (ctx) => ctx.Set<T>().FromSqlRaw(sqlQuery).AsNoTracking().IgnoreQueryFilters() :
                (ctx) => ctx.Set<T>().FromSqlRaw(sqlQuery).AsNoTracking();
        }
        return ordered ?
            Expression.Lambda<Func<DbContext, IQueryable<T>>>(OrderBy(typeof(T), expression.Body, PrimaryKeysPropertyColumnNameDict.Select(a => a.Key).ToList()), expression.Parameters) :
            expression;

        // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
        //var queryOrdered = query.OrderBy(PrimaryKeys[0]);
    }

    /// <summary>
    /// Returns an expression for the SQL query
    /// </summary>
    public Expression<Func<DbContext, IEnumerable>> GetQueryExpression(Type entityType, string sqlQuery, bool ordered = true)
    {
        var parameter = Expression.Parameter(typeof(DbContext), "ctx");
        var expression = Expression.Call(parameter, "Set", new Type[] { entityType });
        expression = Expression.Call(typeof(RelationalQueryableExtensions), "FromSqlRaw", new Type[] { entityType }, expression, Expression.Constant(sqlQuery), Expression.Constant(Array.Empty<object>()));
        if (BulkConfig.TrackingEntities) // If Else can not be replaced with Ternary operator for Expression
        {
        }
        else
        {
            expression = Expression.Call(typeof(EntityFrameworkQueryableExtensions), "AsNoTracking", new Type[] { entityType }, expression);
        }
        expression = ordered ? OrderBy(entityType, expression, PrimaryKeysPropertyColumnNameDict.Select(a => a.Key).ToList()) : expression;
        return Expression.Lambda<Func<DbContext, IEnumerable>>(expression, parameter);

        // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
        //var queryOrdered = query.OrderBy(PrimaryKeys[0]);
    }

    private static MethodCallExpression OrderBy(Type entityType, Expression source, List<string> orderings)
    {
        var expression = (MethodCallExpression)source;
        ParameterExpression parameter = Expression.Parameter(entityType);
        bool firstArgOrderBy = true;
        foreach (var ordering in orderings)
        {
            PropertyInfo? property = entityType.GetProperty(ordering);
            if (property != null)
            {
                MemberExpression propertyAccess = Expression.MakeMemberAccess(parameter, property);
                LambdaExpression orderByExp = Expression.Lambda(propertyAccess, parameter);
                string methodName = firstArgOrderBy ? "OrderBy" : "ThenBy";
                expression = Expression.Call(typeof(Queryable), methodName, new Type[] { entityType, property.PropertyType }, expression, Expression.Quote(orderByExp));
                firstArgOrderBy = false;
            }
        }
        return expression;
    }
    #endregion

    // Currently not used until issue from previous segment is fixed in EFCore
    #region DirectQuery
    /*public void UpdateOutputIdentity<T>(DbContext context, IList<T> entities) where T : class
    {
        if (HasSinglePrimaryKey)
        {
            var entitiesWithOutputIdentity = QueryOutputTable<T>(context).ToList();
            UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
        }
    }

    public async Task UpdateOutputIdentityAsync<T>(DbContext context, IList<T> entities) where T : class
    {
        if (HasSinglePrimaryKey)
        {
            var entitiesWithOutputIdentity = await QueryOutputTable<T>(context).ToListAsync().ConfigureAwait(false);
            UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
        }
    }

    protected IQueryable<T> QueryOutputTable<T>(DbContext context) where T : class
    {
        string q = SqlQueryBuilderBase.SelectFromOutputTable(this);
        var query = context.Set<T>().FromSql(q);
        if (!BulkConfig.TrackingEntities)
        {
            query = query.AsNoTracking();
        }

        var queryOrdered = OrderBy(query, PrimaryKeys[0]);
        // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
        //var queryOrdered = query.OrderBy(PrimaryKeys[0]);

        return queryOrdered;
    }

    private static IQueryable<T> OrderBy<T>(IQueryable<T> source, string ordering)
    {
        Type entityType = typeof(T);
        PropertyInfo property = entityType.GetProperty(ordering);
        ParameterExpression parameter = Expression.Parameter(entityType);
        MemberExpression propertyAccess = Expression.MakeMemberAccess(parameter, property);
        LambdaExpression orderByExp = Expression.Lambda(propertyAccess, parameter);
        MethodCallExpression resultExp = Expression.Call(typeof(Queryable), "OrderBy", new Type[] { entityType, property.PropertyType }, source.Expression, Expression.Quote(orderByExp));
        var orderedQuery = source.Provider.CreateQuery<T>(resultExp);
        return orderedQuery;
    }*/
    #endregion
}

internal class PrimaryKeysPropertyColumnNameValues
{
    public List<object?> PkValues { get; }

    public PrimaryKeysPropertyColumnNameValues(IEnumerable<object?> pkValues)
    {
        PkValues = pkValues.ToList();
    }

    public override bool Equals(object? obj)
    {
        return obj is PrimaryKeysPropertyColumnNameValues values &&
               PkValues.SequenceEqual(values.PkValues);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 19;
            foreach (var value in PkValues)
            {
                hash = hash * 31 + (value == null ? 0 : value.GetHashCode());
            }
            return hash;
        }
    }
    
    public string ToLogString()
    {
        return "( " + string.Join(", ", PkValues) + " )";
    }
}
