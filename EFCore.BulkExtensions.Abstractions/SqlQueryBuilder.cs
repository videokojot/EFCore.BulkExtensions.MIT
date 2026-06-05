using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
namespace EFCore.BulkExtensions;

/// <summary>
/// Contains a compilation of SQL queries used in EFCore.
/// </summary>
public static class SqlQueryBuilder
{
    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    public static string SelectFromOutputTable(TableInfo tableInfo)
    {
        List<string> columnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
        string commaSeparatedColumns = GetCommaSeparatedColumns(columnsNames);
        string fullTempOutputTableName = tableInfo.FullTempOutputTableName;
        string outputFilter = tableInfo.BulkConfig.OutputTableHasSqlActionColumn ? $" WHERE {tableInfo.SqlActionIUD} <> 'D'" : "";
        string q = $"SELECT {commaSeparatedColumns} FROM {fullTempOutputTableName}{outputFilter}";
        // Filter out the information about deleted rows (since we do not care about them when setting the output identity etc.)
        return q;
    }

    /// <summary>
    /// Generates SQL query to drop table
    /// </summary>
    /// <returns></returns>
    public static string DropTable(string tableName, bool isTempTable)
    {
        string q;

        if (isTempTable)
        {
            string tableNameSuffix = tableName.Split('#')[1];
            q = $"IF OBJECT_ID ('tempdb..[#{tableNameSuffix}', 'U') IS NOT NULL DROP TABLE {tableName}";
        }
        else
        {
            q = $"IF OBJECT_ID ('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName}";
        }

        return q;
    }

    /// <summary>
    /// Generates SQL query to join table
    /// </summary>
    public static string SelectJoinTable(TableInfo tableInfo)
    {
        string sourceTable = tableInfo.FullTableName;
        string joinTable = tableInfo.FullTempTableName;
        List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();
        List<string> selectByPropertyNames = tableInfo.PropertyColumnNamesDict.Where(a => tableInfo.PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Key)).Select(a => a.Value).ToList();
        string commaSeparatedColumns = GetCommaSeparatedColumns(columnsNames, "S");
        string andSeparatedColumns = GetANDSeparatedColumns(selectByPropertyNames, "S", "J", tableInfo.UpdateByPropertiesAreNullable);
        string q = $"SELECT {commaSeparatedColumns} FROM {sourceTable} AS S " +
                $"JOIN {joinTable} AS J " +
                $"ON {andSeparatedColumns}";

        return q;
    }

    public static (string sql, IEnumerable<object> parameters) MergeTable<T>(DbContext? context, TableInfo tableInfo, OperationType operationType, IEnumerable<string>? entityPropertyWithDefaultValue = default) where T : class
    {
        List<object> parameters = new List<object>();
        string targetTable = tableInfo.FullTableName;
        string sourceTable = tableInfo.FullTempTableName;
        bool keepIdentity = tableInfo.BulkConfig.BulkCopyOptions.HasFlag(BulkCopyOptions.KeepIdentity);
        List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Key)).Select(a => a.Value).ToList();
        List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();
        List<string> columnsNamesOnCompare = tableInfo.PropertyColumnNamesCompareDict.Values.ToList();
        List<string> columnsNamesOnUpdate = tableInfo.PropertyColumnNamesUpdateDict.Values.ToList();
        List<string> outputColumnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
        List<string> nonIdentityColumnsNames = columnsNames.Where(a => !a.Equals(tableInfo.IdentityColumnName, StringComparison.OrdinalIgnoreCase)).ToList();
        List<string> compareColumnNames = columnsNamesOnCompare.Where(a => !a.Equals(tableInfo.IdentityColumnName, StringComparison.OrdinalIgnoreCase)).ToList();
        List<string> updateColumnNames = columnsNamesOnUpdate.Where(a => !a.Equals(tableInfo.IdentityColumnName, StringComparison.OrdinalIgnoreCase)).ToList();
        List<string> insertColumnsNames = (tableInfo.HasIdentity && !keepIdentity) ? nonIdentityColumnsNames : columnsNames;

        if (tableInfo.DefaultValueProperties.Any()) // Properties with DefaultValue exclude OnInsert but keep OnUpdate
        {
            List<string> defaults = insertColumnsNames.Where(a => tableInfo.DefaultValueProperties.Contains(a)).ToList();
            //If the entities assign value to properties with default value, don't skip this property 
            if (entityPropertyWithDefaultValue != default)
            {
                defaults = defaults.Where(x => entityPropertyWithDefaultValue.Contains(x)).ToList();
            }
            insertColumnsNames = insertColumnsNames.Where(a => !defaults.Contains(a)).ToList();
        }
        
        if (tableInfo.BulkConfig.PreserveInsertOrder)
        {
            int numberOfEntities = tableInfo.BulkConfig.CustomSourceTableName == null
                ? tableInfo.NumberOfEntities
                : int.MaxValue;
            string orderBy = (primaryKeys.Count == 0) ? string.Empty : $"ORDER BY {GetCommaSeparatedColumns(primaryKeys)}";
            sourceTable = $"(SELECT TOP {numberOfEntities} * FROM {sourceTable} {orderBy})";
        }

        string textWITH_HOLDLOCK = tableInfo.BulkConfig.WithHoldlock ? " WITH (HOLDLOCK)" : string.Empty;
        string setOutputIdentityPrefix = tableInfo.BulkConfig.SetOutputIdentity ? "DECLARE @temp INT; \n" : string.Empty;
        string andSeparatedPrimaryKeys = GetANDSeparatedColumns(primaryKeys, "T", "S", tableInfo.UpdateByPropertiesAreNullable);
        string q = setOutputIdentityPrefix +
                $"MERGE {targetTable}{textWITH_HOLDLOCK} AS T " +
                $"USING {sourceTable} AS S " +
                $"ON {andSeparatedPrimaryKeys}";
        q += (primaryKeys.Count == 0) ? "1=0" : string.Empty;

        if (operationType == OperationType.Insert || operationType == OperationType.InsertOrUpdate || operationType == OperationType.InsertOrUpdateOrDelete)
        {
            string insertColumns = GetCommaSeparatedColumns(insertColumnsNames);
            string insertValues = GetCommaSeparatedColumns(insertColumnsNames, "S");
            q += $" WHEN NOT MATCHED BY TARGET " +
                 $"THEN INSERT ({insertColumns})" +
                 $" VALUES ({insertValues})";
        }

        q = q.Replace("INSERT () VALUES ()", "INSERT DEFAULT VALUES"); // case when table has only one column that is Identity

        if (operationType == OperationType.Update || operationType == OperationType.InsertOrUpdate || operationType == OperationType.InsertOrUpdateOrDelete)
        {
            if (updateColumnNames.Count == 0 && operationType == OperationType.Update)
            {
                throw new InvalidBulkConfigException($"'Bulk{operationType}' operation can not have zero columns to update.");
            }
            else if (updateColumnNames.Count > 0)
            {
                string existsClause = tableInfo.BulkConfig.OmitClauseExistsExcept || tableInfo.HasSpatialType ? string.Empty :
                      $" AND EXISTS (SELECT {GetCommaSeparatedColumns(compareColumnNames, "S")}" +
                      $" EXCEPT SELECT {GetCommaSeparatedColumns(compareColumnNames, "T")})";
                string timeStampClause = !tableInfo.BulkConfig.DoNotUpdateIfTimeStampChanged || tableInfo.TimeStampColumnName == null ? string.Empty :
                      $" AND S.[{tableInfo.TimeStampColumnName}] = T.[{tableInfo.TimeStampColumnName}]";
                string onConflictClause = tableInfo.BulkConfig.OnConflictUpdateWhereSql != null ? $" AND {tableInfo.BulkConfig.OnConflictUpdateWhereSql("T", "S")}" : string.Empty;
                string updateSetColumns = GetCommaSeparatedColumns(updateColumnNames, "T", "S");
                q += $" WHEN MATCHED" + existsClause + timeStampClause + onConflictClause + $" THEN UPDATE SET {updateSetColumns}";
            }
            else if (updateColumnNames.Count == 0 && tableInfo.BulkConfig.SetOutputIdentity)
            {
                // Do nothing operation (set dummy value), but we need to have 'WHEN MATCHED' clause so we will get the output ids in the output table.
                q += " WHEN MATCHED THEN UPDATE SET @temp = 1 "; 
            }
        }

        if (operationType == OperationType.InsertOrUpdateOrDelete)
        {
            string deleteSearchCondition = string.Empty;

            if (tableInfo.BulkConfig.SynchronizeFilter != null)
            {
                if (context is null)
                {
                    throw new ArgumentNullException(nameof(context));
                }
                Expression<Func<T, bool>> synchronizeFilter = (Expression<Func<T, bool>>)tableInfo.BulkConfig.SynchronizeFilter;
                IQueryable<T> querable = context.Set<T>()
                    .IgnoreQueryFilters()
                    .IgnoreAutoIncludes()
                    .Where(synchronizeFilter);
                (string Sql, string TableAlias, string TableAliasSufixAs, string TopStatement, string LeadingComments, IEnumerable<object> InnerParameters) = BatchUtil.GetBatchSql(querable, context, false);
                string whereClause = $"{Environment.NewLine}WHERE ";
                int wherePos = Sql.IndexOf(whereClause, StringComparison.OrdinalIgnoreCase);

                if (wherePos > 0)
                {
                    int whereClauseLength = whereClause.Length;
                    string sqlWhere = Sql[(wherePos + whereClauseLength)..];
                    string tableAliasPrefix = $"[{TableAlias}].";
                    sqlWhere = sqlWhere.Replace(tableAliasPrefix, string.Empty);

                    deleteSearchCondition = " AND " + sqlWhere;
                    parameters.AddRange(InnerParameters);
                }
                else
                {
                    throw new InvalidBulkConfigException($"'Bulk{operationType}' SynchronizeFilter expression can not be translated to SQL");
                }
            }

            q += " WHEN NOT MATCHED BY SOURCE" + deleteSearchCondition + " THEN DELETE";
        }

        if (operationType == OperationType.Delete)
        {
            q += " WHEN MATCHED THEN DELETE";
        }
        
        if (tableInfo.CreatedOutputTable)
        {
            string commaSeparatedColumnsNames;

            if (operationType == OperationType.InsertOrUpdateOrDelete || operationType == OperationType.Delete)
            {
                commaSeparatedColumnsNames = string.Join(", ", outputColumnsNames.Select(x => $"COALESCE(INSERTED.[{x}], DELETED.[{x}])"));
            }
            else
            {
                commaSeparatedColumnsNames = GetCommaSeparatedColumns(outputColumnsNames, "INSERTED");
            }
            
            string mergeActionColumn = "";
            string outputIndexColumn = "";

            if (tableInfo.BulkConfig.OutputTableHasSqlActionColumn)
            {
                mergeActionColumn = " ,SUBSTRING($action, 1, 1)";
            }
            
            if (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn)
            {
                outputIndexColumn = $" ,S.[{tableInfo.OriginalIndexColumnName}]";
            }
            
            string fullTempOutputTableName = tableInfo.FullTempOutputTableName;
            q += $" OUTPUT {commaSeparatedColumnsNames}" + mergeActionColumn + outputIndexColumn +
                 $" INTO {fullTempOutputTableName}";
        }
        q += ";";

        Dictionary<string, string> sourceDestinationMappings = tableInfo.BulkConfig.CustomSourceDestinationMappingColumns ?? new Dictionary<string, string>();

        if (tableInfo.BulkConfig.CustomSourceTableName != null
            && sourceDestinationMappings != null
            && sourceDestinationMappings.Count > 0)
        {
            string textOrderBy = "ORDER BY ";
            string textAsS = " AS S";
            int startIndex = q.IndexOf(textOrderBy);
            int textAsSIndex = q.IndexOf(textAsS);
            string qSegment = q[startIndex..textAsSIndex];
            string qSegmentUpdated = qSegment;

            foreach (KeyValuePair<string, string> mapping in sourceDestinationMappings)
            {
                string propertySourceFormated = $"S.[{mapping.Value}]";
                string propertyFormated = $"[{mapping.Value}]";
                string sourceProperty = mapping.Key;

                if (q.Contains(propertySourceFormated))
                {
                    string replacement = $"S.[{sourceProperty}]";
                    q = q.Replace(propertySourceFormated, replacement);
                }

                if (qSegment.Contains(propertyFormated))
                {
                    string segmentReplacement = $"[{sourceProperty}]";
                    qSegmentUpdated = qSegmentUpdated.Replace(propertyFormated, segmentReplacement);
                }
            }

            if (qSegment != qSegmentUpdated)
            {
                q = q.Replace(qSegment, qSegmentUpdated);
            }
        }

        (string sql, IEnumerable<object> parameters) result = (q, parameters);
        return result;
    }


    // propertColumnsNamesDict used with Sqlite for @parameter to be save from non valid charaters ('', '!', ...) that are allowed as column Names in Sqlite

    /// <summary>
    /// Generates SQL query to get comma seperated column
    /// </summary>
    public static string GetCommaSeparatedColumns(List<string> columnsNames, string? prefixTable = null, string? equalsTable = null, Dictionary<string, string>? propertColumnsNamesDict = null)
    {
        prefixTable += (prefixTable != null && prefixTable != "@") ? "." : "";
        equalsTable += (equalsTable != null && equalsTable != "@") ? "." : "";

        string commaSeparatedColumns = "";

        foreach (string columnName in columnsNames)
        {
            string equalsParameter = propertColumnsNamesDict == null ? columnName : propertColumnsNamesDict.SingleOrDefault(a => a.Value == columnName).Key;
            commaSeparatedColumns += prefixTable != "" ? $"{prefixTable}[{columnName}]" : $"[{columnName}]";
            commaSeparatedColumns += equalsTable != "" ? $" = {equalsTable}[{equalsParameter}]" : "";
            commaSeparatedColumns += ", ";
        }

        if (commaSeparatedColumns != "")
        {
            commaSeparatedColumns = commaSeparatedColumns.Remove(commaSeparatedColumns.Length - 2, 2); // removes last excess comma and space: ", "
        }

        return commaSeparatedColumns;
    }

    /// <summary>
    /// Generates SQL query to seperate columns
    /// </summary>
    public static string GetANDSeparatedColumns(List<string> columnsNames, string? prefixTable = null, string? equalsTable = null, bool updateByPropertiesAreNullable = false, Dictionary<string, string>? propertColumnsNamesDict = null)
    {
        string commaSeparatedColumns = GetCommaSeparatedColumns(columnsNames, prefixTable, equalsTable, propertColumnsNamesDict);

        if (updateByPropertiesAreNullable)
        {
            string[] columns = commaSeparatedColumns.Split(',');
            string commaSeparatedColumnsNullable = String.Empty;

            foreach (string column in columns)
            {
                string[] columnTS = column.Split('=');
                string columnT = columnTS[0].Trim();
                string columnS = columnTS[1].Trim();
                string columnTrimmed = column.Trim();
                string columnNullable = $"({columnTrimmed} OR ({columnT} IS NULL AND {columnS} IS NULL))";
                commaSeparatedColumnsNullable += columnNullable + ", ";
            }

            if (commaSeparatedColumns != "")
            {
                commaSeparatedColumnsNullable = commaSeparatedColumnsNullable.Remove(commaSeparatedColumnsNullable.Length - 2, 2);
            }
            commaSeparatedColumns = commaSeparatedColumnsNullable;
        }

        string ANDSeparatedColumns = commaSeparatedColumns.Replace(",", " AND");
        string result = ANDSeparatedColumns;
        return result;
    }
}
