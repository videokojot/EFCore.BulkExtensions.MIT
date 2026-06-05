using System;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

public sealed class SqlQueryBuilderMySql : QueryBuilderExtensions
{
    /// <summary> Generates SQL query to create table copy </summary>
    public static string CreateTableCopy(TableInfo tableInfo, string existingTableName, string newTableName, bool useTempDb)
    {
        string indexMappingColumn = (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn) ? $", -1 AS {tableInfo.OriginalIndexColumnName} " : "";

        string keywordTemp = useTempDb ? "TEMPORARY " : "";
        string query = $"CREATE {keywordTemp}TABLE {newTableName} " +
                       $"SELECT * {indexMappingColumn} FROM {existingTableName} " +
                       "LIMIT 0;";
        query = query.Replace("[", "").Replace("]", "");

        return query;
    }

    /// <summary> Generates SQL query to drop table </summary>
    public static string DropTable(string tableName, bool isTempTable)
    {
        string keywordTemp = isTempTable ? "TEMPORARY " : "";
        string query = $"DROP {keywordTemp}TABLE IF EXISTS {tableName}";
        query = query.Replace("[", "").Replace("]", "");

        return query;
    }

    /// <summary> Returns a list of columns for the given table </summary>
    public static List<string> GetColumnList(TableInfo tableInfo, OperationType operationType)
    {
        Dictionary<string, string> tempDict = tableInfo.PropertyColumnNamesDict;

        if (operationType == OperationType.Insert && tableInfo.PropertyColumnNamesDict.Any()) // Only OnInsert omit colums with Default values
        {
            tableInfo.PropertyColumnNamesDict = tableInfo.PropertyColumnNamesDict.Where(a => !tableInfo.DefaultValueProperties.Contains(a.Key)).ToDictionary(a => a.Key, a => a.Value);
        }

        List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();

        tableInfo.PropertyColumnNamesDict = tempDict;

        bool keepIdentity = tableInfo.BulkConfig.BulkCopyOptions.HasFlag(BulkCopyOptions.KeepIdentity);
        string? uniquColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList().FirstOrDefault();

        if (!keepIdentity && tableInfo.HasIdentity && (operationType == OperationType.Insert || tableInfo.IdentityColumnName != uniquColumnName))
        {
            columnsList = columnsList.Where(a => a != tableInfo.IdentityColumnName).ToList();
        }

        return columnsList;
    }

    /// <summary> Generates SQL merge statement </summary>
    public static string MergeTable<T>(TableInfo tableInfo, OperationType operationType) where T : class
    {
        List<string> columnsList = GetColumnList(tableInfo, operationType);

        if (operationType == OperationType.InsertOrUpdateOrDelete)
        {
            throw new NotImplementedException($"For MySql method {OperationType.InsertOrUpdateOrDelete} is not yet supported. Use combination of InsertOrUpdate with Read and Delete");
        }

        string query;
        string firstPrimaryKey = tableInfo.PrimaryKeysPropertyColumnNameDict.FirstOrDefault().Value;

        if (operationType == OperationType.Delete)
        {
            query = "delete A " +
                    $"FROM {tableInfo.FullTableName} AS A " +
                    $"INNER JOIN {tableInfo.FullTempTableName} B on A.{firstPrimaryKey} = B.{firstPrimaryKey}; ";
        }
        else
        {
            string commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", "").Replace("]", "");
            List<string> columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
            List<string> columnsToUpdate = columnsListEquals.Where(c => tableInfo.PropertyColumnNamesUpdateDict.ContainsValue(c)).ToList();
            string equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsToUpdate, equalsTable: "EXCLUDED").Replace("[", "").Replace("]", "");

            string updateAction;

            if (string.IsNullOrEmpty(equalsColumns) || operationType == OperationType.Insert)
            {
                // This is 'do nothing' on update:
                updateAction = $"ON DUPLICATE KEY UPDATE {firstPrimaryKey} = EXCLUDED.{firstPrimaryKey}";
            }
            else
            {
                updateAction = $"ON DUPLICATE KEY UPDATE {equalsColumns}";
            }

            string orderBy = (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn) ? $" ORDER BY {tableInfo.OriginalIndexColumnName} " : "";

            query = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                    $"SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName} AS EXCLUDED " +
                    orderBy +
                    updateAction +
                    " ;";


            if (tableInfo.CreatedOutputTable)
            {
                if (operationType == OperationType.Insert || operationType == OperationType.InsertOrUpdate)
                {
                    string rowNum = (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn) ? $" ,(row_number() OVER(ORDER BY {firstPrimaryKey} )) - 1 " : "";

                    query += $"INSERT INTO {tableInfo.FullTempOutputTableName} " +
                             $"SELECT * {rowNum}  FROM {tableInfo.FullTableName} " +
                             $"WHERE {firstPrimaryKey} >= LAST_INSERT_ID() " +
                             $"AND {firstPrimaryKey} < LAST_INSERT_ID() + row_count(); ";
                }
                else if (operationType == OperationType.Update)
                {
                    query += $"INSERT INTO {tableInfo.FullTempOutputTableName} " +
                             $"SELECT * FROM {tableInfo.FullTempTableName} ";
                }

                // This also is commented in original code, just ignoring the ids of updated values.
                // So the set output identity just does not work on MySql.
                // See: https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/90

                // if (operationType == OperationType.InsertOrUpdate)
                // {
                //     // We cannot refer to FullTempOutputTableName twice in one query. See:
                //     // https://dev.mysql.com/doc/refman/8.0/en/temporary-table-problems.html
                //     // So we need to find a way to 
                //     query += $"INSERT INTO {tableInfo.FullTempOutputTableName} " +
                //              $"SELECT A.* FROM {tableInfo.FullTempTableName} A " +
                //              $"LEFT OUTER JOIN {tableInfo.FullTempOutputTableName} B " +
                //              $" ON A.{firstPrimaryKey} = B.{firstPrimaryKey} " +
                //              $"WHERE  B.{firstPrimaryKey} IS NULL; ";
                // }
            }
        }

        query = query.Replace("[", "").Replace("]", "");

        Dictionary<string, string>? sourceDestinationMappings = tableInfo.BulkConfig.CustomSourceDestinationMappingColumns;

        if (tableInfo.BulkConfig.CustomSourceTableName != null && sourceDestinationMappings != null && sourceDestinationMappings.Count > 0)
        {
            string textSelect = "SELECT ";
            string textFrom = " FROM";
            int startIndex = query.IndexOf(textSelect);
            string qSegment = query[startIndex..query.IndexOf(textFrom)];
            string qSegmentUpdated = qSegment;

            foreach (KeyValuePair<string, string> mapping in sourceDestinationMappings)
            {
                string propertyFormated = $"{mapping.Value}";
                string sourceProperty = mapping.Key;

                if (qSegment.Contains(propertyFormated))
                {
                    qSegmentUpdated = qSegmentUpdated.Replace(propertyFormated, $"{sourceProperty}");
                }
            }

            if (qSegment != qSegmentUpdated)
            {
                query = query.Replace(qSegment, qSegmentUpdated);
            }
        }

        return query;
    }

    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    public override string SelectFromOutputTable(TableInfo tableInfo)
    {
        List<string> columnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
        string query = $"SELECT {SqlQueryBuilder.GetCommaSeparatedColumns(columnsNames)} FROM {tableInfo.FullTempOutputTableName}" +
                       (tableInfo.BulkConfig.OutputTableHasSqlActionColumn ? $" WHERE {tableInfo.SqlActionIUD} <> 'D'" : ""); // Filter out the information about deleted rows, not needed for setting output identity
        query = query.Replace("[", "").Replace("]", "");

        return query;
    }

    /// <summary>
    /// Generates SQL query to create a unique constraint
    /// </summary>
    public static string CreateUniqueConstrain(TableInfo tableInfo)
    {
        string? tableName = tableInfo.TableName;
        string schemaFormated = tableInfo.Schema == null ? "" : $@"`{tableInfo.Schema}`.";
        string fullTableNameFormated = $@"{schemaFormated}`{tableName}`";

        List<string> uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        string schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        string uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        string uniqueColumnNamesComma =
            string.Join(",", uniqueColumnNames); // TODO When Column is string without defined max length, it should be UNIQUE (`Name`(255)); otherwise exception: BLOB/TEXT column 'Name' used in key specification without a key length'
        uniqueColumnNamesComma = "`" + uniqueColumnNamesComma;
        uniqueColumnNamesComma = uniqueColumnNamesComma.Replace(",", "`, `");
        string uniqueColumnNamesFormated = uniqueColumnNamesComma.TrimEnd(',');
        uniqueColumnNamesFormated = uniqueColumnNamesFormated + "`";

        string q = $@"ALTER TABLE {fullTableNameFormated} " +
                   $@"ADD CONSTRAINT `{uniqueConstrainName}` " +
                   $@"UNIQUE ({uniqueColumnNamesFormated})";

        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a unique contstraint
    /// </summary>
    public static string DropUniqueConstrain(TableInfo tableInfo)
    {
        string? tableName = tableInfo.TableName;
        string schemaFormated = tableInfo.Schema == null ? "" : $@"`{tableInfo.Schema}`.";
        string fullTableNameFormated = $@"{schemaFormated}`{tableName}`";

        List<string> uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        string schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        string uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        string q = $@"ALTER TABLE {fullTableNameFormated} " +
                   $@"DROP INDEX `{uniqueConstrainName}`;";

        return q;
    }

    /// <summary> Generates SQL query to check if a unique constraint exist </summary>
    public static string HasUniqueConstrain(TableInfo tableInfo)
    {
        string? tableName = tableInfo.TableName;

        List<string> uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        string schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        string uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        string q = $@"SELECT DISTINCT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE " +
                   $@"CONSTRAINT_TYPE = 'UNIQUE' AND CONSTRAINT_NAME = '{uniqueConstrainName}';";

        return q;
    }

    public override string RestructureForBatch(string sql, bool isDelete = false) => throw new NotImplementedException();


    public override object CreateParameter(DbParameter dbParameter) => throw new NotImplementedException();

    public override object Dbtype() => throw new NotImplementedException();


    public override void SetDbTypeParam(object npgsqlParameter, object dbType) => throw new NotImplementedException();
}