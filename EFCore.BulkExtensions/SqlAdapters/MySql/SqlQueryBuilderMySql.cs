﻿using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.SqlClient;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

public class SqlQueryBuilderMySql : QueryBuilderExtensions
{
    /// <summary>
    /// Generates SQL query to create table copy
    /// </summary>
    public static string CreateTableCopy(TableInfo tableInfo, string existingTableName, string newTableName, bool useTempDb)
    {
        string indexMappingColumn = (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn) ? $", -1 AS {tableInfo.OriginalIndexColumnName} " : "";

        string keywordTemp = useTempDb ? "TEMPORARY " : "";
        var query = $"CREATE {keywordTemp}TABLE {newTableName} " +
                    $"SELECT * {indexMappingColumn} FROM {existingTableName} " +
                    "LIMIT 0;";
        query = query.Replace("[", "").Replace("]", "");

        return query;
    }

    /// <summary>
    /// Generates SQL query to drop table
    /// </summary>
    public static string DropTable(string tableName, bool isTempTable)
    {
        string keywordTemp = isTempTable ? "TEMPORARY " : "";
        var query = $"DROP {keywordTemp}TABLE IF EXISTS {tableName}";
        query = query.Replace("[", "").Replace("]", "");

        return query;
    }

    /// <summary>
    /// Returns a list of columns for the given table
    /// </summary>
    public static List<string> GetColumnList(TableInfo tableInfo, OperationType operationType)
    {
        var tempDict = tableInfo.PropertyColumnNamesDict;

        if (operationType == OperationType.Insert && tableInfo.PropertyColumnNamesDict.Any()) // Only OnInsert omit colums with Default values
        {
            tableInfo.PropertyColumnNamesDict = tableInfo.PropertyColumnNamesDict.Where(a => !tableInfo.DefaultValueProperties.Contains(a.Key)).ToDictionary(a => a.Key, a => a.Value);
        }

        List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();

        tableInfo.PropertyColumnNamesDict = tempDict;

        bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
        var uniquColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList().FirstOrDefault();

        if (!keepIdentity && tableInfo.HasIdentity && (operationType == OperationType.Insert || tableInfo.IdentityColumnName != uniquColumnName))
        {
            columnsList = columnsList.Where(a => a != tableInfo.IdentityColumnName).ToList();
        }

        return columnsList;
    }

    /// <summary>
    /// Generates SQL merge statement
    /// </summary>
    public static string MergeTable<T>(TableInfo tableInfo, OperationType operationType) where T : class
    {
        var columnsList = GetColumnList(tableInfo, operationType);

        if (operationType == OperationType.InsertOrUpdateOrDelete)
        {
            throw new NotImplementedException($"For MySql method {OperationType.InsertOrUpdateOrDelete} is not yet supported. Use combination of InsertOrUpdate with Read and Delete");
        }

        string query;
        var firstPrimaryKey = tableInfo.PrimaryKeysPropertyColumnNameDict.FirstOrDefault().Value;

        if (operationType == OperationType.Delete)
        {
            query = "delete A " +
                    $"FROM {tableInfo.FullTableName} AS A " +
                    $"INNER JOIN {tableInfo.FullTempTableName} B on A.{firstPrimaryKey} = B.{firstPrimaryKey}; ";
        }
        else
        {
            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", "").Replace("]", "");
            var columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
            var columnsToUpdate = columnsListEquals.Where(c => tableInfo.PropertyColumnNamesUpdateDict.ContainsValue(c)).ToList();
            var equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsToUpdate, equalsTable: "EXCLUDED").Replace("[", "").Replace("]", "");

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

            var orderBy = (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn) ? $" ORDER BY {tableInfo.OriginalIndexColumnName} " : "";

            query = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                    $"SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName} AS EXCLUDED " +
                    orderBy +
                    updateAction +
                    " ;";


            if (tableInfo.CreatedOutputTable)
            {
                if (operationType == OperationType.Insert || operationType == OperationType.InsertOrUpdate)
                {
                    var rowNum = (tableInfo.BulkConfig.UseOriginalIndexToIdentityMappingColumn) ? $" ,(row_number() OVER(ORDER BY {firstPrimaryKey} )) - 1 " : "";

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
            var textSelect = "SELECT ";
            var textFrom = " FROM";
            int startIndex = query.IndexOf(textSelect);
            var qSegment = query[startIndex..query.IndexOf(textFrom)];
            var qSegmentUpdated = qSegment;

            foreach (var mapping in sourceDestinationMappings)
            {
                var propertyFormated = $"{mapping.Value}";
                var sourceProperty = mapping.Key;

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
        var query = $"SELECT {SqlQueryBuilder.GetCommaSeparatedColumns(columnsNames)} FROM {tableInfo.FullTempOutputTableName}" +
                    (tableInfo.BulkConfig.OutputTableHasSqlActionColumn ? $" WHERE {tableInfo.SqlActionIUD} <> 'D'" : ""); // Filter out the information about deleted rows, not needed for setting output identity
        query = query.Replace("[", "").Replace("]", "");

        return query;
    }

    /// <summary>
    /// Generates SQL query to create a unique constraint
    /// </summary>
    public static string CreateUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"`{tableInfo.Schema}`.";
        var fullTableNameFormated = $@"{schemaFormated}`{tableName}`";

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        var schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        var uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        var uniqueColumnNamesComma =
            string.Join(",", uniqueColumnNames); // TODO When Column is string without defined max length, it should be UNIQUE (`Name`(255)); otherwise exception: BLOB/TEXT column 'Name' used in key specification without a key length'
        uniqueColumnNamesComma = "`" + uniqueColumnNamesComma;
        uniqueColumnNamesComma = uniqueColumnNamesComma.Replace(",", "`, `");
        var uniqueColumnNamesFormated = uniqueColumnNamesComma.TrimEnd(',');
        uniqueColumnNamesFormated = uniqueColumnNamesFormated + "`";

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"ADD CONSTRAINT `{uniqueConstrainName}` " +
                $@"UNIQUE ({uniqueColumnNamesFormated})";

        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a unique contstraint
    /// </summary>
    public static string DropUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"`{tableInfo.Schema}`.";
        var fullTableNameFormated = $@"{schemaFormated}`{tableName}`";

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        var schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        var uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"DROP INDEX `{uniqueConstrainName}`;";

        return q;
    }

    /// <summary>
    /// Generates SQL query to chaeck if a unique constrain exist
    /// </summary>
    public static string HasUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        var schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        var uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        var q = $@"SELECT DISTINCT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE " +
                $@"CONSTRAINT_TYPE = 'UNIQUE' AND CONSTRAINT_NAME = '{uniqueConstrainName}';";

        return q;
    }

    public override string RestructureForBatch(string sql, bool isDelete = false) => throw new NotImplementedException();


    public override object CreateParameter(SqlParameter sqlParameter) => throw new NotImplementedException();

    public override object Dbtype() => throw new NotImplementedException();


    public override void SetDbTypeParam(object npgsqlParameter, object dbType) => throw new NotImplementedException();
}
