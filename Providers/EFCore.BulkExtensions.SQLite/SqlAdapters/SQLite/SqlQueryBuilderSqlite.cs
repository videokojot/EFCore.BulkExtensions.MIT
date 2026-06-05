using System.Data.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions.SqlAdapters.SQLite;

/// <summary>
/// Contains a compilation of SQL queries used in EFCore.
/// </summary>
public sealed class SqlQueryBuilderSqlite : QueryBuilderExtensions
{
    /// <summary>
    /// Generates SQL query to retrieve the last inserted row id
    /// </summary>
    public static string SelectLastInsertRowId()
    {
        string query = "SELECT last_insert_rowid();";

        return query;
    }

    // In Sqlite if table has AutoIncrement then InsertOrUpdate is not supported in one call,
    // we can not simultaneously Insert without PK(being 0,0,...) and Update with PK(1,2,...), separate calls Insert, Update are required.
    /// <summary>
    /// Generates SQL query to insert data into table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <param name="tableName"></param>
    public static string InsertIntoTable(TableInfo tableInfo, OperationType operationType, string? tableName = null)
    {
        tableName ??= tableInfo.InsertToTempTable ? tableInfo.TempTableName : tableInfo.TableName;

        Dictionary<string, string> tempDict = tableInfo.PropertyColumnNamesDict;
        if (operationType == OperationType.Insert && tableInfo.PropertyColumnNamesDict.Any()) // Only OnInsert omit colums with Default values
        {
            tableInfo.PropertyColumnNamesDict = tableInfo.PropertyColumnNamesDict.Where(a => !tableInfo.DefaultValueProperties.Contains(a.Key)).ToDictionary(a => a.Key, a => a.Value);
        }

        List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
        List<string> propertiesList = tableInfo.PropertyColumnNamesDict.Keys.ToList();

        bool keepIdentity = tableInfo.BulkConfig.BulkCopyOptions.HasFlag(BulkCopyOptions.KeepIdentity);
        if (!tableInfo.InsertToTempTable && !keepIdentity && tableInfo.HasIdentity)
        {
            string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
            columnsList = columnsList.Where(a => a != tableInfo.IdentityColumnName).ToList();
            propertiesList = propertiesList.Where(a => a != identityPropertyName).ToList();
        }

        string commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList);
        string commaSeparatedColumnsParams = SqlQueryBuilder.GetCommaSeparatedColumns(propertiesList, "@").Replace("[", "").Replace("]", "").Replace(".", "_");

        string q = $"INSERT INTO [{tableName}] " +
                $"({commaSeparatedColumns}) " +
                $"VALUES ({commaSeparatedColumnsParams})";

        if (operationType == OperationType.InsertOrUpdate)
        {
            List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Select(k => tableInfo.PropertyColumnNamesDict[k.Key]).ToList();
            string commaSeparatedPrimaryKeys = SqlQueryBuilder.GetCommaSeparatedColumns(primaryKeys);
            string commaSeparatedColumnsEquals = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList, equalsTable: "", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = .[", "] = @").Replace(".", "_");
            string commaANDSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_");

            q += $" ON CONFLICT({commaSeparatedPrimaryKeys}) DO UPDATE" +
                 $" SET {commaSeparatedColumnsEquals}" +
                 $" WHERE {commaANDSeparatedPrimaryKeys}";

            if (tableInfo.BulkConfig.OnConflictUpdateWhereSql != null)
            {
                q += $" AND {tableInfo.BulkConfig.OnConflictUpdateWhereSql($"[{tableName}]", "excluded")}";
            }
        }

        tableInfo.PropertyColumnNamesDict = tempDict;

        string result = q + ";";

        return result;
    }


    /// <summary>
    /// Generates SQL query to update table record data
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="tableName"></param>
    public static string UpdateSetTable(TableInfo tableInfo, string? tableName = null)
    {
        tableName ??= tableInfo.TableName;
        List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
        List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Select(k => tableInfo.PropertyColumnNamesDict[k.Key]).ToList();
        string commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_"); ;
        string commaSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_"); ;

        string q = $"UPDATE [{tableName}] " +
                $"SET {commaSeparatedColumns} " +
                $"WHERE {commaSeparatedPrimaryKeys};";
        return q;
    }

    /// <summary>
    /// Generates SQL query to delete from table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="tableName"></param>
    public static string DeleteFromTable(TableInfo tableInfo, string? tableName = null)
    {
        tableName ??= tableInfo.TableName;
        List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Select(k => tableInfo.PropertyColumnNamesDict[k.Key]).ToList();
        string commaSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_");

        string q = $"DELETE FROM [{tableName}] " +
                $"WHERE {commaSeparatedPrimaryKeys};";
        return q;
    }

    /// <summary>
    /// Generates SQL query to create table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    public static string CreateTableCopy(string existingTableName, string newTableName) // Used for BulkRead
    {
        string q = $"CREATE TABLE {newTableName} AS SELECT * FROM {existingTableName} WHERE 0;";

        return q;
    }

    /// <summary>
    /// Generates SQL query to drop table
    /// </summary>
    /// <param name="tableName"></param>
    public static string DropTable(string tableName)
    {
        string q = $"DROP TABLE IF EXISTS {tableName}";
        return q;
    }

    /// <inheritdoc/>
    public override object CreateParameter(DbParameter dbParameter)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override object Dbtype()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override string RestructureForBatch(string sql, bool isDelete = false)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override string SelectFromOutputTable(TableInfo tableInfo)
    {
        string query = SqlQueryBuilder.SelectFromOutputTable(tableInfo);

        return query;
    }

    /// <inheritdoc/>
    public override void SetDbTypeParam(object npgsqlParameter, object dbType)
    {
        throw new NotImplementedException();
    }
}
