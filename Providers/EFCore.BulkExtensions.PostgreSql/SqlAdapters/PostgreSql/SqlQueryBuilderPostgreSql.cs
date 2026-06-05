using System;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions.SqlAdapters.PostgreSql;

/// <summary>
/// Contains a list of methods to generate SQL queries required by EFCore
/// </summary>
public sealed class SqlQueryBuilderPostgreSql : QueryBuilderExtensions
{
    /// <summary>
    /// Generates SQL query to create table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    /// <param name="useTempDb"></param>
    public static string CreateTableCopy(string existingTableName, string newTableName, bool useTempDb)
    {
        string keywordTEMP = useTempDb ? "TEMP " : ""; // "TEMP " or "TEMPORARY "
        string q = $"CREATE {keywordTEMP}TABLE {newTableName} " +
                $"AS TABLE {existingTableName} " +
                $"WITH NO DATA;";
        q = q.Replace("[", @"""").Replace("]", @"""");

        return q;
    }

    /// <summary>
    /// Generates SQL to copy table columns from STDIN 
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <param name="tableName"></param>
    public static string InsertIntoTable(TableInfo tableInfo, OperationType operationType, string? tableName = null)
    {
        tableName ??= tableInfo.InsertToTempTable ? tableInfo.FullTempTableName : tableInfo.FullTableName;
        tableName = tableName.Replace("[", @"""").Replace("]", @"""");

        List<string> columnsList = GetColumnList(tableInfo, operationType);

        string commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", @"""").Replace("]", @"""");

        string q = $"COPY {tableName} " +
                $"({commaSeparatedColumns}) " +
                $"FROM STDIN (FORMAT BINARY)";

        string result = q + ";";

        return result;
    }

    /// <summary>
    /// Generates SQL merge statement
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <exception cref="NotImplementedException"></exception>
    public static string MergeTable<T>(TableInfo tableInfo, OperationType operationType) where T : class
    {
        List<string> columnsList = GetColumnList(tableInfo, operationType);

        if (operationType == OperationType.InsertOrUpdateOrDelete)
        {
            throw new NotImplementedException($"For Postgres method {OperationType.InsertOrUpdateOrDelete} is not yet supported. Use combination of InsertOrUpdate with Read and Delete");
        }

        string q;

        if (operationType == OperationType.Read)
        {
            string readByColumns = SqlQueryBuilder.GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList()); //, tableInfo.FullTableName, tableInfo.FullTempTableName

            q = $"SELECT {tableInfo.FullTableName}.* FROM {tableInfo.FullTableName} " +
                $"JOIN {tableInfo.FullTempTableName} " +
                $"USING ({readByColumns})"; //$"ON ({tableInfo.FullTableName}.readByColumns = {tableInfo.FullTempTableName}.readByColumns);";
        }
        else if (operationType == OperationType.Delete)
        {
            string deleteByColumns = SqlQueryBuilder.GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList(), tableInfo.FullTableName, tableInfo.FullTempTableName);
            deleteByColumns = deleteByColumns.Replace(",", " AND");
            deleteByColumns = deleteByColumns.Replace("[", @"""").Replace("]", @"""");

            q = $"DELETE FROM {tableInfo.FullTableName} " +
                $"USING {tableInfo.FullTempTableName} " +
                $@"WHERE {deleteByColumns}";
        }
        else
        {
            string commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", @"""").Replace("]", @"""");

            string updateByColumns = SqlQueryBuilder.GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList()).Replace("[", @"""").Replace("]", @"""");

            List<string> columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
            List<string> columnsToUpdate = columnsListEquals.Where(c => tableInfo.PropertyColumnNamesUpdateDict.ContainsValue(c)).ToList();
            string equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsToUpdate, equalsTable: "EXCLUDED").Replace("[", @"""").Replace("]", @"""");

            bool doNothingOnUpdate = columnsToUpdate.Count == 0 || string.IsNullOrWhiteSpace(equalsColumns);


            q = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                $"(SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName}) " +
                $"ON CONFLICT ({updateByColumns}) " +
                (doNothingOnUpdate
                     ? "DO NOTHING"
                     : $"DO UPDATE SET {equalsColumns}");

            if (tableInfo.BulkConfig.OnConflictUpdateWhereSql != null)
            {
                string fullTableNameFormatted = tableInfo.FullTableName.Replace("[", @"""").Replace("]", @"""");
                string onConflictWhereSql = tableInfo.BulkConfig.OnConflictUpdateWhereSql(fullTableNameFormatted, "EXCLUDED");
                q += $" WHERE {onConflictWhereSql}";
            }

            if (tableInfo.CreatedOutputTable)
            {
                List<string> allColumnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
                string commaSeparatedColumnsNames = SqlQueryBuilder.GetCommaSeparatedColumns(allColumnsList).Replace("[", @"""").Replace("]", @"""");
                q += $" RETURNING {commaSeparatedColumnsNames}";
            }
        }

        q = q.Replace("[", @"""").Replace("]", @"""");
        q += ";";

        Dictionary<string, string>? sourceDestinationMappings = tableInfo.BulkConfig.CustomSourceDestinationMappingColumns;

        if (tableInfo.BulkConfig.CustomSourceTableName != null && sourceDestinationMappings != null && sourceDestinationMappings.Count > 0)
        {
            string textSelect = "SELECT ";
            string textFrom = " FROM";
            int startIndex = q.IndexOf(textSelect);
            string qSegment = q[startIndex..q.IndexOf(textFrom)];
            string qSegmentUpdated = qSegment;

            foreach (KeyValuePair<string, string> mapping in sourceDestinationMappings)
            {
                string propertyFormated = $@"""{mapping.Value}""";
                string sourceProperty = mapping.Key;

                if (qSegment.Contains(propertyFormated))
                {
                    qSegmentUpdated = qSegmentUpdated.Replace(propertyFormated, $@"""{sourceProperty}""");
                }
            }

            if (qSegment != qSegmentUpdated)
            {
                q = q.Replace(qSegment, qSegmentUpdated);
            }
        }

        return q;
    }

    /// <summary>
    /// Returns a list of columns for the given table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    public static List<string> GetColumnList(TableInfo tableInfo, OperationType operationType)
    {
        Dictionary<string, string> tempDict = tableInfo.PropertyColumnNamesDict;

        if (operationType == OperationType.Insert && tableInfo.PropertyColumnNamesDict.Any()) // Only OnInsert omit colums with Default values
        {
            tableInfo.PropertyColumnNamesDict = tableInfo.PropertyColumnNamesDict.Where(a => !tableInfo.DefaultValueProperties.Contains(a.Key)).ToDictionary(a => a.Key, a => a.Value);
        }

        List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
        List<string> propertiesList = tableInfo.PropertyColumnNamesDict.Keys.ToList();

        tableInfo.PropertyColumnNamesDict = tempDict;

        bool keepIdentity = tableInfo.BulkConfig.BulkCopyOptions.HasFlag(BulkCopyOptions.KeepIdentity);
        string? uniquColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList().FirstOrDefault();

        if (!keepIdentity && tableInfo.HasIdentity && (operationType == OperationType.Insert || tableInfo.IdentityColumnName != uniquColumnName))
        {
            string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
            columnsList = columnsList.Where(a => a != tableInfo.IdentityColumnName).ToList();
            propertiesList = propertiesList.Where(a => a != identityPropertyName).ToList();
        }

        return columnsList;
    }

    /// <summary>
    /// Generates SQL query to truncate a table
    /// </summary>
    /// <param name="tableName"></param>
    public static string TruncateTable(string tableName)
    {
        string q = $"TRUNCATE {tableName} RESTART IDENTITY;";
        q = q.Replace("[", @"""").Replace("]", @"""");

        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a table
    /// </summary>
    /// <param name="tableName"></param>
    public static string DropTable(string tableName)
    {
        string q = $"DROP TABLE IF EXISTS {tableName}";
        q = q.Replace("[", @"""").Replace("]", @"""");

        return q;
    }

    /// <summary>
    /// Generates SQL query to count the unique constranints
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CountUniqueConstrain(TableInfo tableInfo)
    {
        List<string> primaryKeysColumns = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();

        string q = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ";

        foreach ((string pkColumn, int index) in primaryKeysColumns.Select((value, i) => (value, i)))
        {
            q = q +
                $"INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu{index} " +
                $"ON cu{index}.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND cu{index}.COLUMN_NAME = '{pkColumn}' ";
        }

        q = q +
            $"WHERE (tc.CONSTRAINT_TYPE = 'UNIQUE' OR tc.CONSTRAINT_TYPE = 'PRIMARY KEY') " +
            $"AND tc.TABLE_NAME = '{tableInfo.TableName}' ";

        return q;
    }

    /// <summary>
    /// Generate SQL query to create a unique index
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CreateUniqueIndex(TableInfo tableInfo)
    {
        string? tableName = tableInfo.TableName;
        string schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        string fullTableNameFormated = $@"{schemaFormated}""{tableName}""";

        List<string> uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        string uniqueColumnNamesFormated = @"""" + string.Join(@""", """, uniqueColumnNames) + @"""";
        string schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";

        string q = $@"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ""tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}"" " +
                $@"ON {fullTableNameFormated} ({uniqueColumnNamesFormated})";

        return q;
    }

    /// <summary>
    /// Generates SQL query to create a unique constraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CreateUniqueConstrain(TableInfo tableInfo)
    {
        string? tableName = tableInfo.TableName;
        string schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        string fullTableNameFormated = $@"{schemaFormated}""{tableName}""";

        List<string> uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        string schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        string uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        string q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"ADD CONSTRAINT ""{uniqueConstrainName}"" " +
                $@"UNIQUE USING INDEX ""{uniqueConstrainName}""";

        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a unique contstraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string DropUniqueConstrain(TableInfo tableInfo)
    {
        string? tableName = tableInfo.TableName;
        string schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        string fullTableNameFormated = $@"{schemaFormated}""{tableName}""";

        List<string> uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        string schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        string uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        string q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"DROP CONSTRAINT ""{uniqueConstrainName}"";";

        return q;
    }

    /// <summary>
    /// Restructures a sql query for batch commands
    /// </summary>
    /// <param name="sql"></param>
    /// <param name="isDelete"></param>
    public override string RestructureForBatch(string sql, bool isDelete = false)
    {
        sql = sql.Replace("[", @"""").Replace("]", @"""");
        string firstLetterOfTable = sql.Substring(7, 1);

        if (isDelete)
        {
            //FROM
            // DELETE i FROM "Item" AS i WHERE i."ItemId" <= 1"
            //TO
            // DELETE FROM "Item" AS i WHERE i."ItemId" <= 1"
            //WOULD ALSO WORK
            // DELETE FROM "Item" WHERE "ItemId" <= 1

            sql = sql.Replace($"DELETE {firstLetterOfTable}", "DELETE ");
        }
        else
        {
            //FROM
            // UPDATE i SET "Description" = @Description, "Price\" = @Price FROM "Item" AS i WHERE i."ItemId" <= 1
            //TO
            // UPDATE "Item" AS i SET "Description" = 'Update N', "Price" = 1.5 FROM "Item" WHERE i."ItemId" <= 1
            //WOULD ALSO WORK
            // UPDATE "Item" SET "Description" = 'Update N', "Price" = 1.5 FROM "Item" WHERE "ItemId" <= 1

            string tableAS = sql.Substring(sql.IndexOf("FROM") + 4, sql.IndexOf($"AS {firstLetterOfTable}") - sql.IndexOf("FROM"));

            if (!sql.Contains("JOIN"))
            {
                sql = sql.Replace($"AS {firstLetterOfTable}", "");
            }
            else
            {
                int positionFROM = sql.IndexOf("FROM");
                int positionEndJOIN = sql.IndexOf("JOIN ") + "JOIN ".Length;
                int positionON = sql.IndexOf(" ON");
                int positionEndON = positionON + " ON".Length;
                int positionWHERE = sql.IndexOf("WHERE");
                string oldSqlSegment = sql[positionFROM..positionWHERE];
                string newSqlSegment = "FROM " + sql[positionEndJOIN..positionON];
                string equalsPkFk = sql[positionEndON..positionWHERE];
                sql = sql.Replace(oldSqlSegment, newSqlSegment);
                sql = sql.Replace("WHERE", " WHERE");
                sql = sql + " AND" + equalsPkFk;
            }

            sql = sql.Replace($"UPDATE {firstLetterOfTable}", "UPDATE" + tableAS);
        }

        return sql;
    }

    /// <summary>
    /// Returns a DbParameters intanced per provider
    /// </summary>
    /// <param name="sqlParameter"></param>
    /// <returns></returns>
    public override object CreateParameter(DbParameter dbParameter)
    {
        Npgsql.NpgsqlParameter parameter = new Npgsql.NpgsqlParameter(dbParameter.ParameterName, dbParameter.Value);

        return parameter;
    }

    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public override string SelectFromOutputTable(TableInfo tableInfo)
    {
        string query = SqlQueryBuilder.SelectFromOutputTable(tableInfo);

        return query;
    }

    /// <summary>
    /// Returns NpgsqlDbType for PostgreSql parameters. Throws <see cref="NotImplementedException"/> for anothers providers
    /// </summary>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public override object Dbtype()
    {
        NpgsqlTypes.NpgsqlDbType dbType = NpgsqlTypes.NpgsqlDbType.Jsonb;

        return dbType;
    }

    /// <summary>
    /// Returns void. Throws <see cref="NotImplementedException"/> for anothers providers
    /// </summary>
    /// <exception cref="NotImplementedException"></exception>
    public override void SetDbTypeParam(object npgsqlParameter, object dbType)
    {
        ((Npgsql.NpgsqlParameter)npgsqlParameter).NpgsqlDbType = (NpgsqlTypes.NpgsqlDbType)dbType;
    }

    /// <inheritdoc/>
    public override (string Sql, List<object> Parameters) FinalizeBatchQuery(Microsoft.EntityFrameworkCore.DbContext context, string sql, List<object> sqlParameters, Type? entityType, bool isDelete)
    {
        string restructuredSql = RestructureForBatch(sql, isDelete);

        var finalizedParameters = new List<object>();

        foreach (object parameter in sqlParameters)
        {
            DbParameter dbParam = (DbParameter)parameter;
            dynamic npgsqlParameter = CreateParameter(dbParam);

            if (!isDelete && entityType is not null)
            {
                string parameterName = ((string)npgsqlParameter.ParameterName).Replace("@", string.Empty);
                Type? propertyType = entityType.GetProperties().SingleOrDefault(a => a.Name == parameterName)?.PropertyType;

                if (propertyType == typeof(System.Text.Json.JsonElement) || propertyType == typeof(System.Text.Json.JsonElement?))
                {
                    npgsqlParameter.NpgsqlDbType = Dbtype();
                }
            }

            finalizedParameters.Add(npgsqlParameter);
        }

        return (restructuredSql, finalizedParameters);
    }
}
