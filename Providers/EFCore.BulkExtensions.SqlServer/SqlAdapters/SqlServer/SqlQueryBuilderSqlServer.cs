using System;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

public sealed class SqlQueryBuilderSqlServer : QueryBuilderExtensions
{
    /// <inheritdoc/>
    public override object CreateParameter(DbParameter dbParameter) => throw new NotImplementedException();

    /// <inheritdoc/>
    public override object Dbtype() => throw new NotImplementedException();

    /// <inheritdoc/>
    public override string RestructureForBatch(string sql, bool isDelete = false) => throw new NotImplementedException();

    /// <inheritdoc/>
    public override string SelectFromOutputTable(TableInfo tableInfo)
    {
        string query = SqlQueryBuilder.SelectFromOutputTable(tableInfo);

        return query;
    }

    /// <inheritdoc/>
    public override void SetDbTypeParam(object npgsqlParameter, object dbType) => throw new NotImplementedException();

    /// <inheritdoc/>
    public override string GetBatchDeleteSql(string leadingComments, string topStatement, string tableAlias, string sql)
    {
        tableAlias = $"[{tableAlias}]";
        int outerQueryOrderByIndex = -1;
        bool useUpdateableCte = false;
        int lastOrderByIndex = sql.LastIndexOf(Environment.NewLine + "ORDER BY ", StringComparison.OrdinalIgnoreCase);

        if (lastOrderByIndex > -1)
        {
            int subQueryEnd = sql.LastIndexOf($") AS {tableAlias}" + Environment.NewLine, StringComparison.OrdinalIgnoreCase);

            if (subQueryEnd == -1 || lastOrderByIndex > subQueryEnd)
            {
                outerQueryOrderByIndex = lastOrderByIndex;

                if (topStatement.Length > 0)
                {
                    useUpdateableCte = true;
                }
                else
                {
                    int offSetIndex = sql.LastIndexOf(Environment.NewLine + "OFFSET ", StringComparison.OrdinalIgnoreCase);

                    if (offSetIndex > outerQueryOrderByIndex)
                    {
                        useUpdateableCte = true;
                    }
                }
            }
        }

        string resultQuery;

        if (useUpdateableCte)
        {
            string cte = string.Concat("cte", Guid.NewGuid().ToString().AsSpan(0, 8)); // 8 chars of Guid as tableNameSuffix to avoid same name collision with other tables
            resultQuery = $"{leadingComments}WITH [{cte}] AS (SELECT {topStatement}* {sql}) DELETE FROM [{cte}]";
        }
        else
        {
            if (outerQueryOrderByIndex > -1)
            {
                // ORDER BY is not allowed without TOP or OFFSET.
                sql = sql[..outerQueryOrderByIndex];
            }

            resultQuery = $"{leadingComments}DELETE {topStatement}{tableAlias}{sql}";
        }

        return resultQuery;
    }

    /// <inheritdoc/>
    public override string FormatBatchUpdateColumnsSql(string updateColumnsSql, string tableAlias)
    {
        return updateColumnsSql;
    }
}
