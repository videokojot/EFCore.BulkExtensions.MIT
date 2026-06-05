using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters;


/// <summary>
/// Contains a list of methods to generate Adpaters and helpers instances
/// </summary>
public abstract class QueryBuilderExtensions
{
    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    public abstract string SelectFromOutputTable(TableInfo tableInfo);

    /// <summary>
    /// Restructures a sql query for batch commands
    /// </summary>
    public abstract string RestructureForBatch(string sql, bool isDelete = false);

    /// <summary>
    /// Returns a DbParameters intanced per provider
    /// </summary>
    public abstract object CreateParameter(DbParameter dbParameter);

    /// <summary>
    /// Returns NpgsqlDbType for PostgreSql parameters. Throws <see cref="NotImplementedException"/> for anothers providers
    /// </summary>
    public abstract object Dbtype();

    /// <summary>
    /// Returns void. Throws <see cref="NotImplementedException"/> for anothers providers
    /// </summary>
    public abstract void SetDbTypeParam(object npgsqlParameter, object dbType);

    /// <summary>
    /// Builds the provider specific batch DELETE statement. The default implementation produces a plain
    /// <c>DELETE</c> statement; providers that need a different shape (e.g. CTE based deletes) override this.
    /// </summary>
    public virtual string GetBatchDeleteSql(string leadingComments, string topStatement, string tableAlias, string sql)
    {
        return $"{leadingComments}DELETE {topStatement}{tableAlias}{sql}";
    }

    /// <summary>
    /// Adjusts the SET columns fragment of a batch UPDATE for the provider. The default implementation removes the
    /// table alias prefix; providers that keep the alias (e.g. SQL Server) override this.
    /// </summary>
    public virtual string FormatBatchUpdateColumnsSql(string updateColumnsSql, string tableAlias)
    {
        string formatted = updateColumnsSql.Replace($"[{tableAlias}].", string.Empty);

        return formatted;
    }

    /// <summary>
    /// Performs any provider specific final reshaping of a batch query and its parameters. The default implementation
    /// returns the query and parameters unchanged.
    /// </summary>
    public virtual (string Sql, List<object> Parameters) FinalizeBatchQuery(DbContext context, string sql, List<object> sqlParameters, Type? entityType, bool isDelete)
    {
        return (sql, sqlParameters);
    }
}
