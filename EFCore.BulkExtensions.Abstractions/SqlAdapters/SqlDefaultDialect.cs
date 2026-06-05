using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;

namespace EFCore.BulkExtensions.SqlAdapters;

public abstract class SqlDefaultDialect : IQueryBuilderSpecialization
{
    private static readonly int SelectStatementLength = "SELECT".Length;

    public abstract char EscL { get; }

    public abstract char EscR { get; }

    public virtual string? DefaultSchema => null;

    public virtual bool MatchEntitiesByPosition => false;

    public virtual bool SupportsGraphOperations => true;

    public virtual bool UseValueGenerationStrategyForIdentity => true;

    public virtual bool DetectIdentityByIntegerPrimaryKey => false;

    protected virtual string BatchSqlTableAliasSplitEscapeStart => "[";

    protected virtual string BatchSqlTableAliasSplitEscapeEnd => "]";

    public virtual List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters)
    {
        var sqlParametersReloaded = new List<object>();
        foreach (var parameter in sqlParameters)
        {
            var sqlParameter = (IDbDataParameter)parameter;

            try
            {
                DbType dt = sqlParameter.DbType;
                if (sqlParameter.DbType == DbType.DateTime)
                {
                    sqlParameter.DbType = DbType.DateTime2; // sets most specific parameter DbType possible for so that precision is not lost
                }
            }
            catch (Exception ex)
            {
                string noMappingText = "No mapping exists from object type "; // Fixes for Batch ops on PostgreSQL with:
                if (!ex.Message.StartsWith(noMappingText + "System.Collections.Generic.List") &&             // - Contains
                    !ex.Message.StartsWith(noMappingText + "System.Int32[]") &&                              // - Contains
                    !ex.Message.StartsWith(noMappingText + "System.Int64[]") &&                              // - Contains
                    !ex.Message.StartsWith(noMappingText + typeof(System.Text.Json.JsonElement).FullName) && // - JsonElement param
                    !ex.Message.StartsWith(noMappingText + typeof(System.Text.Json.JsonDocument).FullName))  // - JsonElement param
                {
                    throw;
                }
            }
            sqlParametersReloaded.Add(sqlParameter);
        }
        return sqlParametersReloaded;
    }


    public virtual string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression)
    {
        return "+";
    }

    public virtual (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery)
    {
        var escapeSymbolEnd = BatchSqlTableAliasSplitEscapeEnd;
        var escapeSymbolStart = BatchSqlTableAliasSplitEscapeStart;
        var tableAliasEnd = sqlQuery[SelectStatementLength..sqlQuery.IndexOf(escapeSymbolEnd, StringComparison.Ordinal)]; // " TOP(10) [table_alias" / " [table_alias" : " table_alias"
        var tableAliasStartIndex = tableAliasEnd.IndexOf(escapeSymbolStart, StringComparison.Ordinal);
        var tableAlias = tableAliasEnd[(tableAliasStartIndex + escapeSymbolStart.Length)..]; // "table_alias"
        var topStatement = tableAliasEnd[..tableAliasStartIndex].TrimStart(); // "TOP(10) " / if TOP not present in query this will be a Substring(0,0) == ""
        (string, string) result = (tableAlias, topStatement);
        return result;
    }

    public virtual ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias, string tableAliasSuffixAs)
    {
        return new ExtractedTableAlias
        {
            TableAlias = tableAlias,
            TableAliasSuffixAs = tableAliasSuffixAs,
            Sql = fullQuery
        };
    }
}
