using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters;


/// <summary>
/// Contains the table alias and SQL query
/// </summary>
public class ExtractedTableAlias
{
#pragma warning disable CS1591 // No XML comments required
    public string TableAlias { get; set; } = null!;
    public string TableAliasSuffixAs { get; set; } = null!;
    public string Sql { get; set; } = null!;
#pragma warning restore CS1591 // No XML comments required
}

/// <summary>
/// Contains a list of methods for query operations
/// </summary>
public interface IQueryBuilderSpecialization
{
    char EscL { get; }
    char EscR { get; }

    List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters);
    
    string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression);

    /// <summary>
    /// Returns a tuple containing the batch sql reformat table alias
    /// </summary>
    (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery, DbServerType databaseType);
    
    ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias, string tableAliasSuffixAs);
}
