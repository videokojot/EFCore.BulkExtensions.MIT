using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Contains a list of methods for query operations
/// </summary>
public interface IQueryBuilderSpecialization
{
    char EscL { get; }
    char EscR { get; }

    /// <summary>
    /// Default schema applied when an entity does not specify one, or null when the provider has no default schema.
    /// </summary>
    string? DefaultSchema { get; }

    /// <summary>
    /// When true, an existing entity that is not matched by its key values is matched by its position in the list.
    /// </summary>
    bool MatchEntitiesByPosition { get; }

    /// <summary>
    /// When true, the provider supports bulk operations over an entity graph.
    /// </summary>
    bool SupportsGraphOperations { get; }

    /// <summary>
    /// When true, the identity column is detected from the provider value generation strategy.
    /// </summary>
    bool UseValueGenerationStrategyForIdentity { get; }

    /// <summary>
    /// When true, the identity column is detected from an auto generated integral primary key.
    /// </summary>
    bool DetectIdentityByIntegerPrimaryKey { get; }

    List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters);
    
    string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression);

    /// <summary>
    /// Returns a tuple containing the batch sql reformat table alias
    /// </summary>
    (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery);
    
    ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias, string tableAliasSuffixAs);
}
