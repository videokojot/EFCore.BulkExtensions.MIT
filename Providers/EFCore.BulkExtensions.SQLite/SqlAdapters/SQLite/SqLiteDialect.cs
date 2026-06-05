using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace EFCore.BulkExtensions.SqlAdapters.SQLite;

public sealed class SqliteDialect : SqlDefaultDialect
{
    public override char EscL => '[';

    public override char EscR => ']';

    public override bool SupportsGraphOperations => false;

    public override bool UseValueGenerationStrategyForIdentity => false;

    public override bool DetectIdentityByIntegerPrimaryKey => true;

    public override List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters)
    {
        var sqlParametersReloaded = new List<object>();

        foreach (object parameter in sqlParameters)
        {
            IDbDataParameter sqlParameter = (IDbDataParameter)parameter;
            SqliteParameter sqliteParameter = new SqliteParameter(sqlParameter.ParameterName, sqlParameter.Value);
            sqlParametersReloaded.Add(sqliteParameter);
        }

        return sqlParametersReloaded;
    }

    /// <inheritdoc/>
    public override string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression)
    {
        bool isStringConcat = IsStringConcat(binaryExpression);
        string operation = isStringConcat ? "||" : "+";

        return operation;
    }

    /// <inheritdoc/>
    public override (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery)
    {
        (string, string) result = (string.Empty, string.Empty);

        return result;
    }

    /// <inheritdoc/>
    public override ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias, string tableAliasSuffixAs)
    {
        ExtractedTableAlias result = new ExtractedTableAlias();
        Match match = Regex.Match(fullQuery, @"FROM (""[^""]+"")( AS ""[^""]+"")");
        result.TableAlias = match.Groups[1].Value;
        result.TableAliasSuffixAs = match.Groups[2].Value;
        result.Sql = fullQuery[(match.Index + match.Length)..];

        return result;
    }

    internal static bool IsStringConcat(BinaryExpression binaryExpression)
    {
        PropertyInfo? methodProperty = binaryExpression.GetType().GetProperty("Method");
        if (methodProperty == null)
        {
            return false;
        }

        MethodInfo? method = methodProperty.GetValue(binaryExpression) as MethodInfo;
        if (method == null)
        {
            return false;
        }

        bool isStringConcat = method.DeclaringType == typeof(string) && method.Name == nameof(string.Concat);

        return isStringConcat;
    }
}
