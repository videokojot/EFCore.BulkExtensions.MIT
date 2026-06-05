using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace EFCore.BulkExtensions;

/// <summary>
/// Class responsible for all batch utilities related to EFCore.BulkExtensions
/// </summary>
public static class BatchUtil
{
    // In comment are Examples of how SqlQuery is changed for Sql Batch

    // SELECT [a].[Column1], [a].[Column2], .../r/n
    // FROM [Table] AS [a]/r/n
    // WHERE [a].[Column] = FilterValue
    // --
    // DELETE [a]
    // FROM [Table] AS [a]
    // WHERE [a].[Columns] = FilterValues
    /// <summary>
    /// Generates delete sql query
    /// </summary>
    public static (string, List<object>) GetSqlDelete(IQueryable query, DbContext context)
    {
        (string sql, string tableAlias, string _, string topStatement, string leadingComments, IEnumerable<object> innerParameters) = GetBatchSql(query, context, isUpdate: false);

        innerParameters = ReloadSqlParameters(context, innerParameters.ToList()); // Sqlite requires SqliteParameters
        QueryBuilderExtensions queryBuilder = SqlAdaptersMapping.DbServer(context).QueryBuilder;

        string resultQuery = queryBuilder.GetBatchDeleteSql(leadingComments, topStatement, tableAlias, sql);

        List<object> deleteParameters = new List<object>(innerParameters);
        (string Sql, List<object> Parameters) deleteResult = queryBuilder.FinalizeBatchQuery(context, resultQuery, deleteParameters, entityType: null, isDelete: true);
        return deleteResult;
    }

    // SELECT [a].[Column1], [a].[Column2], .../r/n
    // FROM [Table] AS [a]/r/n
    // WHERE [a].[Column] = FilterValue
    // --
    // UPDATE [a] SET [UpdateColumns] = N'updateValues'
    // FROM [Table] AS [a]
    // WHERE [a].[Columns] = FilterValues
    /// <summary>
    /// Generates sql query to update data
    /// </summary>
    public static (string, List<object>) GetSqlUpdate(IQueryable query, DbContext context, Type type, object? updateValues, List<string>? updateColumns)
    {
        (string sql, string tableAlias, string tableAliasSufixAs, string topStatement, string leadingComments, IEnumerable<object> innerParameters) = GetBatchSql(query, context, isUpdate: true);
        List<object> sqlParameters = new List<object>(innerParameters);

        string sqlSET = GetSqlSetSegment(context, updateValues?.GetType(), updateValues, updateColumns, sqlParameters);

        sqlParameters = ReloadSqlParameters(context, sqlParameters); // Sqlite requires SqliteParameters

        string resultQuery = $"{leadingComments}UPDATE {topStatement}{tableAlias}{tableAliasSufixAs} {sqlSET}{sql}";

        if (resultQuery.Contains("ORDER") && resultQuery.Contains("TOP"))
        {
            resultQuery = $"WITH C AS (SELECT {topStatement}*{sql}) UPDATE C {sqlSET}";
        }
        if (resultQuery.Contains("ORDER") && !resultQuery.Contains("TOP")) // When query has ORDER only without TOP(Take) then it is removed since not required and to avoid invalid Sql
        {
            resultQuery = resultQuery.Split("ORDER", StringSplitOptions.None)[0];
        }

        QueryBuilderExtensions queryBuilder = SqlAdaptersMapping.DbServer(context).QueryBuilder;
        (string Sql, List<object> Parameters) updateResult = queryBuilder.FinalizeBatchQuery(context, resultQuery, sqlParameters, type, isDelete: false);
        return updateResult;
    }

    /// <summary>
    /// get Update Sql
    /// </summary>
    public static (string, List<object>) GetSqlUpdate<T>(IQueryable<T> query, DbContext context, Type type, Expression<Func<T, T>> expression) where T : class
    {
        (string sql, string tableAlias, string tableAliasSufixAs, string topStatement, string leadingComments, IEnumerable<object> innerParameters) = GetBatchSql(query, context, isUpdate: true);

        BatchUpdateCreateBodyData createUpdateBodyData = new BatchUpdateCreateBodyData(sql, context, innerParameters, query, type, tableAlias, expression);

        CreateUpdateBody(context, createUpdateBodyData, expression.Body);

        List<object> sqlParameters = ReloadSqlParameters(context, createUpdateBodyData.SqlParameters); // Sqlite requires SqliteParameters
        QueryBuilderExtensions queryBuilder = SqlAdaptersMapping.DbServer(context).QueryBuilder;
        string updateColumnsSql = createUpdateBodyData.UpdateColumnsSql.ToString();
        string sqlColumns = queryBuilder.FormatBatchUpdateColumnsSql(updateColumnsSql, tableAlias);

        string resultQuery = $"{leadingComments}UPDATE {topStatement}{tableAlias}{tableAliasSufixAs} SET {sqlColumns} {sql}";

        if (resultQuery.Contains("ORDER") && resultQuery.Contains("TOP"))
        {
            string tableAliasPrefix = "[" + tableAlias + "].";
            resultQuery = $"WITH C AS (SELECT {topStatement}*{sql}) UPDATE C SET {sqlColumns.Replace(tableAliasPrefix, "")}";
        }
        if (resultQuery.Contains("ORDER") && !resultQuery.Contains("TOP")) // When query has ORDER only without TOP(Take) then it is removed since not required and to avoid invalid Sql
        {
            resultQuery = resultQuery.Split("ORDER", StringSplitOptions.None)[0];
        }

        (string Sql, List<object> Parameters) updateResult = queryBuilder.FinalizeBatchQuery(context, resultQuery, sqlParameters, type, isDelete: false);
        return updateResult;
    }

    /// <summary>
    /// Reloads SQL paramaters
    /// </summary>
    public static List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters)
    {
        List<object> reloadedParameters = SqlAdaptersMapping.GetAdapterDialect(context).ReloadSqlParameters(context, sqlParameters);
        return reloadedParameters;
    }

    /// <summary>
    /// Generates SQL queries for batch operations
    /// </summary>
    public static (string Sql, string TableAlias, string TableAliasSufixAs, string TopStatement, string LeadingComments, IEnumerable<object> InnerParameters) GetBatchSql(IQueryable query, DbContext context, bool isUpdate)
    {
        IQueryBuilderSpecialization sqlQueryBuilder = SqlAdaptersMapping.GetAdapterDialect(context);
        (string fullSqlQuery, IEnumerable<DbParameter> innerParametersEnumerable) = query.ToParametrizedSql(context);
        IEnumerable<object> innerParameters = innerParametersEnumerable;

        (string leadingComments, string sqlQuery) = SplitLeadingCommentsAndMainSqlQuery(fullSqlQuery);

        string tableAlias;
        string tableAliasSufixAs = string.Empty;
        string topStatement;

        (string reformatTableAlias, string reformatTopStatement) = sqlQueryBuilder.GetBatchSqlReformatTableAliasAndTopStatement(sqlQuery);
        tableAlias = reformatTableAlias;
        topStatement = reformatTopStatement;

        int indexFrom = sqlQuery.IndexOf(Environment.NewLine, StringComparison.Ordinal);
        string sql = sqlQuery[indexFrom..];
        sql = sql.Contains('{') ? sql.Replace("{", "{{") : sql; // Curly brackets have to be escaped:
        sql = sql.Contains('}') ? sql.Replace("}", "}}") : sql; // https://github.com/aspnet/EntityFrameworkCore/issues/8820

        if (isUpdate)
        {
            ExtractedTableAlias extracted = sqlQueryBuilder.GetBatchSqlExtractTableAliasFromQuery(
                sql, tableAlias, tableAliasSufixAs
            );
            tableAlias = extracted.TableAlias;
            tableAliasSufixAs = extracted.TableAliasSuffixAs;
            sql = extracted.Sql;
        }

        (string Sql, string TableAlias, string TableAliasSufixAs, string TopStatement, string LeadingComments, IEnumerable<object> InnerParameters) batchResult = (sql, tableAlias, tableAliasSufixAs, topStatement, leadingComments, innerParameters);
        return batchResult;
    }

    /// <summary>
    /// Returns a sql set seqment query
    /// </summary>
    public static string GetSqlSetSegment(DbContext context, Type? updateValuesType, object? updateValues, List<string>? updateColumns, List<object> parameters)
    {
        TableInfo tableInfo = TableInfo.CreateInstance(context, updateValuesType, new List<object>(), OperationType.Read, new BulkConfig());
        return GetSqlSetSegment(context, tableInfo, updateValuesType, updateValues, updateValuesType is null ? null : Activator.CreateInstance(updateValuesType), updateColumns, parameters);
    }

    private static string GetSqlSetSegment(DbContext context, TableInfo tableInfo, Type? updateValuesType, object? updateValues, object? defaultValues, List<string>? updateColumns, List<object> parameters)
    {
        string sql = string.Empty;
        foreach (KeyValuePair<string, string> propertyNameColumnName in tableInfo.PropertyColumnNamesDict)
        {
            string propertyName = propertyNameColumnName.Key;
            string columnName = propertyNameColumnName.Value;
            string[] pArray = propertyName.Split(new char[] { '.' });
            Type? lastType = updateValuesType;
            PropertyInfo? property = lastType?.GetProperty(pArray[0]);
            if (property != null)
            {
                object? propertyUpdateValue = property.GetValue(updateValues);
                object? propertyDefaultValue = property.GetValue(defaultValues);

                for (int i = 1; i < pArray.Length; i++)
                {
                    lastType = property?.PropertyType;
                    property = lastType?.GetProperty(pArray[i]);
                    propertyUpdateValue = propertyUpdateValue != null
                        ? property?.GetValue(propertyUpdateValue)
                        : propertyUpdateValue;

                    object? lastDefaultValues = lastType!.Assembly.CreateInstance(lastType.FullName!);
                    propertyDefaultValue = property?.GetValue(lastDefaultValues);
                }

                if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName))
                {
                    bool isEnum = tableInfo.ColumnToPropertyDictionary[columnName].ClrType.IsEnum;
                    if (!isEnum) // Omit from ConvertibleColumns because there Enum of byte type gets converter to Number which is then different from default enum value // Test: RunBatchUpdateEnum
                    {
                        propertyUpdateValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyUpdateValue);
                        propertyDefaultValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyDefaultValue);
                    }
                }

                bool isDifferentFromDefault = propertyUpdateValue != null && propertyUpdateValue?.ToString() != propertyDefaultValue?.ToString();
                bool updateColumnExplicit = updateColumns != null && updateColumns.Contains(propertyName);
                if (isDifferentFromDefault || updateColumnExplicit)
                {
                    sql += $"[{columnName}] = @{columnName}, ";
                    string parameterName = $"@{columnName}";
                    IDbDataParameter? param = TryCreateRelationalMappingParameter(
                        context,
                        columnName,
                        parameterName,
                        propertyUpdateValue,
                        tableInfo);

                    if (param == null)
                    {
                        propertyUpdateValue ??= DBNull.Value;

                        DbCommand tempCommand = context.Database.GetDbConnection().CreateCommand();
                        param = tempCommand.CreateParameter();
                        param.ParameterName = $"@{columnName}";
                        param.Value = propertyUpdateValue;
                        if (!isDifferentFromDefault && propertyUpdateValue == DBNull.Value
                            && property?.PropertyType == typeof(byte[])) // needed only when having complex type property to be updated to default 'null'
                        {
                            param.DbType = DbType.Binary; // fix for ByteArray since implicit conversion nvarchar to varbinary(max) is not allowed
                        }
                    }

                    parameters.Add(param);
                }
            }
        }
        if (string.IsNullOrEmpty(sql))
        {
            throw new InvalidOperationException("SET Columns not defined. If one or more columns should be updated to theirs default value use 'updateColumns' argument.");
        }
        sql = sql.Remove(sql.Length - 2, 2); // removes last excess comma and space: ", "
        return $"SET {sql}";
    }

    /// <summary>
    /// Recursive analytic expression 
    /// </summary>
    public static void CreateUpdateBody(DbContext context, BatchUpdateCreateBodyData createBodyData, Expression expression, string? columnName = null)
    {
TableInfo? rootTypeTableInfo = createBodyData.GetTableInfoForType(createBodyData.RootType);
Dictionary<string, string>? columnNameValueDict = rootTypeTableInfo?.PropertyColumnNamesDict;
string tableAlias = createBodyData.TableAlias;
StringBuilder sqlColumns = createBodyData.UpdateColumnsSql;
List<object> sqlParameters = createBodyData.SqlParameters;

        if (expression is MemberInitExpression memberInitExpression)
        {
            foreach (MemberBinding item in memberInitExpression.Bindings)
            {
                if (item is MemberAssignment assignment)
                {
                    string? currentColumnName;
                    if (columnNameValueDict?.TryGetValue(assignment.Member.Name, out string? value) ?? false)
                    {
                        currentColumnName = value;
                    }
                    else
                    {
                        currentColumnName = assignment.Member.Name;
                    }

                    sqlColumns.Append($" [{tableAlias}].[{currentColumnName}]");
                    sqlColumns.Append(" =");

                    if (!TryCreateUpdateBodyNestedQuery(createBodyData, assignment.Expression, assignment))
                    {
                        CreateUpdateBody(context, createBodyData, assignment.Expression, currentColumnName);
                    }

                    if (memberInitExpression.Bindings.IndexOf(item) < (memberInitExpression.Bindings.Count - 1))
                    {
                        sqlColumns.Append(" ,");
                    }
                }
            }

            return;
        }

        if (expression is MemberExpression memberExpression
            && memberExpression.Expression is ParameterExpression parameterExpression
            && parameterExpression.Name == createBodyData.RootInstanceParameterName)
        {
            if (columnNameValueDict?.TryGetValue(memberExpression.Member.Name, out string? value) ?? false)
            {
                sqlColumns.Append($" [{tableAlias}].[{value}]");
            }
            else
            {
                sqlColumns.Append($" [{tableAlias}].[{memberExpression.Member.Name}]");
            }

            return;
        }

        if (expression is ConstantExpression constantExpression)
        {
            // TODO: I believe the EF query builder inserts constant expressions directly into the SQL.
            // This should probably match that behavior for the update body
            AddSqlParameter(context, sqlColumns, sqlParameters, rootTypeTableInfo, columnName, constantExpression.Value);
            return;
        }

        if (expression is UnaryExpression unaryExpression)
        {
            switch (unaryExpression.NodeType)
            {
                case ExpressionType.Convert:
                    CreateUpdateBody(context, createBodyData, unaryExpression.Operand, columnName);
                    break;
                case ExpressionType.Not:
                    sqlColumns.Append(" ~");//this way only for SQL Server 
                    CreateUpdateBody(context, createBodyData, unaryExpression.Operand, columnName);
                    break;
                default: break;
            }

            return;
        }

        if (expression is BinaryExpression binaryExpression)
        {
            switch (binaryExpression.NodeType)
            {
                case ExpressionType.Add:
                    CreateUpdateBody(context, createBodyData, binaryExpression.Left, columnName);
                    string sqlOperator = SqlAdaptersMapping.GetAdapterDialect(context)
                        .GetBinaryExpressionAddOperation(binaryExpression);
                    sqlColumns.Append(" " + sqlOperator);
                    CreateUpdateBody(context, createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.Divide:
                    CreateUpdateBody(context, createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" /");
                    CreateUpdateBody(context, createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.Multiply:
                    CreateUpdateBody(context, createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" *");
                    CreateUpdateBody(context, createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.Subtract:
                    CreateUpdateBody(context, createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" -");
                    CreateUpdateBody(context, createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.And:
                    CreateUpdateBody(context, createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" &");
                    CreateUpdateBody(context, createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.Or:
                    CreateUpdateBody(context, createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" |");
                    CreateUpdateBody(context, createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.ExclusiveOr:
                    CreateUpdateBody(context, createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" ^");
                    CreateUpdateBody(context, createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.Coalesce:
                    sqlColumns.Append("COALESCE(");
                    CreateUpdateBody(context, createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(',');
                    CreateUpdateBody(context, createBodyData, binaryExpression.Right, columnName);
                    sqlColumns.Append(')');
                    break;

                default:
                    throw new NotSupportedException($"{nameof(BatchUtil)}.{nameof(CreateUpdateBody)}(..) is not supported for a binary exression of type {binaryExpression.NodeType}");
            }

            return;
        }

        // For any other case fallback on compiling and executing the expression
object? compiledExpressionValue = Expression.Lambda(expression).Compile().DynamicInvoke();
        AddSqlParameter(context, sqlColumns, sqlParameters, rootTypeTableInfo, columnName, compiledExpressionValue);
    }

    /// <summary>
    /// Returns the DbContext
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    public static DbContext? GetDbContext(IQueryable query)
    {
#pragma warning disable EF1001 // Internal EF Core API usage.
        const BindingFlags bindingFlags = BindingFlags.NonPublic | BindingFlags.Instance;
object? queryCompiler = typeof(EntityQueryProvider).GetField("_queryCompiler", bindingFlags)?.GetValue(query.Provider);
object? queryContextFactory = queryCompiler?.GetType().GetField("_queryContextFactory", bindingFlags)?.GetValue(queryCompiler);

object? dependencies = typeof(RelationalQueryContextFactory).GetProperty("Dependencies", bindingFlags)?.GetValue(queryContextFactory);

Type? queryContextDependencies = typeof(DbContext).Assembly.GetType(typeof(QueryContextDependencies).FullName!);
object? stateManagerProperty = queryContextDependencies?.GetProperty("StateManager", bindingFlags | BindingFlags.Public)?.GetValue(dependencies);
IStateManager? stateManager = (IStateManager?)stateManagerProperty;

        return stateManager?.Context;
#pragma warning restore EF1001
    }

    /// <summary>
    /// Splits the leading comments from the main sql query
    /// </summary>
    public static (string, string) SplitLeadingCommentsAndMainSqlQuery(string sqlQuery)
    {
StringBuilder leadingCommentsBuilder = new StringBuilder();
string mainSqlQuery = sqlQuery;
        while (!string.IsNullOrWhiteSpace(mainSqlQuery) 
            && !mainSqlQuery.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
        {
            if (mainSqlQuery.StartsWith("--"))
            {
                // pull off line comment
                int indexOfNextNewLine = mainSqlQuery.IndexOf(Environment.NewLine, StringComparison.Ordinal);
                if (indexOfNextNewLine > -1)
                {
                    leadingCommentsBuilder.Append(mainSqlQuery[..(indexOfNextNewLine + Environment.NewLine.Length)]);
                    mainSqlQuery = mainSqlQuery[(indexOfNextNewLine + Environment.NewLine.Length)..];
                    continue;
                }
            }

            if (mainSqlQuery.StartsWith("/*"))
            {
                int nextBlockCommentEndIndex = mainSqlQuery.IndexOf("*/", StringComparison.Ordinal);
                if (nextBlockCommentEndIndex > -1)
                {
                    leadingCommentsBuilder.Append(mainSqlQuery.AsSpan(0, nextBlockCommentEndIndex + 2));
                    mainSqlQuery = mainSqlQuery[(nextBlockCommentEndIndex + 2)..];
                    continue;
                }
            }

            int nextNonWhitespaceIndex = Array.FindIndex(mainSqlQuery.ToCharArray(), a => !char.IsWhiteSpace(a));

            if (nextNonWhitespaceIndex > 0)
            {
                leadingCommentsBuilder.Append(mainSqlQuery[..nextNonWhitespaceIndex]);
                mainSqlQuery = mainSqlQuery[nextNonWhitespaceIndex..];
                continue;
            }

            // Fallback... just find the first index of SELECT
            int selectIndex = mainSqlQuery.IndexOf("SELECT", StringComparison.OrdinalIgnoreCase);
            if (selectIndex > 0)
            {
                leadingCommentsBuilder.Append(mainSqlQuery[..selectIndex]);
                mainSqlQuery = mainSqlQuery[selectIndex..];
            }

            break;
        }

        string leadingCommentsResult = leadingCommentsBuilder.ToString();
        (string, string) splitResult = (leadingCommentsResult, mainSqlQuery);
        return splitResult;
    }

    private static void AddSqlParameter(DbContext context, StringBuilder sqlColumns, List<object> sqlParameters, TableInfo? tableInfo, string? columnName, object? value)
    {
        string paramName = $"@param_{sqlParameters.Count}";

        if (columnName != null && (tableInfo?.ConvertibleColumnConverterDict.TryGetValue(columnName, out Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter? valueConverter) ?? false))
        {
            value = valueConverter.ConvertToProvider.Invoke(value);
        }

        // will rely on SqlClientHelper.CorrectParameterType to fix the type before executing
        DbParameter? sqlParameter = TryCreateRelationalMappingParameter(context, columnName, paramName, value, tableInfo);
        if (sqlParameter == null)
        {
            DbCommand tempCommand = context.Database.GetDbConnection().CreateCommand();
            sqlParameter = tempCommand.CreateParameter();
            sqlParameter.ParameterName = paramName;
            sqlParameter.Value = value ?? DBNull.Value;
            string? columnType = columnName is null ? null : tableInfo?.ColumnNamesTypesDict[columnName];
            if (value == null
                && (columnType?.Contains(DbType.Binary.ToString(), StringComparison.OrdinalIgnoreCase) ?? false)) //"varbinary(max)".Contains("binary")
            {
                sqlParameter.DbType = DbType.Binary; // fix for ByteArray since implicit conversion nvarchar to varbinary(max) is not allowed
            }
        }

        sqlParameters.Add(sqlParameter);
        sqlColumns.Append($" {paramName}");
    }

    private static readonly MethodInfo? DbContextSetMethodInfo =
        typeof(DbContext).GetMethod(nameof(DbContext.Set), BindingFlags.Public | BindingFlags.Instance, null, Array.Empty<Type>(), null);

    /// <summary>
    /// Regex pattern to get table alias
    /// </summary>
    public static readonly Regex TableAliasPattern = new(@"(?:FROM|JOIN)\s+(\[\S+\]) AS (\[\S+\])", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// Attempt to create a DbParameter using the "Microsoft.EntityFrameworkCore.Storage.RelationalTypeMapping.CreateParameter(DbCommand, string, object, bool?)
    /// call for the specified column name.
    /// </summary>
    public static DbParameter? TryCreateRelationalMappingParameter(DbContext context, string? columnName, string parameterName, object? value, TableInfo? tableInfo)
    {
        if (columnName == null)
        {
            return null;
        }

        IProperty? propertyInfo = null;
        if (!tableInfo?.ColumnToPropertyDictionary.TryGetValue(columnName, out propertyInfo) ?? false)
        {
            return null;
        }

        try
        {
            Microsoft.EntityFrameworkCore.Storage.RelationalTypeMapping? relationalTypeMapping = propertyInfo?.GetRelationalTypeMapping();

            DbConnection connection = context.Database.GetDbConnection();
            using DbCommand dbCommand = connection.CreateCommand();
            DbParameter? parameter = relationalTypeMapping?.CreateParameter(dbCommand, parameterName, value, propertyInfo?.IsNullable);
            if (parameter == null)
            {
                return null;
            }

            return CloneDbParameter(connection, parameter);
        }
        catch (Exception) { }

        return null;
    }

    /// <summary>
    /// Tries to create nested body query
    /// </summary>
    public static bool TryCreateUpdateBodyNestedQuery(BatchUpdateCreateBodyData createBodyData, Expression expression, MemberAssignment memberAssignment)
    {
        if (expression is MemberExpression rootMemberExpression && rootMemberExpression.Expression is ParameterExpression)
        {
            // This is a basic assignment expression so don't try checking for a nested query
            return false;
        }

TableInfo? rootTypeTableInfo = createBodyData.GetTableInfoForType(createBodyData.RootType);

Stack<ExpressionNode> expressionStack = new Stack<ExpressionNode>();
HashSet<Expression> visited = new HashSet<Expression>();

List<ExpressionNode> rootParameterExpressionNodes = new List<ExpressionNode>();
        expressionStack.Push(new ExpressionNode(expression, null));

        // Perform a depth first traversal of the expression tree and see if there is a
        // leaf node in the format rootLambdaParameter.NavigationProperty indicating a nested
        // query is needed
        while (expressionStack.Count > 0)
        {
ExpressionNode currentExpressionNode = expressionStack.Pop();
Expression currentExpression = currentExpressionNode.Expression;
            if (visited.Contains(currentExpression))
            {
                continue;
            }

            visited.Add(currentExpression);
            switch (currentExpression)
            {
                case MemberExpression currentMemberExpression:
                    if (currentMemberExpression.Expression is ParameterExpression finalExpression
                        && finalExpression.Name == createBodyData.RootInstanceParameterName)
                    {
                        if (rootTypeTableInfo?.AllNavigationsDictionary.TryGetValue(currentMemberExpression.Member.Name, out _) ?? false)
                        {
                            rootParameterExpressionNodes.Add(new ExpressionNode(finalExpression, currentExpressionNode));
                            break;
                        }
                    }

                    expressionStack.Push(new ExpressionNode(currentMemberExpression.Expression!, currentExpressionNode));
                    break;

                case MethodCallExpression currentMethodCallExpresion:
                    if (currentMethodCallExpresion.Object != null)
                    {
                        expressionStack.Push(new ExpressionNode(currentMethodCallExpresion.Object, currentExpressionNode));
                    }

                    if (currentMethodCallExpresion.Arguments?.Count > 0)
                    {
                        foreach (Expression argumentExpression in currentMethodCallExpresion.Arguments)
                        {
                            expressionStack.Push(new ExpressionNode(argumentExpression, currentExpressionNode));
                        }
                    }
                    break;

                case LambdaExpression currentLambdaExpression:
                    expressionStack.Push(new ExpressionNode(currentLambdaExpression.Body, currentExpressionNode));
                    break;

                case UnaryExpression currentUnaryExpression:
                    expressionStack.Push(new ExpressionNode(currentUnaryExpression.Operand, currentExpressionNode));
                    break;

                case BinaryExpression currentBinaryExpression:
                    expressionStack.Push(new ExpressionNode(currentBinaryExpression.Left, currentExpressionNode));
                    expressionStack.Push(new ExpressionNode(currentBinaryExpression.Right, currentExpressionNode));
                    break;

                case ConditionalExpression currentConditionalExpression:
                    expressionStack.Push(new ExpressionNode(currentConditionalExpression.Test, currentExpressionNode));
                    expressionStack.Push(new ExpressionNode(currentConditionalExpression.IfTrue, currentExpressionNode));
                    expressionStack.Push(new ExpressionNode(currentConditionalExpression.IfFalse, currentExpressionNode));
                    break;

                default:
                    break;
            }
        }

        if (rootParameterExpressionNodes.Count < 1)
        {
            return false;
        }

        if (memberAssignment.Member is not PropertyInfo memberPropertyInfo)
        {
            return false;
        }

ExpressionNode? originalParameterNode = rootParameterExpressionNodes.FirstOrDefault();
ExpressionNode? firstNavigationNode = originalParameterNode?.Parent;
MemberExpression? firstMemberExpression = (MemberExpression?)firstNavigationNode?.Expression;
INavigation? firstNavigation = firstMemberExpression?.Member.Name is null ? null : rootTypeTableInfo?.AllNavigationsDictionary[firstMemberExpression?.Member.Name!];
bool? isFirstNavigationACollectionType = firstNavigation?.IsCollection;

IEntityType? firstNavigationTargetType = firstNavigation?.TargetEntityType;
Type? firstNavigationType = firstNavigationTargetType?.ClrType;
string? firstNavigationTableName = firstNavigationTargetType?.GetTableName();

        IQueryable? innerQueryable;
        if (isFirstNavigationACollectionType == true)
        {
MethodInfo? dbSetGenericMethod = DbContextSetMethodInfo?.MakeGenericMethod(createBodyData.RootType);
IQueryable? dbSetQueryable = (IQueryable?)dbSetGenericMethod?.Invoke(createBodyData.DbContext, null);

ParameterExpression? rootParameter = originalParameterNode?.Expression as ParameterExpression;
            innerQueryable = dbSetQueryable?.Provider.CreateQuery(Expression.Call(
                null,
                QueryableMethods.Select.MakeGenericMethod(createBodyData.RootType, memberPropertyInfo.PropertyType),
                dbSetQueryable.Expression,
                Expression.Lambda(expression, rootParameter!)
            ));
        }
        else
        {
            MethodInfo? dbSetGenericMethod = firstNavigationType is null ? null : DbContextSetMethodInfo?.MakeGenericMethod(firstNavigationType);
IQueryable? dbSetQueryable = (IQueryable?)dbSetGenericMethod?.Invoke(createBodyData.DbContext, null);

string rootParamterName = $"x{firstMemberExpression?.Member.Name}";
ParameterExpression rootParameter = firstNavigationType is null
                ? throw new ArgumentException("Unable to create root paramater if Navigation type is null")
                : Expression.Parameter(firstNavigationType, rootParamterName);

            Expression lambdaBody = rootParameter;
ExpressionNode? previousNode = firstNavigationNode;
ExpressionNode? currentNode = previousNode?.Parent;

            while (currentNode != null)
            {
bool wasNodeHandled = false;
                switch (currentNode.Expression)
                {
                    case MemberExpression currentMemberExpression:
                        lambdaBody = Expression.MakeMemberAccess(lambdaBody, currentMemberExpression.Member);
                        wasNodeHandled = true;
                        break;

                    case MethodCallExpression currentMethodCallExpression:
                        if (currentMethodCallExpression.Object == previousNode?.Expression)
                        {
                            lambdaBody = Expression.Call(lambdaBody, currentMethodCallExpression.Method, currentMethodCallExpression.Arguments);
                            wasNodeHandled = true;
                        }
                        else if (currentMethodCallExpression.Arguments != null)
                        {
bool didFindArgumentToSwap = false;
List<Expression> newArguments = new List<Expression>();
                            foreach (Expression nextArgument in currentMethodCallExpression.Arguments)
                            {
                                if (nextArgument == previousNode?.Expression)
                                {
                                    newArguments.Add(lambdaBody);
                                    didFindArgumentToSwap = true;
                                    continue;
                                }

                                newArguments.Add(nextArgument);
                            }

                            if (didFindArgumentToSwap)
                            {
                                lambdaBody = Expression.Call(currentMethodCallExpression.Object, currentMethodCallExpression.Method, newArguments);
                                wasNodeHandled = true;
                            }
                        }
                        break;

                    case UnaryExpression currentUnaryExpression:
                        if (currentUnaryExpression.Operand == previousNode?.Expression)
                        {
                            lambdaBody = Expression.MakeUnary(currentUnaryExpression.NodeType, lambdaBody, currentUnaryExpression.Type);
                            wasNodeHandled = true;
                        }
                        break;

                    case BinaryExpression currentBinaryExpression:
                        if (currentBinaryExpression.Left == previousNode?.Expression)
                        {
                            lambdaBody = Expression.MakeBinary(currentBinaryExpression.NodeType, lambdaBody, currentBinaryExpression.Right);
                            wasNodeHandled = true;
                        }
                        else if (currentBinaryExpression.Right == previousNode?.Expression)
                        {
                            lambdaBody = Expression.MakeBinary(currentBinaryExpression.NodeType, currentBinaryExpression.Left, lambdaBody);
                            wasNodeHandled = true;
                        }
                        break;

                    case LambdaExpression currentLambdaExpression:
                        if (currentLambdaExpression.Body == previousNode?.Expression)
                        {
                            lambdaBody = Expression.Lambda(lambdaBody, currentLambdaExpression.Parameters);
                            wasNodeHandled = true;
                        }
                        break;

                    case ConditionalExpression currentConditionalExpression:
                        if (currentConditionalExpression.Test == previousNode?.Expression)
                        {
                            lambdaBody = Expression.Condition(lambdaBody, currentConditionalExpression.IfTrue, currentConditionalExpression.IfFalse, currentConditionalExpression.Type);
                            wasNodeHandled = true;
                        }
                        else if (currentConditionalExpression.IfTrue == previousNode?.Expression)
                        {
                            lambdaBody = Expression.Condition(currentConditionalExpression.Test, lambdaBody, currentConditionalExpression.IfFalse, currentConditionalExpression.Type);
                            wasNodeHandled = true;
                        }
                        else if (currentConditionalExpression.IfFalse == previousNode?.Expression)
                        {
                            lambdaBody = Expression.Condition(currentConditionalExpression.Test, currentConditionalExpression.IfTrue, lambdaBody, currentConditionalExpression.Type);
                            wasNodeHandled = true;
                        }
                        break;

                    default:
                        break;
                }

                if (!wasNodeHandled)
                {
                    return false;
                }

                previousNode = currentNode;
                currentNode = currentNode.Parent;
            }

            innerQueryable = dbSetQueryable?.Provider.CreateQuery(Expression.Call(
                null,
                QueryableMethods.Select.MakeGenericMethod(firstNavigationType, memberPropertyInfo.PropertyType),
                dbSetQueryable.Expression,
                Expression.Lambda(lambdaBody, rootParameter)
            ));
        }

        if (innerQueryable is null)
        {
            throw new ArgumentException("InnerQuerable is null");
        }

        (string innerSql, IEnumerable<DbParameter> innerSqlParametersEnumerable) = innerQueryable.ToParametrizedSql(createBodyData.DbContext);
        IEnumerable<object> innerSqlParameters = innerSqlParametersEnumerable;

        innerSql = innerSql.Trim();

        string? firstNavigationAlias = null;
string rootTableNameWithBrackets = $"[{rootTypeTableInfo?.TableName}]";
string rootTableAliasWithBrackets = $"[{createBodyData.TableAlias}]";
string firstNavigationTableNameWithBrackets = $"[{firstNavigationTableName}]";
        foreach (Match match in TableAliasPattern.Matches(innerSql))
        {
string tableName = match.Groups[1].Value;
string originalAlias = match.Groups[2].Value;

            if ((isFirstNavigationACollectionType ?? false)
                && tableName.Equals(rootTableNameWithBrackets, StringComparison.OrdinalIgnoreCase)
                && originalAlias.Equals(rootTableAliasWithBrackets, StringComparison.OrdinalIgnoreCase))
            {
                // Don't rename this alias, and cut off the unnecessary FROM clause
                innerSql = innerSql[..match.Index];
                continue;
            }

            if (!createBodyData.TableAliasesInUse.Contains(originalAlias))
            {
                if (tableName.Equals(firstNavigationTableNameWithBrackets, StringComparison.OrdinalIgnoreCase))
                {
                    firstNavigationAlias = originalAlias;
                }

                createBodyData.TableAliasesInUse.Add(originalAlias);
                continue;
            }

int aliasIndex = -1;
string aliasPrefix = originalAlias[0..^1];
            string newAlias;
            do
            {
                ++aliasIndex;
                newAlias = $"{aliasPrefix}{aliasIndex}]";
            }
            while (createBodyData.TableAliasesInUse.Contains(newAlias));

            createBodyData.TableAliasesInUse.Add(newAlias);
            innerSql = innerSql.Replace(originalAlias, newAlias);

            if (tableName.Equals(firstNavigationTableNameWithBrackets, StringComparison.OrdinalIgnoreCase))
            {
                firstNavigationAlias = newAlias;
            }
        }

        if (isFirstNavigationACollectionType ?? false)
        {
            innerSql = innerSql[6..].Trim();
            if (innerSql.StartsWith("("))
            {
                createBodyData.UpdateColumnsSql.Append(' ').Append(innerSql);
            }
            else
            {
                createBodyData.UpdateColumnsSql.Append(" (").Append(innerSql).Append(')');
            }

            createBodyData.SqlParameters.AddRange(innerSqlParameters);
            return true;
        }

StringBuilder whereClauseCondition = new StringBuilder("WHERE ");
IReadOnlyList<IProperty>? dependencyKeyProperties = firstNavigation?.ForeignKey.Properties;
IReadOnlyList<IProperty>? principalKeyProperties = firstNavigation?.ForeignKey.PrincipalKey.Properties;
Dictionary<string, string>? navigationColumnFastLookup = createBodyData?.GetTableInfoForType(firstNavigationType!)?.PropertyColumnNamesDict;
Dictionary<string, string>? columnNameValueDict = rootTypeTableInfo?.PropertyColumnNamesDict;
string? rootTableAlias = createBodyData?.TableAlias;
        if (firstNavigation?.IsOnDependent ?? false)
        {
            for (int keyIndex = 0; keyIndex < dependencyKeyProperties?.Count; ++keyIndex)
            {
                if (keyIndex > 0)
                {
                    whereClauseCondition.Append(" AND ");
                }

string dependencyColumnName = navigationColumnFastLookup![dependencyKeyProperties[keyIndex].Name];
string principalColumnName = columnNameValueDict![principalKeyProperties![keyIndex].Name];
                whereClauseCondition.Append(firstNavigationAlias).Append(".[").Append(principalColumnName).Append("] = [")
                    .Append(rootTableAlias).Append("].[").Append(dependencyColumnName).Append(']');
            }
        }
        else
        {
            for (int keyIndex = 0; keyIndex < dependencyKeyProperties?.Count; ++keyIndex)
            {
                if (keyIndex > 0)
                {
                    whereClauseCondition.Append(" AND ");
                }

string dependencyColumnName = navigationColumnFastLookup![dependencyKeyProperties[keyIndex].Name];
string principalColumnName = columnNameValueDict![principalKeyProperties![keyIndex].Name];
                whereClauseCondition.Append(firstNavigationAlias).Append(".[").Append(dependencyColumnName).Append("] = [")
                    .Append(rootTableAlias).Append("].[").Append(principalColumnName).Append(']');
            }
        }

int whereClauseIndex = innerSql.LastIndexOf("WHERE ", StringComparison.OrdinalIgnoreCase);
        if (whereClauseIndex > -1)
        {
            innerSql = innerSql[..whereClauseIndex] + whereClauseCondition.ToString() + "AND " + innerSql[(whereClauseIndex + 5)..];
        }
        else
        {
int orderByIndex = innerSql.LastIndexOf("ORDER BY ", StringComparison.OrdinalIgnoreCase);
            if (orderByIndex > -1)
            {
                innerSql = innerSql[..orderByIndex] + '\n' + whereClauseCondition.ToString() + '\n' + innerSql[orderByIndex..];
            }
            else
            {
                innerSql = innerSql + '\n' + whereClauseCondition.ToString();
            }

        }

        createBodyData?.UpdateColumnsSql.Append(" (\n    ").Append(innerSql.Replace("\n", "\n    ")).Append(')');
        createBodyData?.SqlParameters.AddRange(innerSqlParameters);

        return true;
    }

    private static DbParameter CloneDbParameter(DbConnection connection, DbParameter source)
    {
        DbCommand tempCommand = connection.CreateCommand();
        try
        {
            DbParameter clone = tempCommand.CreateParameter();
            clone.ParameterName = source.ParameterName;
            clone.Value = source.Value;
            clone.DbType = source.DbType;
            clone.Direction = source.Direction;
            clone.IsNullable = source.IsNullable;
            clone.Size = source.Size;
            clone.Precision = source.Precision;
            clone.Scale = source.Scale;

            return clone;
        }
        finally
        {
            tempCommand.Dispose();
        }
    }

    /// <summary>
    /// ExpressionNode
    /// </summary>
    public class ExpressionNode
    {
#pragma warning disable CS1591 // No XML comment required here. Used internally only
        public ExpressionNode (Expression expression, ExpressionNode? parent)
        {
            Expression = expression;
            Parent = parent;
        }

        public Expression Expression { get; }
        public ExpressionNode? Parent { get; }
#pragma warning restore CS1591 // No XML comment required here. Used internally only
    }
}
