using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;

namespace EFCore.BulkExtensions;

/// <summary>
/// Contains a list of IQuerable extensions
/// </summary>
public static class IQueryableExtensions
{
    /// <summary>
    /// Extension method to paramatize sql query
    /// </summary>
    /// <param name="query"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static (string Sql, IEnumerable<DbParameter> Parameters) ToParametrizedSql(this IQueryable query, DbContext context)
    {
        string relationalQueryContextText = "_relationalQueryContext";
        string relationalCommandCacheText = "_relationalCommandCache";
        string relationalCommandResolverText = "_relationalCommandResolver";

        string cannotGetText = "Cannot get";

        IEnumerator enumerator = query.Provider.Execute<IEnumerable>(query.Expression).GetEnumerator();

        RelationalQueryContext queryContext = enumerator.Private<RelationalQueryContext>(relationalQueryContextText) ?? throw new InvalidOperationException($"{cannotGetText} {relationalQueryContextText}");

#if NET10_0_OR_GREATER
        IReadOnlyDictionary<string, object?> parameterValuesReadOnly = queryContext.Parameters;
#else
        IReadOnlyDictionary<string, object?> parameterValuesReadOnly = queryContext.ParameterValues;
#endif
        Dictionary<string, object?> parameterValues = parameterValuesReadOnly.ToDictionary(x => x.Key, x => x.Value);


        #pragma warning disable EF1001 // Internal EF Core API usage.
        RelationalCommandCache? relationalCommandCache = (RelationalCommandCache?)enumerator.Private(relationalCommandCacheText);
        Delegate? relationalCommandResolver = enumerator.Private<Delegate>(relationalCommandResolverText);
        #pragma warning restore EF1001

        IRelationalCommand? command = null;

        if (relationalCommandCache != null)
        {
            #pragma warning disable EF1001 // Internal EF Core API usage.
            command = (IRelationalCommand)relationalCommandCache.GetRelationalCommandTemplate(parameterValues);
            #pragma warning restore EF1001
        }

        if (command == null && relationalCommandResolver != null)
        {
            #pragma warning disable EF1001 // Internal EF Core API usage.
            command = (IRelationalCommand?)relationalCommandResolver.DynamicInvoke(parameterValues);
            #pragma warning restore EF1001
        }
        
        if (command == null)
        {
            string selectExpressionText = "_selectExpression";
            string querySqlGeneratorFactoryText = "_querySqlGeneratorFactory";
            SelectExpression selectExpression = enumerator.Private<SelectExpression>(selectExpressionText) ?? throw new InvalidOperationException($"{cannotGetText} {selectExpressionText}");
            IQuerySqlGeneratorFactory factory = enumerator.Private<IQuerySqlGeneratorFactory>(querySqlGeneratorFactoryText) ?? throw new InvalidOperationException($"{cannotGetText} {querySqlGeneratorFactoryText}");
            command = factory.Create().GetCommand(selectExpression);
        }

        string sql = command.CommandText;

        List<DbParameter> parameters;

        DbConnection connection = context.Database.GetDbConnection();
        using DbCommand dbCommand = connection.CreateCommand();

        try
        {
            foreach (IRelationalParameter param in command.Parameters)
            {
                object? values = parameterValues[param.InvariantName];
                param.AddDbParameter(dbCommand, values);
            }

            parameters = new List<DbParameter>();
            foreach (DbParameter parameter in dbCommand.Parameters)
            {
                parameters.Add(CloneDbParameter(connection, parameter));
            }
        }
        catch (Exception ex) // Fix for BatchDelete with 'uint' param on Sqlite. TEST: RunBatchUint
        {
            string npgsqlSpecParamMessage = "Npgsql-specific type mapping ";
            bool isDbTypeMappingError = ex.Message.StartsWith("No mapping exists from DbType") && ex.Message.EndsWith("to a known SqlDbType.");
            bool isNpgsqlParamError = ex.Message.StartsWith(npgsqlSpecParamMessage);
            if (isDbTypeMappingError || isNpgsqlParamError) // Fix for BatchDelete with Contains on PostgreSQL
            {
                HashSet<string> parameterNames = new HashSet<string>(command.Parameters.Select(p => p.InvariantName));
                parameters = new List<DbParameter>();
                foreach (KeyValuePair<string, object?> parameterValue in parameterValues.Where(pv => parameterNames.Contains(pv.Key)))
                {
                    DbParameter parameter = dbCommand.CreateParameter();
                    parameter.ParameterName = "@" + parameterValue.Key;
                    parameter.Value = parameterValue.Value ?? DBNull.Value;
                    parameters.Add(parameter);
                }
            }
            else
            {
                throw;
            }
        }

        (string Sql, IEnumerable<DbParameter> Parameters) result = (sql, parameters);
        return result;
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

    private static readonly BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.NonPublic;

    private static object? Private(this object obj, string privateField)
    {
        object? result = obj?.GetType().GetField(privateField, bindingFlags)?.GetValue(obj);
        return result;
    }

    private static T? Private<T>(this object obj, string privateField)
    {
        T? result = (T?)obj?.GetType().GetField(privateField, bindingFlags)?.GetValue(obj);
        return result;
    }
}
