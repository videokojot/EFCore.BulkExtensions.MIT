using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions;

/// <summary>
/// Contains a list of Batch IQuerable extensions
/// </summary>
public static class IQueryableBatchExtensions
{
    // Delete methods
    #region BatchDelete
    /// <summary>
    /// Extension method to batch delete data
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    public static int BatchDelete(this IQueryable query)
    {
        (DbContext context, string sql, List<object> sqlParameters) = GetBatchDeleteArguments(query);
        int result = context.Database.ExecuteSqlRaw(sql, sqlParameters);
        return result;
    }

    /// <summary>
    /// Extension method to batch delete data
    /// </summary>
    /// <param name="query"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static async Task<int> BatchDeleteAsync(this IQueryable query, CancellationToken cancellationToken = default)
    {
        (DbContext context, string sql, List<object> sqlParameters) = GetBatchDeleteArguments(query);
        int result = await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        return result;
    }

    private static (DbContext, string, List<object>) GetBatchDeleteArguments(IQueryable query)
    {
        DbContext? context = BatchUtil.GetDbContext(query);

        if (context is null)
        {
            throw new ArgumentException("Unable to determine context");
        }
        (string sql, List<object> sqlParameters) = BatchUtil.GetSqlDelete(query, context);
        (DbContext, string, List<object>) result = (context, sql, sqlParameters);
        return result;
    }
    #endregion

    // Update methods
    #region BatchUpdate
    /// <summary>
    /// Extension method to batch update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="query"></param>
    /// <param name="updateValues"></param>
    /// <param name="updateColumns"></param>
    /// <returns></returns>
    public static int BatchUpdate<T>(this IQueryable<T> query, object updateValues, List<string> ?updateColumns = null) where T : class
    {
        (DbContext context, string sql, List<object> sqlParameters) = GetBatchUpdateArguments(query, updateValues, updateColumns);
        int result = context.Database.ExecuteSqlRaw(sql, sqlParameters);
        return result;
    }

    /// <summary>
    /// Extension method to batch update data
    /// </summary>
    /// <param name="query"></param>
    /// <param name="updateValues"></param>
    /// <param name="updateColumns"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static async Task<int> BatchUpdateAsync(this IQueryable query, object updateValues, List<string>? updateColumns = null, CancellationToken cancellationToken = default)
    {
        (DbContext context, string sql, List<object> sqlParameters) = GetBatchUpdateArguments((IQueryable<object>)query, updateValues, updateColumns);
        int result = await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        return result;
    }

    /// <summary>
    /// Extension method to batch update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="query"></param>
    /// <param name="updateExpression"></param>
    /// <param name="type"></param>
    /// <returns></returns>
    public static int BatchUpdate<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression, Type? type = null) where T : class
    {
        (DbContext context, string sql, List<object> sqlParameters) = GetBatchUpdateArguments(query, updateExpression: updateExpression, type: type);
        int result = context.Database.ExecuteSqlRaw(sql, sqlParameters);
        return result;
    }

    /// <summary>
    /// Extension method to batch update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="query"></param>
    /// <param name="updateExpression"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static async Task<int> BatchUpdateAsync<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        (DbContext context, string sql, List<object> sqlParameters) = GetBatchUpdateArguments(query, updateExpression: updateExpression, type: type);
        int result = await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        return result;
    }

    private static (DbContext, string, List<object>) GetBatchUpdateArguments<T>(IQueryable<T> query, object? updateValues = null, List<string>? updateColumns = null, Expression<Func<T, T>>? updateExpression = null, Type? type = null) where T : class
    {
        if (type == null)
        {
            type = typeof(T);
        }
        DbContext? context = BatchUtil.GetDbContext(query);

        if (context is null)
        {
            throw new ArgumentException("Unable to determine context");
        }
        string sql;
        List<object> sqlParameters;
        if (updateExpression == null)
        {
            (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, type, updateValues, updateColumns);
        }
        else
        {
            (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, type, updateExpression);
        }
        (DbContext, string, List<object>) result = (context, sql, sqlParameters);
        return result;
    }
    #endregion
}
