using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Contains a list of SQL operations
/// </summary>
public interface ISqlOperationsAdapter
{
    /// <summary>
    /// Inserts a list of entities
    /// </summary>
    void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress);

    /// <summary>
    /// Inserts a list of entities
    /// </summary>
    Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken);

    /// <summary>
    /// Merges a list of entities with a table source
    /// </summary>
    void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress) where T : class;

    /// <summary>
    /// Merges a list of entities with a table source
    /// </summary>
    Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, CancellationToken cancellationToken) where T : class;

    /// <summary>
    /// Reads a list of entities from database
    /// </summary>
    void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class;

    /// <summary>
    /// Reads a list of entities from database
    /// </summary>
    Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class;

    /// <summary>
    /// Truncates a table
    /// </summary>
    void Truncate(DbContext context, TableInfo tableInfo);

    /// <summary>
    /// Truncates a table
    /// </summary>
    Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken);
}
