using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

/// <summary>
/// Expected to be used as fixture in XUnit tests. For example usage see: EfCoreBulkInsertOrUpdateTests.cs
/// </summary>
public abstract class BulkDbTestsFixture<TDbContext> : IDisposable
    where TDbContext : DbContext
{
    protected abstract string DbName { get; }

    private readonly HashSet<DbServerType> _initialized = new();

    public TDbContext GetDb(DbServerType sqlType)
    {
        var shouldInitialize = _initialized.Add(sqlType);

        if (shouldInitialize)
        {
            using var db = CreateContextInternal(sqlType);

            db.Database.EnsureDeleted();
            db.Database.EnsureCreated();
        }

        return CreateContextInternal(sqlType);
    }

    private TDbContext CreateContextInternal(DbServerType sqlType)
    {
        var options = ContextUtil.GetOptions<TDbContext>(sqlType, databaseName: DbName);

        var ctorWithSingleDbOptionsParameters = typeof(TDbContext).GetConstructors()
                                                                  .SingleOrDefault(x => x.GetParameters().Length == 1 && x.GetParameters().Single().ParameterType == typeof(DbContextOptions));

        if (ctorWithSingleDbOptionsParameters is null)
        {
            throw new InvalidOperationException($"Type {typeof(TDbContext)}  must have ctor with single DbContextOptions parameter.");
        }

        return (TDbContext)ctorWithSingleDbOptionsParameters.Invoke(new object?[] { options });
    }

    public void Dispose()
    {
        // Drop all dbs which were initialized:
        foreach (var sqlType in _initialized)
        {
            using var db = GetDb(sqlType);
            db.Database.EnsureDeleted();
        }
    }
}
