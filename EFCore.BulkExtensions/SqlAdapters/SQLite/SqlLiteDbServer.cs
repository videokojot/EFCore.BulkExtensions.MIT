using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SqlAdapters.SQLite;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.SQLite;

public class SqlLiteDbServer : IDbServer
{
    DbServerType IDbServer.Type => DbServerType.SQLite;

    SqliteOperationsAdapter _adapter = new ();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    SqliteDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    public DbConnection? DbConnection { get; set; }

    public DbTransaction? DbTransaction { get; set; }

    SqlAdapters.QueryBuilderExtensions _queryBuilder = new SqlQueryBuilderSqlite();
    public QueryBuilderExtensions QueryBuilder => _queryBuilder;


    bool IDbServer.PropertyHasIdentity(IProperty property) => false;
}
