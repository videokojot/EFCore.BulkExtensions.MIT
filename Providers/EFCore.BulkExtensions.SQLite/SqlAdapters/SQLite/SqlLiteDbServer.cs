using Microsoft.EntityFrameworkCore.Metadata;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.SQLite;

public sealed class SqlLiteDbServer : IDbServer
{
    SqliteOperationsAdapter _adapter = new ();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    SqliteDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    public DbConnection? DbConnection { get; set; }

    public DbTransaction? DbTransaction { get; set; }

    QueryBuilderExtensions _queryBuilder = new SqlQueryBuilderSqlite();
    public QueryBuilderExtensions QueryBuilder => _queryBuilder;


    bool IDbServer.PropertyHasIdentity(IProperty property) => false;
}
