using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

public sealed class SqlServerDbServer : IDbServer
{
    SqlOperationsServerAdapter _adapter = new();

    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    SqlServerDialect _dialect = new();

    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    public DbConnection? DbConnection { get; set; }

    public DbTransaction? DbTransaction { get; set; }

    QueryBuilderExtensions _queryBuilder = new SqlQueryBuilderSqlServer();

    public QueryBuilderExtensions QueryBuilder => _queryBuilder;

    bool IDbServer.PropertyHasIdentity(IProperty property)
    {
        SqlServerValueGenerationStrategy strategy = SqlServerPropertyExtensions.GetValueGenerationStrategy(property);
        bool hasIdentity = strategy == SqlServerValueGenerationStrategy.IdentityColumn;

        return hasIdentity;
    }
}
