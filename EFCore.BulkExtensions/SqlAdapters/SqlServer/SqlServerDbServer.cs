using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

public class SqlServerDbServer : IDbServer
{
    DbServerType IDbServer.Type => DbServerType.SQLServer;

    SqlOperationsServerAdapter _adapter = new();

    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    SqlServerDialect _dialect = new();

    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    public DbConnection? DbConnection { get; set; }

    public DbTransaction? DbTransaction { get; set; }

    SqlAdapters.QueryBuilderExtensions _queryBuilder = new SqlQueryBuilderSqlServer();

    public QueryBuilderExtensions QueryBuilder => _queryBuilder;

    bool IDbServer.PropertyHasIdentity(IProperty property)
    {
        return SqlServerPropertyExtensions.GetValueGenerationStrategy(property) == SqlServerValueGenerationStrategy.IdentityColumn;
    }
}
