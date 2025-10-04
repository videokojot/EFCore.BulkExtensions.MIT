using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SqlAdapters.MySql;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

public class MySqlDbServer : IDbServer
{
    DbServerType IDbServer.Type => DbServerType.MySQL;

    MySqlAdapter _adapter = new();

    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    MySqlDialect _dialect = new();

    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    SqlAdapters.QueryBuilderExtensions _queryBuilder = new SqlQueryBuilderMySql();

    /// <inheritdoc/>
    public QueryBuilderExtensions QueryBuilder => _queryBuilder;

    public DbConnection? DbConnection { get; set; }

    public DbTransaction? DbTransaction { get; set; }

    bool IDbServer.PropertyHasIdentity(IProperty property)
    {
#if V6
        return false;
#endif

        return MySqlPropertyExtensions.GetValueGenerationStrategy(property) == MySqlValueGenerationStrategy.IdentityColumn;
    }
}
