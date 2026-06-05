using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

public sealed class MySqlDbServer : IDbServer
{
    MySqlAdapter _adapter = new();

    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    MySqlDialect _dialect = new();

    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    QueryBuilderExtensions _queryBuilder = new SqlQueryBuilderMySql();

    /// <inheritdoc/>
    public QueryBuilderExtensions QueryBuilder => _queryBuilder;

    public DbConnection? DbConnection { get; set; }

    public DbTransaction? DbTransaction { get; set; }

    bool IDbServer.PropertyHasIdentity(IProperty property)
    {
        MySqlValueGenerationStrategy strategy = MySqlPropertyExtensions.GetValueGenerationStrategy(property);
        bool hasIdentity = strategy == MySqlValueGenerationStrategy.IdentityColumn;

        return hasIdentity;
    }
}