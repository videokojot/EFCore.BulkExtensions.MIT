using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.Internal;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.PostgreSql;

/// <inheritdoc/>
public class PostgreSqlDbServer : IDbServer
{
    DbServerType IDbServer.Type => DbServerType.PostgreSQL;

    PostgreSqlAdapter _adapter = new();

    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    PostgreSqlDialect _dialect = new();

    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    /// <inheritdoc/>
    public DbConnection? DbConnection { get; set; }

    /// <inheritdoc/>
    public DbTransaction? DbTransaction { get; set; }

    SqlAdapters.QueryBuilderExtensions _queryBuilder = new SqlQueryBuilderPostgreSql();

    /// <inheritdoc/>
    public QueryBuilderExtensions QueryBuilder => _queryBuilder;

    bool IDbServer.PropertyHasIdentity(IProperty property)
    {
        var annotation = property.FindAnnotation(NpgsqlAnnotationNames.ValueGenerationStrategy);

        if (annotation == null)
        {
            return false;
        }
        
        return (Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.NpgsqlValueGenerationStrategy?)annotation.Value ==
               Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.NpgsqlValueGenerationStrategy.IdentityByDefaultColumn;
    }
}
