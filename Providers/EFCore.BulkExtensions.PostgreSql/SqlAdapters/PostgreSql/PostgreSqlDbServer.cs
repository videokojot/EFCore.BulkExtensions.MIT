using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.Internal;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.PostgreSql;

/// <inheritdoc/>
public sealed class PostgreSqlDbServer : IDbServer
{
    PostgreSqlAdapter _adapter = new();

    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    PostgreSqlDialect _dialect = new();

    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    /// <inheritdoc/>
    public DbConnection? DbConnection { get; set; }

    /// <inheritdoc/>
    public DbTransaction? DbTransaction { get; set; }

    QueryBuilderExtensions _queryBuilder = new SqlQueryBuilderPostgreSql();

    /// <inheritdoc/>
    public QueryBuilderExtensions QueryBuilder => _queryBuilder;

    bool IDbServer.PropertyHasIdentity(IProperty property)
    {
        IAnnotation? annotation = property.FindAnnotation(NpgsqlAnnotationNames.ValueGenerationStrategy);

        if (annotation == null)
        {
            return false;
        }

        NpgsqlValueGenerationStrategy? strategy = (NpgsqlValueGenerationStrategy?)annotation.Value;
        bool hasIdentity = strategy == NpgsqlValueGenerationStrategy.IdentityByDefaultColumn;

        return hasIdentity;
    }
}
