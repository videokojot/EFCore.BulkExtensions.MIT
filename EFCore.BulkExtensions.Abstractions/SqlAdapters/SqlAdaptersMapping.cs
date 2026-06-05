using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters;

public static class SqlAdaptersMapping
{
    public static IDbServer DbServer(this DbContext dbContext)
    {
        string? providerName = dbContext.Database.ProviderName;
        IDbServer dbServerInstance = DbServerRegistry.Resolve(providerName);

        return dbServerInstance;
    }

    public static ISqlOperationsAdapter CreateBulkOperationsAdapter(this DbContext dbContext)
    {
        ISqlOperationsAdapter adapter = DbServer(dbContext).Adapter;

        return adapter;
    }

    public static IQueryBuilderSpecialization GetAdapterDialect(this DbContext dbContext)
    {
        IQueryBuilderSpecialization dialect = DbServer(dbContext).Dialect;

        return dialect;
    }
}