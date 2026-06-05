using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;

namespace EFCore.BulkExtensions;

#pragma warning disable CS1591 // No XML comments required here
public static class DbContextUnderlyingExtensions
{
    public static DbConnection GetUnderlyingConnection(this DbContext context, BulkConfig config)
    {
        DbConnection connection = context.Database.GetDbConnection();

        if (config?.UnderlyingConnection != null)
        {
            connection = config.UnderlyingConnection(connection);
        }

        DbConnection result = connection;
        return result;
    }

    public static DbTransaction GetUnderlyingTransaction(this IDbContextTransaction ctxTransaction, BulkConfig config)
    {
        DbTransaction dbTransaction = ctxTransaction.GetDbTransaction();

        if (config?.UnderlyingTransaction != null)
        {
            dbTransaction = config.UnderlyingTransaction(dbTransaction);
        }

        DbTransaction result = dbTransaction;
        return result;
    }
}
#pragma warning restore CS1591 // No XML comments required here

