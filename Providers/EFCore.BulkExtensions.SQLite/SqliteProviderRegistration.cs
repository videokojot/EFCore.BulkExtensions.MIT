using System;
using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SqlAdapters.SQLite;

namespace EFCore.BulkExtensions.Providers;

internal static class SqliteProviderRegistration
{
    internal static void Register()
    {
        Func<string, bool> matcher = CanHandle;
        Func<IDbServer> factory = Create;
        DbServerRegistry.Register(matcher, factory);
    }

    private static bool CanHandle(string providerName)
    {
        bool canHandle = providerName.EndsWith("sqlite", StringComparison.OrdinalIgnoreCase);

        return canHandle;
    }

    private static IDbServer Create()
    {
        SqlLiteDbServer server = new SqlLiteDbServer();

        return server;
    }
}
