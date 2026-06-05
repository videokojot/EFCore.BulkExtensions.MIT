using System;
using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SqlAdapters.SqlServer;

namespace EFCore.BulkExtensions.Providers;

internal static class SqlServerProviderRegistration
{
    internal static void Register()
    {
        Func<string, bool> matcher = CanHandle;
        Func<IDbServer> factory = Create;
        DbServerRegistry.Register(matcher, factory, isFallback: true);
    }

    private static bool CanHandle(string providerName)
    {
        bool canHandle = providerName.EndsWith("sqlserver", StringComparison.OrdinalIgnoreCase);

        return canHandle;
    }

    private static IDbServer Create()
    {
        SqlServerDbServer server = new SqlServerDbServer();

        return server;
    }
}
