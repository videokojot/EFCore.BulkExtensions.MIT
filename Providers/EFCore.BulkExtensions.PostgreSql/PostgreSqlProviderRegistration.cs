using System;
using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SqlAdapters.PostgreSql;

namespace EFCore.BulkExtensions.Providers;

internal static class PostgreSqlProviderRegistration
{
    internal static void Register()
    {
        Func<string, bool> matcher = CanHandle;
        Func<IDbServer> factory = Create;
        DbServerRegistry.Register(matcher, factory);
    }

    private static bool CanHandle(string providerName)
    {
        bool canHandle = providerName.EndsWith("postgresql", StringComparison.OrdinalIgnoreCase);

        return canHandle;
    }

    private static IDbServer Create()
    {
        PostgreSqlDbServer server = new PostgreSqlDbServer();

        return server;
    }
}
