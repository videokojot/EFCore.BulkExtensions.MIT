using System;
using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SqlAdapters.MySql;

namespace EFCore.BulkExtensions.Providers;

internal static class MySqlProviderRegistration
{
    internal static void Register()
    {
        Func<string, bool> matcher = CanHandle;
        Func<IDbServer> factory = Create;
        DbServerRegistry.Register(matcher, factory);
    }

    private static bool CanHandle(string providerName)
    {
        bool canHandle = providerName.EndsWith("mysql", StringComparison.OrdinalIgnoreCase);

        return canHandle;
    }

    private static IDbServer Create()
    {
        MySqlDbServer server = new MySqlDbServer();

        return server;
    }
}
