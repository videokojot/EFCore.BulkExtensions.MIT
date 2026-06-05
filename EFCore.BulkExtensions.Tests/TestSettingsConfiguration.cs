using Microsoft.Extensions.Configuration;
using System;

namespace EFCore.BulkExtensions.Tests;

internal static class TestSettingsConfiguration
{
    private static readonly Lazy<IConfiguration> Settings = new(Build);

    internal static IConfiguration Instance => Settings.Value;

    public static bool UseLocalDatabases =>
        string.Equals(Environment.GetEnvironmentVariable("EFCORE_BULK_EXTENSIONS_USE_LOCAL_DB"), "true", StringComparison.OrdinalIgnoreCase)
        || string.Equals(Environment.GetEnvironmentVariable("EFCORE_BULK_EXTENSIONS_USE_LOCAL_DB"), "1", StringComparison.OrdinalIgnoreCase);

    public static string GetConnectionString(string name, string databaseName)
    {
        var connectionString = Settings.Value.GetConnectionString(name)
            ?? throw new InvalidOperationException($"Connection string '{name}' is not configured.");

        return connectionString.Replace("{databaseName}", databaseName, StringComparison.Ordinal);
    }

    private static IConfiguration Build()
    {
        var builder = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("testsettings.json", optional: false, reloadOnChange: false)
            .AddJsonFile("testsettings.local.json", optional: true, reloadOnChange: false);

        var settingsProfile = Environment.GetEnvironmentVariable("EFCORE_BULK_EXTENSIONS_TEST_SETTINGS");
        if (string.Equals(settingsProfile, "docker", StringComparison.OrdinalIgnoreCase))
        {
            builder.AddJsonFile("testsettings.docker.json", optional: false, reloadOnChange: false);
        }

        return builder.Build();
    }
}
