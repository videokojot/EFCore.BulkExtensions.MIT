using DotNet.Testcontainers.Containers;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using System;
using Testcontainers.MsSql;
using Testcontainers.MySql;
using Testcontainers.PostgreSql;

namespace EFCore.BulkExtensions.Tests;

public class DbAssemblyFixture : IDisposable
{
    private static bool _fixtureRequested;

    private static MsSqlContainer? _msSqlContainer;
    private static PostgreSqlContainer? _postgreContainer;
    private static MySqlContainer? _mySqlContainer;

    public DbAssemblyFixture()
    {
        if (_fixtureRequested)
        {
            throw new InvalidOperationException("Container is created more than once!");
        }

        _fixtureRequested = true;
    }


    public void Dispose()
    {
        _msSqlContainer?.DisposeAsync().GetAwaiter().GetResult();
        _postgreContainer?.DisposeAsync().GetAwaiter().GetResult();
        _mySqlContainer?.DisposeAsync().GetAwaiter().GetResult();
    }

    public static string GetConnectionString(DbServerType dbServerType, string databaseName)
    {
        if (!_fixtureRequested)
        {
            throw new InvalidOperationException("Fixture would not be disposed - mark test with: IAssemblyFixture<DbAssemblyFixture>");
        }

        lock (_locker)
        {
            // Initialize container if needed
            DockerContainer container;

            switch (dbServerType)
            {
                case DbServerType.SQLServer:
                    container = _msSqlContainer ??= new MsSqlBuilder().Build();

                    break;
                case DbServerType.PostgreSQL:
                    container = _postgreContainer ??= new PostgreSqlBuilder().Build();

                    break;
                case DbServerType.MySQL:
                    container = _mySqlContainer ??= new MySqlBuilder().WithUsername("root").Build();

                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(dbServerType), dbServerType, null);
            }

            // start if not started
            if (container.State != TestcontainersStates.Running)
            {
                container.StartAsync().GetAwaiter().GetResult();
            }

            if (dbServerType == DbServerType.MySQL)
            {
                _mySqlContainer!.ExecScriptAsync("SET GLOBAL local_infile = true;").GetAwaiter().GetResult();
            }

            return container switch
            {
                MsSqlContainer msSqlContainer => EnhanceConnectionStringSqlServer(msSqlContainer),
                PostgreSqlContainer postgreSqlContainer => EnhanceConnectionStringPostgre(postgreSqlContainer),
                MySqlContainer mySqlContainer => EnhanceConnectionStringMySql(mySqlContainer),
                _ => throw new InvalidOperationException($"Unknown container type {dbServerType}.")
            };

            string EnhanceConnectionStringSqlServer(MsSqlContainer msSqlContainer)
            {
                var builder = new SqlConnectionStringBuilder(msSqlContainer.GetConnectionString());
                builder.InitialCatalog = databaseName;
                builder.MultipleActiveResultSets = true;

                return builder.ToString();
            }

            string EnhanceConnectionStringPostgre(PostgreSqlContainer postgreSqlContainer)
            {
                var builder = new NpgsqlConnectionStringBuilder(postgreSqlContainer.GetConnectionString());
                builder.Database = databaseName;

                return builder + ";Include Error Detail=True";
            }

            string EnhanceConnectionStringMySql(MySqlContainer mySqlContainer)
            {
                var builder = new MySqlConnectionStringBuilder(mySqlContainer.GetConnectionString());
                builder.Database = databaseName;
                builder.AllowLoadLocalInfile = true;

                return builder.ToString();
            }
        }
    }

    private static readonly object _locker = new();
}
