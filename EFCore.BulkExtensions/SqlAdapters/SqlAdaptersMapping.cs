using Microsoft.EntityFrameworkCore;
using System;
using System.ComponentModel;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// A list of database servers supported by EFCore.BulkExtensions
/// </summary>
public enum DbServerType
{
    /// <summary>
    /// Indicates database is Microsoft's SQL Server
    /// </summary>
    [Description("SqlServer")] SQLServer,

    /// <summary>
    /// Indicates database is SQL Lite
    /// </summary>
    [Description("SQLite")] SQLite,

    /// <summary>
    /// Indicates database is Postgres
    /// </summary>
    [Description("PostgreSql")] PostgreSQL,

    /// <summary>
    ///  Indicates database is MySQL
    /// </summary>
    [Description("MySql")] MySQL,
}

public static class SqlAdaptersMapping
{
    private static IDbServer? _sqlLite;
    private static IDbServer? _msSql;
    private static IDbServer? _mySql;
    private static IDbServer? _postgreSql;

    /// <summary>
    /// Contains a list of methods to generate Adapters and helpers instances
    /// </summary>
    public static IDbServer DbServer(this DbContext dbContext)
    {
        //Context.Database. methods: -IsSqlServer() -IsNpgsql() -IsMySql() -IsSqlite() requires specific provider so instead here used -ProviderName
        
        var providerName = dbContext.Database.ProviderName;
        Type? dbServerType;
        var efCoreBulkExtensionsSqlAdaptersText = "EFCore.BulkExtensions.SqlAdapters";
        
        IDbServer dbServerInstance;

        if (providerName?.ToLower().EndsWith(DbServerType.PostgreSQL.ToString().ToLower()) ?? false)
        {
            dbServerType = Type.GetType(efCoreBulkExtensionsSqlAdaptersText + ".PostgreSql.PostgreSqlDbServer");
            dbServerInstance = _postgreSql ??= (IDbServer)Activator.CreateInstance(dbServerType ?? typeof(int))!;    
        }
        else if (providerName?.ToLower().EndsWith(DbServerType.MySQL.ToString().ToLower()) ?? false)
        {
            dbServerType = Type.GetType(efCoreBulkExtensionsSqlAdaptersText + ".MySql.MySqlDbServer");
            dbServerInstance = _mySql ??= (IDbServer)Activator.CreateInstance(dbServerType ?? typeof(int))!;

        }
        else if (providerName?.ToLower().EndsWith(DbServerType.SQLite.ToString().ToLower()) ?? false)
        {
            dbServerType = Type.GetType(efCoreBulkExtensionsSqlAdaptersText + ".SQLite.SqlLiteDbServer");
            dbServerInstance = _sqlLite ??= (IDbServer)Activator.CreateInstance(dbServerType ?? typeof(int))!;
        }
        else
        {
            dbServerType = Type.GetType(efCoreBulkExtensionsSqlAdaptersText + ".SqlServer.SqlServerDbServer");
            dbServerInstance = _msSql ??= (IDbServer)Activator.CreateInstance(dbServerType ?? typeof(int))!;
        }

        return dbServerInstance;
    }

    /// <summary>
    /// Creates the bulk operations adapter
    /// </summary>
    /// <returns></returns>
    public static ISqlOperationsAdapter CreateBulkOperationsAdapter(this DbContext dbContext) => DbServer(dbContext).Adapter;

    /// <summary>
    /// Returns the Adapter dialect to be used
    /// </summary>
    /// <returns></returns>
    public static IQueryBuilderSpecialization GetAdapterDialect(this DbContext dbContext) => DbServer(dbContext).Dialect;

    /// <summary>
    /// Returns the Database type
    /// </summary>
    /// <returns></returns>
    public static DbServerType GetDatabaseType(this DbContext dbContext) => DbServer(dbContext).Type;
}
