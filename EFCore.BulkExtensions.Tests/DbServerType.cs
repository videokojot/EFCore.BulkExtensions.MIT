using System.ComponentModel;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// A list of database servers targeted by the EFCore.BulkExtensions test suite
/// </summary>
public enum DbServerType
{
    [Description("SqlServer")] SQLServer,
    [Description("SQLite")] SQLite,
    [Description("PostgreSql")] PostgreSQL,
    [Description("MySql")] MySQL,
}
