namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

public sealed class SqlServerDialect : SqlDefaultDialect
{
    public override char EscL => '[';

    public override char EscR => ']';

    public override string? DefaultSchema => "dbo";
}
