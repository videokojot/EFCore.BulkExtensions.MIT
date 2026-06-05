namespace EFCore.BulkExtensions.SqlAdapters.MySql;

public sealed class MySqlDialect : SqlDefaultDialect
{
    public override char EscL => '`';

    public override char EscR => '`';
}