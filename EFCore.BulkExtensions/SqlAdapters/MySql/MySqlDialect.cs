namespace EFCore.BulkExtensions.SqlAdapters.MySql;

public class MySqlDialect : SqlDefaultDialect
{
    public override char EscL => '`';

    public override char EscR => '`';
}
