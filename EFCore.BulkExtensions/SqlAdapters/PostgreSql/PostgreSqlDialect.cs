namespace EFCore.BulkExtensions.SqlAdapters.PostgreSql;

public class PostgreSqlDialect : SqlDefaultDialect
{
    public override char EscL => '"';

    public override char EscR => '"';
}
