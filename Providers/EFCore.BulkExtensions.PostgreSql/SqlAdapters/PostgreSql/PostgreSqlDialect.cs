namespace EFCore.BulkExtensions.SqlAdapters.PostgreSql;

public sealed class PostgreSqlDialect : SqlDefaultDialect
{
    public override char EscL => '"';

    public override char EscR => '"';

    public override bool MatchEntitiesByPosition => true;

    protected override string BatchSqlTableAliasSplitEscapeStart => " ";

    protected override string BatchSqlTableAliasSplitEscapeEnd => ".";
}
