namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary> Contains the table alias and SQL query </summary>
public sealed class ExtractedTableAlias
{
    public string TableAlias { get; set; } = null!;

    public string TableAliasSuffixAs { get; set; } = null!;

    public string Sql { get; set; } = null!;
}