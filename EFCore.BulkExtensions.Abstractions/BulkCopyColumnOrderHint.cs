namespace EFCore.BulkExtensions;

/// <summary>
/// Column order hint for bulk copy operations (provider-specific implementations may apply this).
/// </summary>
public sealed class BulkCopyColumnOrderHint
{
    public string ColumnName { get; set; } = string.Empty;

    public BulkCopySortOrder SortOrder { get; set; }
}
