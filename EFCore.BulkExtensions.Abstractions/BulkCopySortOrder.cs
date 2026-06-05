namespace EFCore.BulkExtensions;

/// <summary>
/// Sort order for bulk copy column hints. Numeric values match <c>System.Data.SortOrder</c> for SQL Server conversion.
/// </summary>
public enum BulkCopySortOrder
{
    Ascending = 0,
    Descending = 1,
}
