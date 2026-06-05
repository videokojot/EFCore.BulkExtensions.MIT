using System.Collections.Generic;

namespace EFCore.BulkExtensions;

/// <summary>
/// Class to provide information about how many records have been updated, deleted and inserted.
/// </summary>
public class StatsInfo
{
    /// <summary>
    /// Indicates the number of inserted records.
    /// </summary>
    public int StatsNumberInserted { get; set; }

    /// <summary>
    /// Indicates the number of updated records.
    /// </summary>
    public int StatsNumberUpdated { get; set; }

    /// <summary>
    /// Indicates the number of deleted records.
    /// </summary>
    public int StatsNumberDeleted { get; set; }
}
