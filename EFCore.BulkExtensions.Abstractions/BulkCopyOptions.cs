using System;

namespace EFCore.BulkExtensions;

/// <summary>
/// Options for bulk copy operations. Numeric values match <c>Microsoft.Data.SqlClient.SqlBulkCopyOptions</c> for SQL Server conversion.
/// </summary>
[Flags]
public enum BulkCopyOptions
{
    Default = 0,
    KeepIdentity = 1,
    CheckConstraints = 2,
    TableLock = 4,
    KeepNulls = 8,
    FireTriggers = 16,
    UseInternalTransaction = 32,
}
