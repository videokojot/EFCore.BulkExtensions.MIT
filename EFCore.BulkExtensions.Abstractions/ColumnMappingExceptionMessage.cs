using System;

namespace EFCore.BulkExtensions;

[Serializable]
internal sealed class ColumnMappingExceptionMessage : InvalidBulkConfigException
{
    public ColumnMappingExceptionMessage() : base(BulkExceptionMessage.ColumnMappingNotMatch) { }
}
