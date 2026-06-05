using System;

namespace EFCore.BulkExtensions;

[Serializable]
internal sealed class MultiplePropertyListSetException : InvalidBulkConfigException
{
    public MultiplePropertyListSetException() { }

    public MultiplePropertyListSetException(string propertyList1Name, string PropertyList2Name)
        : base(string.Format(BulkExceptionMessage.SpecifiedDoubleConfigLists, propertyList1Name, PropertyList2Name)) { }
}
