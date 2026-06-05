using System.Collections.Generic;

namespace EFCore.BulkExtensions;

internal sealed class PrimaryKeyList : List<object?>
{
    public override bool Equals(object? obj)
    {
        PrimaryKeyList? objCast = obj as PrimaryKeyList;

        if (objCast is null)
        {
            bool baseEqualsResult = base.Equals(objCast);
            return baseEqualsResult;
        }

        if (objCast.Count != Count)
        {
            bool countMismatchEqualsResult = base.Equals(objCast);
            return countMismatchEqualsResult;
        }

        for (int i = 0; i < Count; i++)
        {
            object? a = this[i];
            object? b = objCast[i];

            if (a?.Equals(b) == false)
            {
                return false;
            }
        }

        return true;
    }

    public override int GetHashCode()
    {
        int hash = 0xC0FFEE;

        foreach (object? x in this)
        {
            int elementHash = x?.GetHashCode() ?? 0;
            hash = hash * 31 + elementHash;
        }

        return hash;
    }
}
