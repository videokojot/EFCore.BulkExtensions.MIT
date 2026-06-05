using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions;

internal sealed class PrimaryKeysPropertyColumnNameValues
{
    public List<object?> PkValues { get; }

    public PrimaryKeysPropertyColumnNameValues(IEnumerable<object?> pkValues)
    {
        PkValues = pkValues.ToList();
    }

    public override bool Equals(object? obj)
    {
        if (obj is not PrimaryKeysPropertyColumnNameValues values)
        {
            return false;
        }

        bool sequenceEqual = PkValues.SequenceEqual(values.PkValues);
        return sequenceEqual;
    }

    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 19;
            foreach (object? value in PkValues)
            {
                int valueHash = value == null ? 0 : value.GetHashCode();
                hash = hash * 31 + valueHash;
            }

            return hash;
        }
    }

    public string ToLogString()
    {
        string joined = string.Join(", ", PkValues);
        string result = "( " + joined + " )";
        return result;
    }
}
