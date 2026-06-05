using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions;

/// <summary>
/// Owned entity utilities
/// </summary>
public static class OwnedTypeUtil
{
    /// <summary>
    /// Determines if entity is owned entity
    /// </summary>
    /// <param name="owned"></param>
    /// <returns></returns>
    public static bool IsOwnedInSameTableAsOwner(IEntityType owned)
    {
        IForeignKey? ownership = owned.FindOwnership();

        if (ownership is null)
        {
            return false;
        }

        IEntityType owner = ownership.PrincipalEntityType;
        IEnumerable<ITableMapping> ownedTables = owned.GetTableMappings();

        foreach (ITableMapping ot in ownedTables)
        {
            bool isSharingTable = ot.Table.EntityTypeMappings.Any(y => y.TypeBase == owner);

            if (!isSharingTable)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Determines if entity is owned entity
    /// </summary>
    /// <param name="navigation"></param>
    /// <returns></returns>
    public static bool IsOwnedInSameTableAsOwner(INavigation navigation)
    {
        bool result = IsOwnedInSameTableAsOwner(navigation.TargetEntityType);
        return result;
    }
}
