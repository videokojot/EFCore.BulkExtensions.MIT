using Microsoft.EntityFrameworkCore.Metadata;
using System.Collections.Generic;

namespace EFCore.BulkExtensions;

internal sealed class GraphDependency
{
    public HashSet<(object entity, INavigation navigation)> DependsOn { get; } = new HashSet<(object, INavigation)>();

    public HashSet<(object entity, INavigation navigation)> Dependents { get; } = new HashSet<(object, INavigation)>();
}
