using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions;

internal sealed class GraphUtil
{
    public static IEnumerable<GraphNode>? GetTopologicallySortedGraph(DbContext dbContext, IEnumerable<object> entities)
    {
        if (!entities.Any())
        {
            return null;
        }

        // Enumerate through all the entities and retrieve a flat list of all the entities with their dependencies
        Dictionary<object, GraphDependency> dependencies = new Dictionary<object, GraphDependency>();

        foreach (object e in entities)
        {
            GetFlatGraph(dbContext, e, dependencies);
        }

        // Sort these entities so the first entity is the least dependendant
        Func<object, IEnumerable<object>> dependencySelector = y => dependencies[y].DependsOn.Select(d => d.entity);
        IEnumerable<object> topologicalSorted = TopologicalSort(dependencies.Keys, dependencySelector);
        List<GraphNode> result = new List<GraphNode>();

        foreach (object s in topologicalSorted)
        {
            result.Add(new GraphNode
            {
                Entity = s,
                Dependencies = dependencies[s]
            });
        }

        return result;
    }

    private static GraphDependency? GetFlatGraph(DbContext dbContext, object graphEntity, IDictionary<object, GraphDependency> result)
    {
        IEntityType? entityType = dbContext.Model.FindEntityType(graphEntity.GetType());

        // The entity is not being apart of the DbContext model, do nothing
        if (entityType is null)
        {
            return null;
        }

        if (!result.TryGetValue(graphEntity, out GraphDependency? graphDependency))
        {
            graphDependency = new GraphDependency();
            result.Add(graphEntity, graphDependency);
        }
        else
        {
            // To prevent circular references & stack overflow, if the graphEntity has already been tracked then just return
            return graphDependency;
        }

        IEnumerable<INavigation> entityNavigations = entityType.GetNavigations();

        foreach (INavigation navigation in entityNavigations)
        {
            if (navigation.IsCollection)
            {
                object? navigationValue = dbContext.Entry(graphEntity).Collection(navigation.Name).CurrentValue;

                if (navigationValue is null)
                {
                    continue;
                }

                List<object> navigationCollectionValue = ((System.Collections.IEnumerable)navigationValue).Cast<object>().ToList();

                foreach (object navEntity in navigationCollectionValue)
                {
                    SetDependencies(dbContext, graphDependency, graphEntity, navigation, navEntity, result);
                }
            }
            else
            {
                object? navigationValue = dbContext.Entry(graphEntity).Reference(navigation.Name).CurrentValue;

                if (navigationValue is null)
                {
                    continue;
                }

                SetDependencies(dbContext, graphDependency, graphEntity, navigation, navigationValue, result);
            }
        }

        return graphDependency;
    }

    private static void SetDependencies(DbContext dbContext, GraphDependency graphDependency, object graphEntity, INavigation navigation, object navigationValue, IDictionary<object, GraphDependency> result)
    {
        // Get the nested dependency for the navigationValue so we can add the inverse navigation dependency
        // incase the navigationValue entity does not have an explicitly defined navigation property back to its principal / dependent
        // i.e WorkOrderSpare.Spare but the Spare entity does not have a Spare.WorkOrderSpares navigation property
        GraphDependency? nestedDependency = GetFlatGraph(dbContext, navigationValue, result);

        if (nestedDependency is null)
        {
            return;
        }

        if (navigation.IsOnDependent

            // A navigation for an OwnedType will be dependent on its owner the in efcore dependency hierarchy,
            // but technically the Owner depends on the OwnedType if the OwnedType is part of its Owner's schema.
            || OwnedTypeUtil.IsOwnedInSameTableAsOwner(navigation))
        {
            graphDependency.DependsOn.Add((navigationValue, navigation));
            nestedDependency.Dependents.Add((graphEntity, navigation.Inverse ?? navigation));
        }
        else
        {
            graphDependency.Dependents.Add((navigationValue, navigation));
            nestedDependency.DependsOn.Add((graphEntity, navigation.Inverse ?? navigation));
        }
    }

    private static IEnumerable<T> TopologicalSort<T>(IEnumerable<T> source, Func<T, IEnumerable<T>> dependencies, bool throwOnCycle = false)
    {
        List<T> sorted = new List<T>();
        HashSet<T> visited = new HashSet<T>();

        foreach (T item in source)
        {
            Visit(item, visited, sorted, dependencies, throwOnCycle);
        }

        return sorted;
    }

    private static void Visit<T>(T item, HashSet<T> visited, List<T> sorted, Func<T, IEnumerable<T>> dependencies, bool throwOnCycle)
    {
        if (!visited.Contains(item))
        {
            visited.Add(item);

            foreach (T dep in dependencies(item))
            {
                Visit(dep, visited, sorted, dependencies, throwOnCycle);
            }

            sorted.Add(item);
        }
        else
        {
            if (throwOnCycle && !sorted.Contains(item))
            {
                throw new Exception("Cyclic dependency found");
            }
        }
    }
}
