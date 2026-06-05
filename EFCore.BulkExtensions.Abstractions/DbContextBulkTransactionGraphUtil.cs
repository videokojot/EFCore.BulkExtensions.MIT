using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace EFCore.BulkExtensions;

internal static class DbContextBulkTransactionGraphUtil
{
    public static void ExecuteWithGraph(DbContext context, IEnumerable<object> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal>? progress)
    {
        ExecuteWithGraphAsync(context, entities, operationType, bulkConfig, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    public static async Task ExecuteWithGraphAsync(DbContext context, IEnumerable<object> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal>? progress, CancellationToken cancellationToken)
    {
        await ExecuteWithGraphAsync(context, entities, operationType, bulkConfig, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }

    private static async Task ExecuteWithGraphAsync(DbContext context, IEnumerable<object> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
    {
        if (operationType != OperationType.Insert
                   && operationType != OperationType.InsertOrUpdate
                   && operationType != OperationType.InsertOrUpdateOrDelete
                   && operationType != OperationType.Update)
        {
            throw new InvalidBulkConfigException($"{nameof(BulkConfig)}.{nameof(BulkConfig.IncludeGraph)} only supports Insert or Update operations.");
        }

        // Sqlite bulk merge adapter does not support multiple objects of the same type with a zero value primary key
        if (!SqlAdaptersMapping.GetAdapterDialect(context).SupportsGraphOperations)
        {
            throw new NotSupportedException("Sqlite is not currently supported due to its BulkInsert implementation.");
        }

        bulkConfig.PreserveInsertOrder = true; // Required for SetOutputIdentity ('true' is default but here explicitly assigned again in case it was changed to 'false' in BulkConfing)
        bulkConfig.SetOutputIdentity = true; // If this is set to false, won't be able to propogate new primary keys to the relationships

        // If this is set to false, wont' be able to support some code first model types as EFCore uses shadow properties when a relationship's foreign keys arent explicitly defined
        bulkConfig.EnableShadowProperties = true;

        IEnumerable<GraphNode>? graphNodes = GraphUtil.GetTopologicallySortedGraph(context, entities);

        if (graphNodes == null)
        {
            return;
        }

        // Inserting an entity graph must be done within a transaction otherwise the database could end up in a bad state
        bool hasExistingTransaction = context.Database.CurrentTransaction != null || Transaction.Current != null;
        IDbContextTransaction? transaction = hasExistingTransaction ? null : context.Database.CurrentTransaction ?? (isAsync ? await context.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false) : context.Database.BeginTransaction());

        try
        {
            // Group the graph nodes by entity type so we can merge them into the database in batches, in the correct order of dependency (topological order)
            IEnumerable<IGrouping<Type, GraphNode>> graphNodesGroupedByType = graphNodes.GroupBy(y => y.Entity.GetType());

            foreach (IGrouping<Type, GraphNode> graphNodeGroup in graphNodesGroupedByType)
            {
                Type entityClrType = graphNodeGroup.Key;
                IEntityType entityType = context.Model.FindEntityType(entityClrType) ?? throw new ArgumentException($"Unable to determine EntityType from given type {entityClrType.Name}");

                if (OwnedTypeUtil.IsOwnedInSameTableAsOwner(entityType))
                {
                    continue;
                }

                // It is possible the object graph contains duplicate entities (by primary key) but the entities are different object instances in memory.
                // This an happen when deserializing a nested JSON tree for example. So filter out the duplicates.
                IEnumerable<object> entitySelection = graphNodeGroup.Select(y => y.Entity);
                List<object> entitiesToAction = GetUniqueEntities(context, entitySelection).ToList();
                TableInfo tableInfo = TableInfo.CreateInstance(context, entityClrType, entitiesToAction, operationType, bulkConfig);

                if (isAsync)
                {
                    await SqlBulkOperation.MergeAsync(context, entityClrType, entitiesToAction, tableInfo, operationType, progress, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    SqlBulkOperation.Merge(context, entityClrType, entitiesToAction, tableInfo, operationType, progress);
                }

                // Set the foreign keys for dependents so they may be inserted on the next loop
                List<object> dependentsOfSameType = SetForeignKeysForDependentsAndYieldSameTypeDependents(context, entityClrType, graphNodeGroup).ToList();

                // If there are any dependents of the same type (parent child relationship), then save those dependent entities again to commit the fk values
                if (dependentsOfSameType.Any())
                {
                    TableInfo dependentTableInfo = TableInfo.CreateInstance(context, entityClrType, dependentsOfSameType, operationType, bulkConfig);

                    if (isAsync)
                    {
                        await SqlBulkOperation.MergeAsync(context, entityClrType, dependentsOfSameType, dependentTableInfo, operationType, progress, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        SqlBulkOperation.Merge(context, entityClrType, dependentsOfSameType, dependentTableInfo, operationType, progress);
                    }
                }
            }

            if (hasExistingTransaction == false)
            {
                if (isAsync)
                {
                    await transaction!.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    transaction!.Commit();
                }
            }
        }
        finally
        {
            if (hasExistingTransaction == false)
            {
                if (isAsync)
                {
                    await transaction!.DisposeAsync().ConfigureAwait(false);
                }
                else
                {
                    transaction!.Dispose();
                }
            }
        }
    }

    private static IEnumerable<object> SetForeignKeysForDependentsAndYieldSameTypeDependents(DbContext context, Type entityClrType, IEnumerable<GraphNode> graphNodeGroup)
    {
        // Loop through the dependants and update their foreign keys with the PK values of the just inserted / merged entities
        foreach (GraphNode graphNode in graphNodeGroup)
        {
            object entity = graphNode.Entity;

            foreach ((object dependentEntity, INavigation navigation) in graphNode.Dependencies.Dependents)
            {
                SetForeignKeyForRelationship(context, navigation, dependentEntity, entity);

                if (dependentEntity.GetType() == entityClrType)
                {
                    yield return dependentEntity;
                }
            }
        }
    }

    private static IEnumerable<object> GetUniqueEntities(DbContext context, IEnumerable<object> entities)
    {
        Type firstEntityType = entities.First().GetType();
        IEntityType entityType = context.Model.FindEntityType(firstEntityType) ?? throw new ArgumentException($"Unable to determine EntityType from given type {firstEntityType.Name}");
        IKey? pk = entityType.FindPrimaryKey();
        HashSet<PrimaryKeyList> processedPks = new HashSet<PrimaryKeyList>();

        foreach (object entity in entities)
        {
            EntityEntry entry = context.Entry(entity);

            // If the entry has its key set, make sure its unique. It is possible for an entity to exist more than once in a graph.
            if (entry.IsKeySet)
            {
                PrimaryKeyList primaryKeyComparer = new PrimaryKeyList();

                if (pk is not null)
                {
                    foreach (IProperty pkProp in pk.Properties)
                    {
                        object? currentValue = entry.Property(pkProp.Name).CurrentValue;
                        primaryKeyComparer.Add(currentValue);
                    }

                    // If the processed pk already exists in the HashSet, its not unique.
                    if (processedPks.Add(primaryKeyComparer))
                    {
                        yield return entity;
                    }
                }

            }
            else
            {
                yield return entity;
            }
        }
    }

    private static void SetForeignKeyForRelationship(DbContext context, INavigation navigation, object dependent, object principal)
    {
        IReadOnlyList<IProperty> principalKeyProperties = navigation.ForeignKey.PrincipalKey.Properties;
        List<object?> pkValues = new List<object?>();

        foreach (IProperty pk in principalKeyProperties)
        {
            object? value = context.Entry(principal).Property(pk.Name).CurrentValue;
            pkValues.Add(value);
        }

        IReadOnlyList<IProperty> dependantKeyProperties = navigation.ForeignKey.Properties;

        for (int i = 0; i < pkValues.Count; i++)
        {
            IProperty dk = dependantKeyProperties[i];
            object? pkVal = pkValues[i];

            context.Entry(dependent).Property(dk.Name).CurrentValue = pkVal;
        }
    }
}
