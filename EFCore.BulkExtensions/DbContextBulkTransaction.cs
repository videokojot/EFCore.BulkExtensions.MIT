using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions;

internal static class DbContextBulkTransaction
{
    public static void Execute<T>(DbContext context, Type? type, ICollection<T> entities, OperationType operationType, BulkConfig? bulkConfig, Action<decimal>? progress) where T : class
    {
        type ??= typeof(T);

        CheckForMySQLUnsupportedFeatures(context, operationType, bulkConfig);

        // Ensure we have an IList<T> for downstream operations that rely on indexing
        IList<T> entitiesList = entities as IList<T> ?? entities.ToList();

        using (ActivitySources.StartExecuteActivity(operationType, entities.Count))
        {
            if (entities.Count == 0 &&
                operationType != OperationType.InsertOrUpdateOrDelete &&
                operationType != OperationType.Truncate &&
                operationType != OperationType.SaveChanges &&
                (bulkConfig == null || bulkConfig.CustomSourceTableName == null))
            {
                return;
            }

            if (operationType == OperationType.SaveChanges)
            {
                DbContextBulkTransactionSaveChanges.SaveChanges(context, bulkConfig, progress);

                return;
            }
            else if (bulkConfig?.IncludeGraph == true)
            {
                DbContextBulkTransactionGraphUtil.ExecuteWithGraph(context, entities, operationType, bulkConfig, progress);
            }
            else
            {
                TableInfo tableInfo = TableInfo.CreateInstance(context, type, entitiesList, operationType, bulkConfig);

                if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity && tableInfo.BulkConfig.CustomSourceTableName == null)
                {
                    SqlBulkOperation.Insert(context, type, entitiesList, tableInfo, progress);
                }
                else if (operationType == OperationType.Read)
                {
                    SqlBulkOperation.Read(context, type, entitiesList, tableInfo, progress);
                }
                else if (operationType == OperationType.Truncate)
                {
                    SqlBulkOperation.Truncate(context, tableInfo);
                }
                else
                {
                    SqlBulkOperation.Merge(context, type, entitiesList, tableInfo, operationType, progress);
                }
            }
        }
    }

    internal static void CheckForMySQLUnsupportedFeatures(DbContext context, OperationType operationType, BulkConfig? bulkConfig)
    {
        // In future versions we want to throw here (uncomment code below):

        // if (SqlAdaptersMapping.GetDatabaseType(context) == DbServerType.MySQL)
        // {
            // Output identity is not supported for the MySQL
            // https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/

            // if (bulkConfig != null && operationType == OperationType.SaveChanges)
            // {
            //     bulkConfig.OnSaveChangesSetFK = false;
            // }
            //
            // if (bulkConfig?.SetOutputIdentity == true)
            // {
            //     throw new NotSupportedException("SetOutputIdentity is not supported for MySQL (see issue https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/90) ");
            // }
        // }
    }

    public static async Task ExecuteAsync<T>(DbContext context, Type? type, ICollection<T> entities, OperationType operationType, BulkConfig? bulkConfig, Action<decimal>? progress, CancellationToken cancellationToken = default) where T : class
    {
        type ??= typeof(T);

        // Ensure we have an IList<T> for downstream operations that rely on indexing
        IList<T> entitiesList = entities as IList<T> ?? entities.ToList();

        using (ActivitySources.StartExecuteActivity(operationType, entities.Count))
        {
            if (entities.Count == 0 && operationType != OperationType.InsertOrUpdateOrDelete && operationType != OperationType.Truncate && operationType != OperationType.SaveChanges)
            {
                return;
            }

            if (operationType == OperationType.SaveChanges)
            {
                await DbContextBulkTransactionSaveChanges.SaveChangesAsync(context, bulkConfig, progress, cancellationToken).ConfigureAwait(false);
            }
            else if (bulkConfig?.IncludeGraph == true)
            {
                await DbContextBulkTransactionGraphUtil.ExecuteWithGraphAsync(context, entities, operationType, bulkConfig, progress, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                TableInfo tableInfo = TableInfo.CreateInstance(context, type, entitiesList, operationType, bulkConfig);

                if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
                {
                    await SqlBulkOperation.InsertAsync(context, type, entitiesList, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                }
                else if (operationType == OperationType.Read)
                {
                    await SqlBulkOperation.ReadAsync(context, type, entitiesList, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                }
                else if (operationType == OperationType.Truncate)
                {
                    await SqlBulkOperation.TruncateAsync(context, tableInfo, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await SqlBulkOperation.MergeAsync(context, type, entitiesList, tableInfo, operationType, progress, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }
}
