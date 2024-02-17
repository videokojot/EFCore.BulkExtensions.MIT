using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions;

internal static class DbContextBulkTransaction
{
    public static void Execute<T>(DbContext context, Type? type, IList<T> entities, OperationType operationType, BulkConfig? bulkConfig, Action<decimal>? progress) where T : class
    {
        type ??= typeof(T);

        CheckForMySQLUnsupportedFeatures(context, operationType, bulkConfig);

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
                TableInfo tableInfo = TableInfo.CreateInstance(context, type, entities, operationType, bulkConfig);

                if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity && tableInfo.BulkConfig.CustomSourceTableName == null)
                {
                    SqlBulkOperation.Insert(context, type, entities, tableInfo, progress);
                }
                else if (operationType == OperationType.Read)
                {
                    SqlBulkOperation.Read(context, type, entities, tableInfo, progress);
                }
                else if (operationType == OperationType.Truncate)
                {
                    SqlBulkOperation.Truncate(context, tableInfo);
                }
                else
                {
                    SqlBulkOperation.Merge(context, type, entities, tableInfo, operationType, progress);
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

    public static async Task ExecuteAsync<T>(DbContext context, Type? type, IList<T> entities, OperationType operationType, BulkConfig? bulkConfig, Action<decimal>? progress, CancellationToken cancellationToken = default) where T : class
    {
        type ??= typeof(T);

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
                TableInfo tableInfo = TableInfo.CreateInstance(context, type, entities, operationType, bulkConfig);

                if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
                {
                    await SqlBulkOperation.InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                }
                else if (operationType == OperationType.Read)
                {
                    await SqlBulkOperation.ReadAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                }
                else if (operationType == OperationType.Truncate)
                {
                    await SqlBulkOperation.TruncateAsync(context, tableInfo, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await SqlBulkOperation.MergeAsync(context, type, entities, tableInfo, operationType, progress, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }
}
