using System.Diagnostics;
using System.Globalization;

namespace EFCore.BulkExtensions;

/// <summary>
/// Contains activity sources
/// </summary>
public static class ActivitySources
{
    private static readonly ActivitySource ActivitySource = new ("EFCore.BulkExtensions");

    /// <summary>
    /// Starts the activity
    /// </summary>
    /// <param name="operationType"></param>
    /// <param name="entitiesCount"></param>
    /// <returns></returns>
    public static Activity? StartExecuteActivity(OperationType operationType, int entitiesCount)
    {
        Activity? activity = ActivitySource.StartActivity("EFCore.BulkExtensions.BulkExecute");

        if (activity != null)
        {
            string operationTypeTag = operationType.ToString("G");
            string entitiesCountTag = entitiesCount.ToString(CultureInfo.InvariantCulture);
            activity.AddTag("operationType", operationTypeTag);
            activity.AddTag("entitiesCount", entitiesCountTag);
        }

        return activity;
    }
}
