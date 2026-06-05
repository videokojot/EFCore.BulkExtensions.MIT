namespace EFCore.BulkExtensions;

public readonly record struct MergeActionCounts(int Inserted, int Updated, int Deleted);
