namespace EFCore.BulkExtensions;

internal sealed class GraphNode
{
    public object Entity { get; set; } = null!;

    public GraphDependency Dependencies { get; set; } = null!;
}
