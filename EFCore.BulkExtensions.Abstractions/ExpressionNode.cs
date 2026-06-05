using System.Linq.Expressions;

namespace EFCore.BulkExtensions;

internal sealed class ExpressionNode
{
#pragma warning disable CS1591
    public ExpressionNode(Expression expression, ExpressionNode? parent)
    {
        Expression = expression;
        Parent = parent;
    }

    public Expression Expression { get; }

    public ExpressionNode? Parent { get; }
#pragma warning restore CS1591
}
