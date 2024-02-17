using EFCore.BulkExtensions.SqlAdapters;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public static class TestHelpers
{
    public static IEnumerable<object[]> AllDbTypes => new List<object[]>
    {
        new object[] { DbServerType.SQLServer },
        new object[] { DbServerType.SQLite },
        new object[] { DbServerType.PostgreSQL },
        new object[] { DbServerType.MySQL },
    };

    private static IEnumerable<object[]> ServerTypesInternal(params DbServerType[] serverTypes) => serverTypes.Select(x => new object[] { x }).ToList();
    public static IEnumerable<object[]> ServerTypes(DbServerType s1) => ServerTypesInternal(s1);
    public static IEnumerable<object[]> ServerTypes(DbServerType s1, DbServerType s2) => ServerTypesInternal(s1, s2);
    public static IEnumerable<object[]> ServerTypes(DbServerType s1, DbServerType s2, DbServerType s3) => ServerTypesInternal(s1, s2, s3);
    public static IEnumerable<object[]> ServerTypes(DbServerType s1, DbServerType s2, DbServerType s3, DbServerType s4) => ServerTypesInternal(s1, s2, s3, s4);
}
