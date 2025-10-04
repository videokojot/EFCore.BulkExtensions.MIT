using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.BulkExtensions.SqlAdapters;

public interface IDbServer
{
    DbServerType Type { get; }

    ISqlOperationsAdapter Adapter { get; }

    IQueryBuilderSpecialization Dialect { get; }

    QueryBuilderExtensions QueryBuilder { get; }

    bool PropertyHasIdentity(IProperty annotation);

    DbConnection? DbConnection { get; set; }
}
