using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;

namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

public class SqlServerDialect : SqlDefaultDialect
{
    public override char EscL => '[';

    public override char EscR => ']';
}
