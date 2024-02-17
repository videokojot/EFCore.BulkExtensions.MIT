using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

public class SqlQueryBuilderSqlServer : QueryBuilderExtensions
{
    /// <inheritdoc/>
    public override object CreateParameter(SqlParameter sqlParameter) => throw new NotImplementedException();

    /// <inheritdoc/>
    public override object Dbtype() => throw new NotImplementedException();

    /// <inheritdoc/>
    public override string RestructureForBatch(string sql, bool isDelete = false) => throw new NotImplementedException();

    /// <inheritdoc/>
    public override string SelectFromOutputTable(TableInfo tableInfo) => SqlQueryBuilder.SelectFromOutputTable(tableInfo);

    /// <inheritdoc/>
    public override void SetDbTypeParam(object npgsqlParameter, object dbType) => throw new NotImplementedException();
}
