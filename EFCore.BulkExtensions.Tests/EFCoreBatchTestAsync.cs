﻿using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class EFCoreBatchTestAsync : IAssemblyFixture<DbAssemblyFixture>
{
    protected static int EntitiesNumber => 1000;

    [Theory]
    [InlineData(DbServerType.SQLServer)]
    [InlineData(DbServerType.SQLite)]
    public async Task BatchTestAsync(DbServerType dbServer)
    {
        ContextUtil.DbServer = dbServer;

        await RunDeleteAllAsync(dbServer);
        await RunInsertAsync();
        await RunBatchUpdateAsync(dbServer);

        int deletedEntities = 1;
        if (dbServer == DbServerType.SQLServer)
        {
            deletedEntities = await RunTopBatchDeleteAsync();
        }

        await RunBatchDeleteAsync();

        await UpdateSettingAsync(SettingsEnum.Sett1, "Val1UPDATE");
        await UpdateByteArrayToDefaultAsync();

        using var context = new TestContext(ContextUtil.GetOptions());

        var firstItem = (await context.Items.ToListAsync()).First();
        var lastItem = (await context.Items.ToListAsync()).Last();
        Assert.Equal(1, deletedEntities);
        Assert.Equal(500, lastItem.ItemId);
        Assert.Equal("Updated", lastItem.Description);
        Assert.Null(lastItem.Price);
        Assert.StartsWith("name ", lastItem.Name);
        Assert.EndsWith(" Concatenated", lastItem.Name);

        if (dbServer == DbServerType.SQLServer)
        {
            Assert.EndsWith(" TOP(1)", firstItem.Name);
        }
    }

    [Fact]
    public async Task BatchUpdateAsync_correctly_specifies_AnsiString_type_on_the_sql_parameter()
    {
        var dbCommandInterceptor = new TestDbCommandInterceptor();
        var interceptors = new[] { dbCommandInterceptor };

        using var testContext = new TestContext(ContextUtil.GetOptions<TestContext>(DbServerType.SQLServer, interceptors));

        string oldPhoneNumber = "7756789999";
        string newPhoneNumber = "3606789999";

        _ = await testContext.Parents
            .Where(parent => parent.PhoneNumber == oldPhoneNumber)
            .BatchUpdateAsync(parent => new Parent { PhoneNumber = newPhoneNumber })
            .ConfigureAwait(false);

        var executedCommand = dbCommandInterceptor.ExecutedNonQueryCommands.Last();
        Assert.Equal(2, executedCommand.DbParameters.Count);

        var oldPhoneNumberParameter = (Microsoft.Data.SqlClient.SqlParameter)executedCommand.DbParameters.Single(param => param.ParameterName == "@__oldPhoneNumber_0");
        Assert.Equal(System.Data.DbType.AnsiString, oldPhoneNumberParameter.DbType);
        Assert.Equal(System.Data.SqlDbType.VarChar, oldPhoneNumberParameter.SqlDbType);

        var newPhoneNumberParameter = (Microsoft.Data.SqlClient.SqlParameter)executedCommand.DbParameters.Single(param => param.ParameterName == "@param_1");
        Assert.Equal(System.Data.DbType.AnsiString, newPhoneNumberParameter.DbType);
        Assert.Equal(System.Data.SqlDbType.VarChar, newPhoneNumberParameter.SqlDbType);

        var expectedSql =
@"UPDATE p SET  [p].[PhoneNumber] = @param_1 
FROM [Parent] AS [p]
WHERE [p].[PhoneNumber] = @__oldPhoneNumber_0";

        Assert.Equal(expectedSql.Replace("\r\n", "\n"), executedCommand.Sql.Replace("\r\n", "\n"));
    }

    internal async Task RunDeleteAllAsync(DbServerType dbServer)
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        await context.Items.AddAsync(new Item { }); // used for initial add so that after RESEED it starts from 1, not 0
        await context.SaveChangesAsync();

        await context.Items.BatchDeleteAsync();
        await context.BulkDeleteAsync(context.Items.ToList());

        // RESET AutoIncrement
        string deleteTableSql = dbServer switch
        {
            DbServerType.SQLServer => $"DBCC CHECKIDENT('[dbo].[{nameof(Item)}]', RESEED, 0);",
            DbServerType.SQLite => $"DELETE FROM sqlite_sequence WHERE name = '{nameof(Item)}';",
            _ => throw new ArgumentException($"Unknown database type: '{dbServer}'.", nameof(dbServer)),
        };
        context.Database.ExecuteSqlRaw(deleteTableSql);
    }

    private static async Task RunInsertAsync()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        var entities = new List<Item>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            var entity = new Item
            {
                Name = "name " + i,
                Description = string.Concat("info ", Guid.NewGuid().ToString().AsSpan(0, 3)),
                Quantity = i % 10,
                Price = i / (i % 5 + 1),
                TimeUpdated = DateTime.Now,
                ItemHistories = new List<ItemHistory>()
            };
            entities.Add(entity);
        }

        await context.Items.AddRangeAsync(entities);
        await context.SaveChangesAsync();
    }

    private static async Task RunBatchUpdateAsync(DbServerType dbServer)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        //var updateColumns = new List<string> { nameof(Item.Quantity) }; // Adding explicitly PropertyName for update to its default value

        decimal price = 0;

        var query = context.Items.AsQueryable();
        if (dbServer == DbServerType.SQLServer)
        {
            query = query.Where(a => a.ItemId <= 500 && a.Price >= price);
        }
        if (dbServer == DbServerType.SQLite)
        {
            query = query.Where(a => a.ItemId <= 500); // Sqlite currently does Not support multiple conditions
        }

        await query.BatchUpdateAsync(new Item { Description = "Updated" }/*, updateColumns*/);

        await query.BatchUpdateAsync(a => new Item { Name = a.Name + " Concatenated", Quantity = a.Quantity + 100, Price = null }); // example of BatchUpdate value Increment/Decrement

        if (dbServer == DbServerType.SQLServer) // Sqlite currently does Not support Take(): LIMIT
        {
            query = context.Items.Where(a => a.ItemId <= 500 && a.Price == null);
            await query.Take(1).BatchUpdateAsync(a => new Item { Name = a.Name + " TOP(1)", Quantity = a.Quantity + 100 }); // example of BatchUpdate with TOP(1)

        }

        var list = new List<string>() { "Updated" };
        var updatedCount = await context.Set<Item>()
                                        .TagWith("From: someCallSite in someClassName") // To test parsing Sql with Tag leading comment
                                        .Where(a => list.Contains(a.Description ?? ""))
                                        .BatchUpdateAsync(a => new Item() { TimeUpdated = DateTime.Now })
                                        .ConfigureAwait(false);

        if (dbServer == DbServerType.SQLServer) // Sqlite Not supported
        {
            var newValue = 5;
            await context.Parents.Where(parent => parent.ParentId == 1)
                .BatchUpdateAsync(parent => new Parent
                {
                    Description = parent.Children.Where(child => child.IsEnabled && child.Value == newValue).Sum(child => child.Value).ToString(),
                    Value = newValue
                })
                .ConfigureAwait(false);
        }
    }

    private static async Task<int> RunTopBatchDeleteAsync()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        return await context.Items.Where(a => a.ItemId > 500).Take(1).BatchDeleteAsync();
    }

    private static async Task RunBatchDeleteAsync()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        await context.Items.Where(a => a.ItemId > 500).BatchDeleteAsync();
    }

    private static async Task UpdateSettingAsync(SettingsEnum settings, object value)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        await context.TruncateAsync<Setting>();

        await context.Settings.AddAsync(new Setting() { Settings = SettingsEnum.Sett1, Value = "Val1" }).ConfigureAwait(false);
        await context.SaveChangesAsync().ConfigureAwait(false);

        // can work with explicit value: .Where(x => x.Settings == SettingsEnum.Sett1) or if named Parameter used then it has to be named (settings) same as Property (Settings) - Case not relevant, it is CaseInsensitive
        await context.Settings.Where(x => x.Settings == settings).BatchUpdateAsync(x => new Setting { Value = value.ToString() }).ConfigureAwait(false);

        await context.TruncateAsync<Setting>();
    }

    private static async Task UpdateByteArrayToDefaultAsync()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        await context.Files.BatchUpdateAsync(new File { DataBytes = null }, updateColumns: new List<string> { nameof(File.DataBytes) }).ConfigureAwait(false);
        await context.Files.BatchUpdateAsync(a => new File { DataBytes = null }).ConfigureAwait(false);
    }
}
