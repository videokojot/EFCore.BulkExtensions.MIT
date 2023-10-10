using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class EFCoreBulkUnderlyingTest : IAssemblyFixture<DbAssemblyFixture>
{
    protected static int EntitiesNumber => 1000;

    private static readonly Func<TestContext, int> ItemsCountQuery = EF.CompileQuery<TestContext, int>(ctx => ctx.Items.Select(i => i.ItemId).Count());
    private static readonly Func<TestContext, Item?> LastItemQuery = EF.CompileQuery<TestContext, Item?>(ctx => ctx.Items.OrderBy(i => i.ItemId).LastOrDefault());
    private static readonly Func<TestContext, IEnumerable<Item>> AllItemsQuery = EF.CompileQuery<TestContext, IEnumerable<Item>>(ctx => ctx.Items.AsNoTracking());

    [Theory(Skip = "Removed flaky test (sometimes fails in Connection Pool Clearing when it is cast to SQLConnection). I think you are not supposed to pass your own DbConnection implmentation.")]
    [InlineData(true)]
    public void OperationsTest(bool isBulk)
    {
        RunDelete(isBulk);
        RunInsert(isBulk);
        RunDelete(isBulk);
    }

    public static DbContextOptions GetOptions()
    {
        var builder = new DbContextOptionsBuilder<TestContext>();
        var databaseName = nameof(EFCoreBulkTest);
        var connectionString = ContextUtil.GetSqlServerConnectionString(databaseName);
        var connection = new SqlConnection(connectionString) as DbConnection;
        connection = new MyConnection(connection);
        builder.UseSqlServer(connection, opt => opt.UseNetTopologySuite());
        builder.UseSqlServer(connection, conf =>
        {
            conf.UseHierarchyId();
        });
        return builder.Options;
    }

    private static void RunInsert(bool isBulk)
    {
        using var context = new TestContext(GetOptions());
        var entities = new List<Item>();
        var subEntities = new List<ItemHistory>();
        for (int i = 1; i < EntitiesNumber; i++)
        {
            var entity = new Item (
                isBulk ? i : 0,
                "name " + i,
                string.Concat("info ", Guid.NewGuid().ToString().AsSpan(0, 3)),
                i % 10,
                i / (i % 5 + 1),
                DateTime.Now,
                new List<ItemHistory>());

            var subEntity1 = new ItemHistory
            {
                ItemHistoryId = SeqGuid.Create(),
                Remark = $"some more info {i}.1"
            };
            var subEntity2 = new ItemHistory
            {
                ItemHistoryId = SeqGuid.Create(),
                Remark = $"some more info {i}.2"
            };
            entity.ItemHistories.Add(subEntity1);
            entity.ItemHistories.Add(subEntity2);

            entities.Add(entity);
        }

        if (isBulk)
        {
            using var transaction = context.Database.BeginTransaction();
            context.BulkInsert(
                entities,
                new BulkConfig
                {
                    PreserveInsertOrder = true,
                    SetOutputIdentity = true,
                    BatchSize = 4000,
                    UseTempDB = true,
                    UnderlyingConnection = GetUnderlyingConnection,
                    UnderlyingTransaction = GetUnderlyingTransaction
                }
            );

            foreach (var entity in entities)
            {
                foreach (var subEntity in entity.ItemHistories)
                {
                    subEntity.ItemId = entity.ItemId; // setting FK to match its linked PK that was generated in DB
                }
                subEntities.AddRange(entity.ItemHistories);
            }
            context.BulkInsert(subEntities, new BulkConfig()
            {
                UnderlyingConnection = GetUnderlyingConnection,
                UnderlyingTransaction = GetUnderlyingTransaction
            });

            transaction.Commit();
        }
        else
        {
            context.Items.AddRange(entities);
            context.SaveChanges();
        }

        //TEST
        int entitiesCount = ItemsCountQuery(context);
        Item? lastEntity = LastItemQuery(context);

        Assert.Equal(EntitiesNumber - 1, entitiesCount);
        Assert.NotNull(lastEntity);
        Assert.Equal("name " + (EntitiesNumber - 1), lastEntity?.Name);
    }

    private void RunDelete(bool isBulk)
    {
        using var context = new TestContext(GetOptions());
        var entities = AllItemsQuery(context).ToList();
        // ItemHistories will also be deleted because of Relationship - ItemId (Delete Rule: Cascade)
        if (isBulk)
        {
            context.BulkDelete(entities, new BulkConfig()
            {
                UnderlyingConnection = GetUnderlyingConnection,
                UnderlyingTransaction = GetUnderlyingTransaction
            });
        }
        else
        {
            context.Items.RemoveRange(entities);
            context.SaveChanges();
        }

        Assert.Equal(0, ItemsCountQuery(context));
        Assert.Null(LastItemQuery(context));

        // Resets AutoIncrement
        context.Database.ExecuteSqlRaw("DBCC CHECKIDENT ('dbo.[" + nameof(Item) + "]', RESEED, 0);");
    }

    public static DbConnection GetUnderlyingConnection(DbConnection connection)
    {
        if (connection is MyConnection mc) return mc.UnderlyingConection;
        return connection;
    }
    public static DbTransaction GetUnderlyingTransaction(DbTransaction transaction)
    {
        if (transaction is MyTransaction mt) return mt.UnderlyingTransaction;
        return transaction;
    }
}

public class MyConnection : DbConnection
{
    public DbConnection UnderlyingConection { get; }

    public override string Database => UnderlyingConection.Database;
    public override string DataSource => UnderlyingConection.DataSource;
    public override string ServerVersion => UnderlyingConection.ServerVersion;
    public override ConnectionState State => UnderlyingConection.State;

    public override string ConnectionString
    {
        get => UnderlyingConection.ConnectionString;
        [param: AllowNull]
#pragma warning disable CS8765 // Complains about a false nullability
        set => UnderlyingConection.ConnectionString = value ?? "";
#pragma warning restore CS8765 // Complains about a false nullability
    }

    public MyConnection(DbConnection underlyingConnection)
    {
        UnderlyingConection = underlyingConnection;
    }

    public override void ChangeDatabase(string databaseName)
    {
        UnderlyingConection.ChangeDatabase(databaseName);
    }

    public override void Open()
    {
        UnderlyingConection.Open();
    }

    public override void Close()
    {
        UnderlyingConection.Close();
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        return new MyTransaction(UnderlyingConection.BeginTransaction(), this);
    }

    protected override DbCommand CreateDbCommand()
    {
        return new MyCommand(UnderlyingConection.CreateCommand(), this);
    }
}

class MyTransaction : DbTransaction
{
    public DbTransaction UnderlyingTransaction { get; }
    public MyConnection MyConnection { get; }

    public override IsolationLevel IsolationLevel => UnderlyingTransaction.IsolationLevel;
    protected override DbConnection DbConnection => MyConnection;

    public MyTransaction(DbTransaction underlyingTransaction, MyConnection connection)
    {
        UnderlyingTransaction = underlyingTransaction;
        MyConnection = connection;
    }

    public override void Commit()
    {
        UnderlyingTransaction.Commit();
    }

    public override void Rollback()
    {
        UnderlyingTransaction.Rollback();
    }
}

class MyCommand : DbCommand
{
    public DbCommand UnderlyingCommand { get; set; }
    public MyConnection? MyConnection { get; set; }

    public MyCommand(DbCommand underlyingCommand, MyConnection? connection)
    {
        UnderlyingCommand = underlyingCommand;
        MyConnection = connection;
    }

    public override string CommandText
    { 
        get => UnderlyingCommand.CommandText ?? string.Empty;
        [param:AllowNull]
#pragma warning disable CS8765 // Complains about a false nullability
        set => UnderlyingCommand.CommandText = value ?? string.Empty;
#pragma warning restore CS8765 // Complains about a false nullability
    }
    public override int CommandTimeout { get => UnderlyingCommand.CommandTimeout; set => UnderlyingCommand.CommandTimeout = value; }
    public override CommandType CommandType { get => UnderlyingCommand.CommandType; set => UnderlyingCommand.CommandType = value; }
    public override bool DesignTimeVisible { get => UnderlyingCommand.DesignTimeVisible; set => UnderlyingCommand.DesignTimeVisible = value; }
    public override UpdateRowSource UpdatedRowSource { get => UnderlyingCommand.UpdatedRowSource; set => UnderlyingCommand.UpdatedRowSource = value; }
    protected override DbConnection? DbConnection { get => MyConnection; set => MyConnection = (MyConnection?)value; }

    protected override DbParameterCollection DbParameterCollection => this.UnderlyingCommand.Parameters;

    public MyTransaction? MyTransaction { get; set; }

    protected override DbTransaction? DbTransaction
    {
        get => MyTransaction;
        set
        {
            MyTransaction = (MyTransaction?)value;
            UnderlyingCommand.Transaction = MyTransaction?.UnderlyingTransaction;
        }
    }

    public override void Cancel()
    {
        UnderlyingCommand.Cancel();
    }

    public override int ExecuteNonQuery()
    {
        return UnderlyingCommand.ExecuteNonQuery();
    }

    public override object? ExecuteScalar()
    {
        return UnderlyingCommand.ExecuteScalar();
    }

    public override void Prepare()
    {
        UnderlyingCommand.Prepare();
    }

    protected override DbParameter CreateDbParameter()
    {
        return UnderlyingCommand.CreateParameter();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        return UnderlyingCommand.ExecuteReader(behavior);
    }
}
