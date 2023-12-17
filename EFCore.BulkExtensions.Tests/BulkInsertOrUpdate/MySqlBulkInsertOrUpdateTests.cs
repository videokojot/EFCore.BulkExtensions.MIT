using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public class MySqlBulkInsertOrUpdateTests : IClassFixture<MySqlBulkInsertOrUpdateTests.DatabaseFixture>, IAssemblyFixture<DbAssemblyFixture>
{
    private readonly DatabaseFixture _dbFixture;

    public MySqlBulkInsertOrUpdateTests(DatabaseFixture dbFixture)
    {
        _dbFixture = dbFixture;
    }

    [Fact(Skip = "Tracked by issue: https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/90")]
    public async Task TestBulkInsertOrUpdate()
    {
        var bulkId = Guid.NewGuid();
        var initialItem = new MySqlItem()
        {
            Id = 0,
            Name = "initialValue",
            BulkId = bulkId,
        };

        using (var db = _dbFixture.GetDb(DbServerType.MySQL))
        {
            db.Items.Add(initialItem);
            await db.SaveChangesAsync();
        }


        var initialId = initialItem.Id;
        Assert.NotEqual(0, initialId);

        var updatingItem = new MySqlItem()
        {
            Id = initialId,
            Name = "updatedValue",
            BulkId = bulkId,
        };

        var newItem = new MySqlItem()
        {
            Id = 0,
            Name = "newValue",
            BulkId = bulkId,
        };

        using (var db = _dbFixture.GetDb(DbServerType.MySQL))
        {
            await db.BulkInsertOrUpdateAsync(new[]
            {
                updatingItem, newItem
            }, config => { config.SetOutputIdentity = true; });

            Assert.NotEqual(0, newItem.Id);
        }

        using (var db = _dbFixture.GetDb(DbServerType.MySQL))
        {
            var items = await db.Items.Where(x => x.BulkId == bulkId).ToListAsync();

            Assert.Equal(2, items.Count);

            // Item was updated
            Assert.Single(items, x => x.Name == updatingItem.Name && x.Id == updatingItem.Id);

            // Item was inserted
            Assert.Single(items, x => x.Name == newItem.Name
                // && x.Id == newItem.Id
            );
        }
    }

    public class DatabaseFixture : BulkDbTestsFixture<MySqlSimpleContext>
    {
        protected override string DbName => nameof(MySqlBulkInsertOrUpdateTests);
    }

    public class MySqlSimpleContext : DbContext
    {
        public MySqlSimpleContext(DbContextOptions dbContextOptions) : base(dbContextOptions)
        {
        }

        public DbSet<MySqlItem> Items { get; private set; }
    }

    public class MySqlItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string Name { get; set; }

        public Guid BulkId { get; set; }
    }
}
