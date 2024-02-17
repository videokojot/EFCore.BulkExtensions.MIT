using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
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

    // https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/90
    [Theory(Skip = "Tracked by issue: https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/90")]
    [InlineData(DbServerType.MySQL)]
    public async Task TestBulkInsertOrUpdate(DbServerType dbType)
    {
        var bulkId = Guid.NewGuid();
        var initialItem = new MySqlItem()
        {
            Id = 0,
            StringProperty = "initialValue",
            BulkIdentifier = bulkId,
        };

        using (var db = _dbFixture.GetDb(dbType))
        {
            db.Items.Add(initialItem);
            await db.SaveChangesAsync();
        }


        var initialId = initialItem.Id;
        Assert.NotEqual(0, initialId);

        var updatingItem = new MySqlItem()
        {
            Id = initialId,
            StringProperty = "updatedValue",
            BulkIdentifier = bulkId,
        };

        var newItem = new MySqlItem()
        {
            Id = 0,
            StringProperty = "newValue",
            BulkIdentifier = bulkId,
        };

        using (var db = _dbFixture.GetDb(dbType))
        {
            await db.BulkInsertOrUpdateAsync(new[]
            {
                updatingItem, newItem
            }, config => { config.SetOutputIdentity = true; });

            Assert.NotEqual(0, newItem.Id);
        }

        using (var db = _dbFixture.GetDb(dbType))
        {
            var items = await db.Items.Where(x => x.BulkIdentifier == bulkId).ToListAsync();

            Assert.Equal(2, items.Count);

            // Item was updated
            Assert.Single(items, x => x.StringProperty == updatingItem.StringProperty && x.Id == updatingItem.Id);

            // Item was inserted
            Assert.Single(items, x => x.StringProperty == newItem.StringProperty
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

        public DbSet<MySqlItem> Items { get; private set; } = null!;

        public List<MySqlItem> GetItemsOfBulk(Guid bulkId)
        {
            return Items.Where(x => x.BulkIdentifier == bulkId).ToList();
        }
    }

    public class MySqlItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Column("DbId")]
        public int Id { get; set; }

        public string StringProperty { get; set; }

        public Guid BulkIdentifier { get; set; }

        public Guid GuidProperty { get; set; }
    }
}
