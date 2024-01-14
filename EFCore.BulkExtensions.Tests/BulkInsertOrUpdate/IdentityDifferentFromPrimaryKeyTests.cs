using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public class IdentityDifferentFromPrimaryKeyTests : IClassFixture<IdentityDifferentFromPrimaryKeyTests>, IAssemblyFixture<DbAssemblyFixture>
{
    public class DatabaseFixture : BulkDbTestsFixture<IdentityDifferentFromPkDbContext>
    {
        protected override string DbName => nameof(IdentityDifferentFromPrimaryKeyTests);
    }

    private readonly DatabaseFixture _dbFixture;

    public IdentityDifferentFromPrimaryKeyTests(DatabaseFixture dbFixture)
    {
        _dbFixture = dbFixture;
    }

    /// <summary>
    /// Covers: https://github.com/borisdj/EFCore.BulkExtensions/issues/1263
    /// </summary>
    [Theory]
    [InlineData(DbServerType.SQLServer)]
    [InlineData(DbServerType.PostgreSQL)] // Added?
    public void IUD_KeepIdentity_IdentityDifferentFromKey(DbServerType dbType)
    {
        var item = new Entity_KeyDifferentFromIdentity()
        {
            ItemTestGid = Guid.NewGuid(),
            ItemTestIdent = 1234,
            Name = "1234",
        };

        var item2 = new Entity_KeyDifferentFromIdentity()
        {
            ItemTestGid = Guid.NewGuid(),
            ItemTestIdent = 12345678,
            Name = "12345678",
        };

        using (var db = _dbFixture.GetDb(dbType))
        {
            var items = new[] { item, item2 };

            db.BulkInsertOrUpdateOrDelete(items, c => { c.SqlBulkCopyOptions = SqlBulkCopyOptions.Default | SqlBulkCopyOptions.KeepIdentity; });
        }

        using (var db = _dbFixture.GetDb(dbType))
        {
            var insertedItem = db.EntityKeyDifferentFromIdentities.Single(x => x.ItemTestGid == item.ItemTestGid);
            Assert.Equal(1234, insertedItem.ItemTestIdent);
        }
    }


    public class IdentityDifferentFromPkDbContext : DbContext
    {
        public DbSet<Entity_KeyDifferentFromIdentity> EntityKeyDifferentFromIdentities { get; set; } = null!;
    }

    public class Entity_KeyDifferentFromIdentity
    {
        [Key] public Guid ItemTestGid { get; set; }

        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int ItemTestIdent { get; set; } // with fluent Api: modelBuilder.Entity<ItemTest>().Property(p => p.ItemTestIdent ).ValueGeneratedOnAdd();

        public string? Name { get; set; }
    }
}
