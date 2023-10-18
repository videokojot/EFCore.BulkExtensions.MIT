using EFCore.BulkExtensions.SqlAdapters;

namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public class InsertNewOnlyTests : IClassFixture<InsertNewOnlyTests.DatabaseFixture>, IAssemblyFixture<DbAssemblyFixture>
{
    private readonly DatabaseFixture _dbFixture;

    public InsertNewOnlyTests(DatabaseFixture dbFixture)
    {
        _dbFixture = dbFixture;
    }

    /// <summary>
    /// Covers issue: https://github.com/borisdj/EFCore.BulkExtensions/issues/321
    /// </summary>
    [Theory]
    [InlineData(DbServerType.SQLServer)]
    [InlineData(DbServerType.PostgreSQL)]
    public void BulkInsertOrUpdate_InsertNewOnly(DbServerType dbType)
    {
        var bulkId = Guid.NewGuid();

        var initialItem = new SimpleItem()
        {
            Name = "initial1",
            BulkIdentifier = bulkId,
            GuidProperty = Guid.NewGuid(),
        };

        using (var db = _dbFixture.GetDb(dbType))
        {
            db.SimpleItems.Add(initialItem);
            db.SaveChanges();
        }

        var initialItemId = initialItem.Id;

        // Should be inserted
        var newItem = new SimpleItem()
        {
            Id = 0,
            StringProperty = "insertedByBulk",
            BulkIdentifier = bulkId,
            GuidProperty = Guid.NewGuid(),
        };

        // Should not be updated (since we use insert only new scenario):
        var updatedItem = new SimpleItem()
        {
            Id = initialItemId,
            BulkIdentifier = bulkId,
            Name = "updated by Bulks",
            GuidProperty = Guid.NewGuid(),
        };

        using (var db = _dbFixture.GetDb(dbType))
        {
            var ensureList = new[] { newItem, updatedItem };

            db.BulkInsertOrUpdate(ensureList,
                                  c =>
                                  {
                                      c.PropertiesToIncludeOnUpdate = new() { "" };
                                  });
        }
        
        var allItems = GetItemsOfBulk(bulkId, dbType);

        Assert.Equal(2, allItems.Count);

        var insertedItem = allItems.SingleOrDefault(x => x.GuidProperty == newItem.GuidProperty);
        Assert.NotNull(insertedItem);

        // initial item was not updated:
        var itemWhichWasNotUpdated = allItems.Single(x => x.GuidProperty == initialItem.GuidProperty);
        Assert.Equal(initialItem.Id, itemWhichWasNotUpdated.Id);
        Assert.Equal(initialItem.Name, itemWhichWasNotUpdated.Name);
    }

    private List<SimpleItem> GetItemsOfBulk(Guid bulkId, DbServerType sqlType)
    {
        using var db = _dbFixture.GetDb(sqlType);

        return db.SimpleItems.Where(x => x.BulkIdentifier == bulkId).ToList();
    }

    public class DatabaseFixture : BulkDbTestsFixture
    {
        public DatabaseFixture() : base(nameof(InsertNewOnlyTests))
        {
        }
    }
}
