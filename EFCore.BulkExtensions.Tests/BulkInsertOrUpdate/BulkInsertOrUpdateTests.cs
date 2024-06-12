using EFCore.BulkExtensions.SqlAdapters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Xunit.Abstractions;


namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public class BulkInsertOrUpdateTests : IClassFixture<BulkInsertOrUpdateTests.DatabaseFixture>, IAssemblyFixture<DbAssemblyFixture>
{
    private readonly ITestOutputHelper _writer;
    private readonly DatabaseFixture _dbFixture;

    public BulkInsertOrUpdateTests(ITestOutputHelper writer, DatabaseFixture dbFixture)
    {
        _writer = writer;
        _dbFixture = dbFixture;
    }

    /// <summary>
    /// Covers issue: https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/46
    /// Original: https://github.com/borisdj/EFCore.BulkExtensions/issues/1249
    /// </summary>
    [Theory]
    [InlineData(DbServerType.SQLServer)]
    public void BulkInsertOrUpdate_CustomUpdateBy_WithOutputIdentity_NullKeys(DbServerType dbType)
    {
        using var db = _dbFixture.GetDb(dbType);

        var newItem = new SimpleItem()
        {
            StringProperty = null,
            GuidProperty = Guid.NewGuid(),
        };

        var ensureList = new[] { newItem, };

        db.BulkInsertOrUpdate(ensureList, c =>
        {
            c.SetOutputIdentity = true;
            c.UpdateByProperties = new List<string> { nameof(SimpleItem.StringProperty) };
            c.PreserveInsertOrder = true;
        });

        var fromDb = db.SimpleItems.Single(x => x.GuidProperty == newItem.GuidProperty);

        Assert.NotNull(fromDb); // Item was inserted

        // Ids were correctly filled
        Assert.Equal(fromDb.Id, newItem.Id);
        Assert.NotEqual(0, newItem.Id);
    }

    /// <summary>
    /// Covers issue: https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/45
    /// Original: https://github.com/borisdj/EFCore.BulkExtensions/issues/1248
    /// </summary>
    [Theory]
    [InlineData(DbServerType.SQLServer)]
    public void BulkInsertOrUpdate_InsertOnlyNew_SetOutputIdentity(DbServerType dbType)
    {
        var bulkId = Guid.NewGuid();
        var existingItemId = "existingId";

        var initialItem = new SimpleItem()
        {
            StringProperty = existingItemId,
            Name = "initial1",
            BulkIdentifier = bulkId,
            GuidProperty = Guid.NewGuid(),
        };

        using (var db = _dbFixture.GetDb(dbType))
        {
            db.SimpleItems.Add(initialItem);
            db.SaveChanges();
        }

        using (var db = _dbFixture.GetDb(dbType))
        {
            var newItem = new SimpleItem()
            {
                StringProperty = "insertedByBulk",
                BulkIdentifier = bulkId,
                GuidProperty = Guid.NewGuid(),
            };

            var updatedItem = new SimpleItem()
            {
                StringProperty = existingItemId,
                BulkIdentifier = bulkId,
                Name = "updated by Bulks",
                GuidProperty = Guid.NewGuid(),
            };

            var ensureList = new[] { newItem, updatedItem };

            db.BulkInsertOrUpdate(ensureList, c =>
            {
                c.PreserveInsertOrder = true;
                c.UpdateByProperties =
                    new List<string> { nameof(SimpleItem.StringProperty), nameof(SimpleItem.BulkIdentifier) };
                c.PropertiesToIncludeOnUpdate = new List<string> { "" };
                c.SetOutputIdentity = true;
            });

            var dbItems = db.GetItemsOfBulk(bulkId).OrderBy(x => x.GuidProperty).ToList();

            var updatedDb = dbItems.Single(x => x.GuidProperty == initialItem.GuidProperty);
            var newDb = dbItems.Single(x => x.GuidProperty == newItem.GuidProperty);

            Assert.Equal(updatedDb.Id, updatedItem.Id); // output identity was set

            // Rest of properties were not updated:
            Assert.Equal(updatedDb.Name, initialItem.Name);
            Assert.Equal(updatedDb.StringProperty, initialItem.StringProperty);
            Assert.Equal(updatedDb.BulkIdentifier, initialItem.BulkIdentifier);

            Assert.Equal(newDb.Id, newItem.Id);
            Assert.Equal(newDb.Name, newItem.Name);
            Assert.Equal(newDb.StringProperty, newItem.StringProperty);
            Assert.Equal(newDb.BulkIdentifier, newItem.BulkIdentifier);
        }
    }

    /// <summary>
    /// Covers: https://github.com/borisdj/EFCore.BulkExtensions/issues/1250
    /// </summary>
    [Theory]
    [InlineData(DbServerType.SQLServer)]
    public void BulkInsertOrUpdate_SetOutputIdentity_SetOutputNonIdentityColumns(DbServerType dbType)
    {
        using var db = _dbFixture.GetDb(dbType);

        var newItem = new SimpleItem()
        {
            StringProperty = "newItem",
            GuidProperty = Guid.NewGuid(),
            Name = "newName",
        };

        var ensureList = new[] { newItem, };

        // OutputIdentity == true && SetOutputNonIdentityColumns == false
        db.BulkInsertOrUpdate(ensureList,
                              c =>
                              {
                                  c.SetOutputIdentity = true;
                                  c.UpdateByProperties = new List<string> { nameof(SimpleItem.StringProperty), nameof(SimpleItem.Name) };
                                  c.PreserveInsertOrder = true;
                                  c.SetOutputNonIdentityColumns = false; 
                              });

        var fromDb = db.SimpleItems.SingleOrDefault(x => x.GuidProperty == newItem.GuidProperty);
        Assert.NotNull(fromDb); // Item was inserted!

        Assert.NotEqual(0, newItem.Id);
    }

    /// <summary>
    /// Covers: https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/48
    /// Original issue: https://github.com/borisdj/EFCore.BulkExtensions/issues/1251
    /// </summary>
    [Theory]
    [InlineData(DbServerType.SQLServer)]
    public void BulkInsertOrUpdate_WillNotSetOutputIdentityIfThereIsConflict(DbServerType dbType)
    {
        var bulkId = Guid.NewGuid();
        var existingItemId = "existingDuplicateOf";

        var initialItem = new SimpleItem()
        {
            StringProperty = existingItemId,
            Name = "initial1",
            BulkIdentifier = bulkId,
        };

        var duplicateInitial = new SimpleItem()
        {
            StringProperty = existingItemId,
            Name = "initial2",
            BulkIdentifier = bulkId,
        };

        using (var db = _dbFixture.GetDb(dbType))
        {
            db.SimpleItems.Add(initialItem);
            db.SimpleItems.Add(duplicateInitial);
            db.SaveChanges();

            Assert.NotEqual(0, initialItem.Id);
            Assert.NotEqual(0, duplicateInitial.Id);
        }

        using (var db = _dbFixture.GetDb(dbType))
        {
            var updatingItem = new SimpleItem()
            {
                StringProperty = existingItemId,
                BulkIdentifier = bulkId,
                Name = "updates multiple rows",
            };

            var ensureList = new[] { updatingItem, };

            // We throw we cannot set output identity deterministically.
            var exception = Assert.Throws<BulkExtensionsException>(
                () => db.BulkInsertOrUpdate(ensureList, c =>
                {
                    c.UpdateByProperties = new List<string> { nameof(SimpleItem.StringProperty), nameof(SimpleItem.BulkIdentifier) };
                    c.SetOutputIdentity = true;
                    c.PreserveInsertOrder = true;
                }));

            Assert.Equal(BulkExtensionsExceptionType.CannotSetOutputIdentityForNonUniqueUpdateByProperties, exception.ExceptionType);
            Assert.StartsWith("Items were Inserted/Updated successfully in db, but we cannot set output identity correctly since single source row(s) matched multiple rows in db.", exception.Message);

            var bulkItems = db.GetItemsOfBulk(bulkId);

            Assert.Equal(2, bulkItems.Count);

            // Item updated both of the matching rows
            foreach (var dbItem in bulkItems)
            {
                Assert.Equal(updatingItem.Name, dbItem.Name);
            }
        }
    }


    /// <summary>
    /// Covers: https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/62
    /// </summary>
    [Theory]
    [InlineData(DbServerType.SQLServer, true)]
    [InlineData(DbServerType.SQLServer, false)]
    public void IUD_UpdateByCustomColumns_SetOutputIdentity_CustomColumnNames(DbServerType dbType, bool setOutputNonIdColumns)
    {
        var item = new Entity_CustomColumnNames()
        {
            CustomColumn = "Value1",
            GuidProperty = Guid.NewGuid(),
        };

        var item2 = new Entity_CustomColumnNames()
        {
            CustomColumn = "Value2",
            GuidProperty = Guid.NewGuid(),
        };

        using (var db = _dbFixture.GetDb(dbType))
        {
            var items = new[] { item, item2 };
            db.BulkInsertOrUpdateOrDelete(items, c =>
            {
                c.SetOutputIdentity = true;
                c.UpdateByProperties = new List<string> { nameof(Entity_CustomColumnNames.CustomColumn) };
                c.SetOutputNonIdentityColumns = setOutputNonIdColumns;
            });
        }

        using (var db = _dbFixture.GetDb(dbType))
        {
            var insertedItem = db.EntityCustomColumnNames.Single(x => x.GuidProperty == item.GuidProperty);
            Assert.Equal("Value1", insertedItem.CustomColumn);
        }
    }

    [Theory]
    [InlineData(DbServerType.SQLServer)]
    public void BulkInsertOrUpdate_ReloadList_IsWorking(DbServerType dbServerType)
    {
        using var db = _dbFixture.GetDb(dbServerType);

        var newItem = new SimpleItem()
        {
            StringProperty = "newItem",
            GuidProperty = Guid.NewGuid(),
            Name = "newName",
        };

        var newItem2 = new SimpleItem()
        {
            StringProperty = "newItem2",
            GuidProperty = Guid.NewGuid(),
            Name = "newName2",
        };

        var ensureList = new List<SimpleItem>() { newItem, newItem2 };

        db.BulkInsertOrUpdate(ensureList, config =>
        {
            config.SetOutputNonIdentityColumns = true;
            config.SetOutputIdentity = true;
            config.PreserveInsertOrder = false;
        });

        Assert.NotSame(ensureList[0], newItem); // Items were reloaded

        Assert.True(newItem.Id == 0);  // We did not touch original object
        Assert.True(newItem2.Id == 0); // We did not touch original object

        Assert.True(ensureList[0].Id != 0);
        Assert.True(ensureList[1].Id != 0);
    }

    /// <summary>
    /// Covers: https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/131
    /// </summary>
    [Theory]
    [InlineData(DbServerType.SQLite)]
    public async Task BulkInsertOrUpdate_InsertsOrUpdatesByPropertyIn(DbServerType dbType)
    {
        await using var db = _dbFixture.GetDb(dbType);

        var bulkConfig = new BulkConfig
        {
            UpdateByProperties = new List<string> { nameof(UniqueItem.FirstName), nameof(UniqueItem.LastName) }
        };

        var mockFirstName = Guid.NewGuid().ToString();
        var mockLastName = Guid.NewGuid().ToString();

        const int testSetSize = 2;
        var testSet = Enumerable.Range(0, testSetSize)
            .Select(x => new UniqueItem()
            {
                FirstName = mockFirstName + x,
                LastName = mockLastName + x
            });
        
        // duplicate enumeration is intentional - I want to create set of duplicates
        // to verify that the "insert or update" (aka merge) works correctly and
        // allows for database generated identity columns while doing so
        var duplicateSet = testSet.Concat(testSet).ToList();
        
        await db.BulkInsertOrUpdateAsync(duplicateSet, bulkConfig, null, null, CancellationToken.None);
        
        Assert.Equal(testSetSize, await db.UniqueItems.CountAsync());
        // before fix, it attempted to insert identity column, so it would insert "0" in this case, as it is default(int)
        Assert.True(await db.UniqueItems.AllAsync(x => x.Id > 0)); 
    }

    public class DatabaseFixture : BulkDbTestsFixture<SimpleBulkTestsContext>
    {
        protected override string DbName => nameof(BulkInsertOrUpdateTests);
    }
}