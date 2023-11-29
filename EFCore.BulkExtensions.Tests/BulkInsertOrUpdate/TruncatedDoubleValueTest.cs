using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public class TruncatedDoubleValueTest : IClassFixture<TruncatedDoubleValueTest.DatabaseFixture>, IAssemblyFixture<DbAssemblyFixture>
{
    private readonly DatabaseFixture _dbFixture;

    public TruncatedDoubleValueTest(DatabaseFixture dbFixture)
    {
        _dbFixture = dbFixture;
    }

    /// <summary>
    /// Covers issue: https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/85
    /// </summary>
    [Theory]
    [InlineData(DbServerType.MySQL)]
    [InlineData(DbServerType.SQLServer)]
    public async Task CheckThatWeDoNotTruncateValue(DbServerType serverType)
    {
        using var dbContext = _dbFixture.GetDb(serverType);
        var servers = Enumerable.Range(500, 1000).Select(v => new Server() { Id = $"00e{v}", Name = $"Name2: {v}" }).ToList();

        await dbContext.BulkInsertOrUpdateAsync(servers, new BulkConfig()
        {
            SetOutputIdentity = true
        });

        // Exception
        await dbContext.BulkInsertOrUpdateAsync(new Server[] { new Server() { Id = "00264417301d87175b75afaed8bf838e", Name = "abc" } }, new BulkConfig()
        {
            SetOutputIdentity = true
        });
    }

    public class DatabaseFixture : BulkDbTestsFixture<GameDbContext>
    {
        protected override string DbName => nameof(TruncatedDoubleValueTest);
    }

    public class GameDbContext : DbContext
    {
        public GameDbContext(DbContextOptions dbContextOptions) : base(dbContextOptions)
        {
        }

        public DbSet<Server> Servers { get; private set; }
    }

    public class Server
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string Id { get; set; }

        public string Name { get; set; }
    }
}
