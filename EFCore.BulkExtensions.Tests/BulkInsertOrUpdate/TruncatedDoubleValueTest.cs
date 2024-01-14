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
            // Uncomment line below to get the exception.
            // SetOutputIdentity = true

            // This fails because of getting the identity values, where we expect the key to be identity and be numeric.
            // The logic for getting the output identity for MySQL have several flaws:
            //  -key must be identity,
            // - key must be numeric,
            //  - possible race condition (the code might be affected by other sql statements running at the server))
            //
            // and generally needs to be rewritten. Covered by issue: https://github.com/videokojot/EFCore.BulkExtensions.MIT/issues/90
            
            // For now, the resolution is not to use SetOutputIdentity for MySql as it does not work anyway.
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

        public DbSet<Server> Servers { get; private set; } = null!;
    }

    public class Server
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string Id { get; set; } = null!;

        public string Name { get; set; } = null!;
    }
}
