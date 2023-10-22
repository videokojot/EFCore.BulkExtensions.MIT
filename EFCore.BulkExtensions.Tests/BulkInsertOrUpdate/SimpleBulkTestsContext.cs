using Microsoft.EntityFrameworkCore;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public class SimpleBulkTestsContext : DbContext
{
    public DbSet<SimpleItem> SimpleItems { get; set; } = null!;

    public DbSet<Entity_KeyDifferentFromIdentity> EntityKeyDifferentFromIdentities { get; set; } = null!;

    public DbSet<Entity_CustomColumnNames> EntityCustomColumnNames { get; set; } = null!;

    public SimpleBulkTestsContext(DbContextOptions options)
        : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
    }
}

public class SimpleItem
{
    public int Id { get; set; }

    public string? Name { get; set; }

    public Guid BulkIdentifier { get; set; }

    public Guid GuidProperty { get; set; }

    public string? StringProperty { get; set; }
}

public class Entity_KeyDifferentFromIdentity
{
    [Key] public Guid ItemTestGid { get; set; }

    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int ItemTestIdent { get; set; } // with fluent Api: modelBuilder.Entity<ItemTest>().Property(p => p.ItemTestIdent ).ValueGeneratedOnAdd();

    public string? Name { get; set; }
}

public class Entity_CustomColumnNames
{
    [Column("Id")] public long Id { get; set; }

    [Column("Custom_Column")] public string? CustomColumn { get; set; }

    [Column("Guid_Property")] public Guid GuidProperty { get; set; }
}
