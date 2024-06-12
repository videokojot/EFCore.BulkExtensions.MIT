using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;

namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public class SimpleBulkTestsContext : DbContext
{
    public DbSet<SimpleItem> SimpleItems { get; set; } = null!;

    public DbSet<Entity_CustomColumnNames> EntityCustomColumnNames { get; set; } = null!;
    
    public DbSet<UniqueItem> UniqueItems { get; set; } = null!;

    public SimpleBulkTestsContext(DbContextOptions options)
        : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UniqueItem>()
            .HasIndex(x => new { x.FirstName, x.LastName })
            .IsUnique();
    }

    public List<SimpleItem> GetItemsOfBulk(Guid bulkId)
    {
        return SimpleItems.Where(x => x.BulkIdentifier == bulkId).ToList();
    }
}

public class SimpleItem
{
    public int Id { get; set; }

    public string? Name { get; set; }

    public Guid BulkIdentifier { get; set; }

    public Guid GuidProperty { get; set; }

    [MaxLength(500)]
    public string? StringProperty { get; set; }
}

public class UniqueItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public string FirstName { get; set; } = "";

    public string LastName { get; set; } = "";
}

public class Entity_CustomColumnNames
{
    [Column("Id")]
    public long Id { get; set; }

    [Column("Custom_Column")]
    public string? CustomColumn { get; set; }

    [Column("Guid_Property")]
    public Guid GuidProperty { get; set; }
}
