using EFCore.BulkExtensions.Tests;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

// xUnit v3 built-in assembly-wide fixture: created before any test runs and disposed after all tests finish.
[assembly: AssemblyFixture(typeof(DbAssemblyFixture))]
