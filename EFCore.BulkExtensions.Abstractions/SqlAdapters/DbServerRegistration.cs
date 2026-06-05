using System;

namespace EFCore.BulkExtensions.SqlAdapters;

internal sealed class DbServerRegistration
{
    private readonly Func<IDbServer> _factory;
    private readonly object _instanceLock = new();
    private IDbServer? _instance;

    public DbServerRegistration(Func<string, bool> canHandleProviderName, Func<IDbServer> factory, bool isFallback)
    {
        CanHandleProviderName = canHandleProviderName;
        _factory = factory;
        IsFallback = isFallback;
    }

    public Func<string, bool> CanHandleProviderName { get; }

    public bool IsFallback { get; }

    public IDbServer GetOrCreate()
    {
        if (_instance is not null)
        {
            return _instance;
        }

        lock (_instanceLock)
        {
            _instance ??= _factory();
        }

        return _instance;
    }
}