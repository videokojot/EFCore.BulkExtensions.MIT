using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace EFCore.BulkExtensions.SqlAdapters;

public static class DbServerRegistry
{
    private static readonly List<DbServerRegistration> Registrations = new();
    private static readonly object RegistrationsLock = new();
    private static bool _providersLoaded;

    public static void Register(Func<string, bool> canHandleProviderName, Func<IDbServer> dbServerFactory, bool isFallback = false)
    {
        if (canHandleProviderName is null)
        {
            throw new ArgumentNullException(nameof(canHandleProviderName));
        }

        if (dbServerFactory is null)
        {
            throw new ArgumentNullException(nameof(dbServerFactory));
        }

        DbServerRegistration registration = new DbServerRegistration(canHandleProviderName, dbServerFactory, isFallback);

        lock (RegistrationsLock)
        {
            Registrations.Add(registration);
        }
    }

    internal static IDbServer Resolve(string? providerName)
    {
        string providerNameValue = providerName ?? string.Empty;

        EnsureProvidersLoaded();

        IDbServer? resolved = TryMatch(providerNameValue);

        if (resolved is not null)
        {
            return resolved;
        }

        bool anyRegistered;

        lock (RegistrationsLock)
        {
            anyRegistered = Registrations.Count > 0;
        }

        if (!anyRegistered)
        {
            throw new InvalidOperationException("No EFCore.BulkExtensions database provider is registered. Reference a provider package (for example EFCore.BulkExtensions.MIT.SqlServer).");
        }

        throw new InvalidOperationException($"No registered EFCore.BulkExtensions database provider can handle the EF Core provider '{providerNameValue}'.");
    }

    private static void EnsureProvidersLoaded()
    {
        lock (RegistrationsLock)
        {
            if (_providersLoaded)
            {
                return;
            }

            _providersLoaded = true;
        }

        string baseDirectory = AppContext.BaseDirectory;

        if (!Directory.Exists(baseDirectory))
        {
            return;
        }

        foreach (string candidateFile in Directory.GetFiles(baseDirectory, "EFCore.BulkExtensions.*.dll"))
        {
            string fileName = Path.GetFileName(candidateFile);

            if (fileName.Equals("EFCore.BulkExtensions.Abstractions.dll", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            try
            {
                Assembly assembly = Assembly.Load(AssemblyName.GetAssemblyName(candidateFile));
                RegisterProvidersFrom(assembly);
            }
            catch (Exception)
            {
            }
        }
    }

    private static void RegisterProvidersFrom(Assembly assembly)
    {
        Type?[] types;

        try
        {
            types = assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            types = ex.Types;
        }

        foreach (Type? type in types)
        {
            if (type is null || !type.Name.EndsWith("ProviderRegistration", StringComparison.Ordinal))
            {
                continue;
            }

            MethodInfo? method = type.GetMethod("Register", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);

            if (method is null || method.GetParameters().Length != 0)
            {
                continue;
            }

            method.Invoke(null, null);
        }
    }

    private static IDbServer? TryMatch(string providerNameValue)
    {
        DbServerRegistration[] snapshot;

        lock (RegistrationsLock)
        {
            snapshot = Registrations.ToArray();
        }

        foreach (DbServerRegistration registration in snapshot)
        {
            if (!registration.IsFallback && registration.CanHandleProviderName(providerNameValue))
            {
                IDbServer matched = registration.GetOrCreate();

                return matched;
            }
        }

        foreach (DbServerRegistration registration in snapshot)
        {
            if (registration.IsFallback)
            {
                IDbServer fallback = registration.GetOrCreate();

                return fallback;
            }
        }

        return null;
    }
}
