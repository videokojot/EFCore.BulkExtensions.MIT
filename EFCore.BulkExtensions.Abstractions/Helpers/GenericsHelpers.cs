using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions.Helpers;

/// <summary> This class helps to extract properties of the incoming type which have default sql values </summary>
public static class GenericsHelpers
{
    public static IEnumerable<string> GetPropertiesDefaultValue<T>(this T value, Type type, TableInfo tableInfo) where T : class
    {
        // type not obtained from typeof(T) but sent as arg. for IncludeGraph in which case it's not declared the same way
        // Obtain all fields with type pointer.

        System.Reflection.PropertyInfo[] arrayPropertyInfos = type.GetProperties();
        var result = new List<string>();

        foreach (System.Reflection.PropertyInfo field in arrayPropertyInfos)
        {
            if (field.GetIndexParameters().Any()) // Skip Indexer: public string this[string pPropertyName] => string.Empty;
            {
                continue;
            }
            string name = field.Name;
            if (!tableInfo.PropertyColumnNamesDict.ContainsKey(name)) // skip non-EF properties
            {
                continue;
            }
            
            object? temp = field.GetValue(value);
            object? defaultValue = null;
            
            //bypass instance creation if incoming type is an interface or class does not have parameterless constructor
            bool hasParameterlessConstructor = type.GetConstructor(Type.EmptyTypes) != null;
            if (!type.IsInterface && hasParameterlessConstructor)
            {
                defaultValue = field.GetValue(Activator.CreateInstance(type, true));
            }

            if (temp == defaultValue)
            {
                result.Add(name);
            }

            if (temp is Guid guid && guid == Guid.Empty)
            {
                result.Add(name);
            }
        }

        return result;
    }

    public static IEnumerable<string>? GetPropertiesWithDefaultValue<T>(this IEnumerable<T> values, Type type, TableInfo tableInfo) where T : class
    {
        //var result = values.SelectMany(x => x.GetPropertiesDefaultValue(type)).ToList().Distinct(); // TODO: Check all options(ComputedAndDefaultValuesTest) and consider optimisation
        T? firstValue = values.FirstOrDefault();
        IEnumerable<string>? result = firstValue?.GetPropertiesDefaultValue(type, tableInfo)?.Distinct();
        return result;
    }
    
}
