using System;

namespace EFCore.BulkExtensions;

public class BulkExtensionsException : Exception
{
    public BulkExtensionsExceptionType ExceptionType { get; }

    public BulkExtensionsException(BulkExtensionsExceptionType exceptionType, string message) : base(message)
    {
        ExceptionType = exceptionType;
    }
}

public enum BulkExtensionsExceptionType
{
    CannotSetOutputIdentityForNonUniqueUpdateByProperties,
}
