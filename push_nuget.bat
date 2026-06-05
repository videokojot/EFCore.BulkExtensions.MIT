@echo off
setlocal
cd /d "%~dp0"

set VERSION=11.0.0
set NUGET_API_KEY=<your-api-key-here>
set NUGET_SOURCE=https://api.nuget.org/v3/index.json

dotnet clean

dotnet pack -c Release .\EFCore.BulkExtensions.Abstractions\EFCore.BulkExtensions.Abstractions.csproj
if errorlevel 1 exit /b 1
dotnet pack -c Release .\Providers\EFCore.BulkExtensions.SqlServer\EFCore.BulkExtensions.SqlServer.csproj
if errorlevel 1 exit /b 1
dotnet pack -c Release .\Providers\EFCore.BulkExtensions.SQLite\EFCore.BulkExtensions.SQLite.csproj
if errorlevel 1 exit /b 1
dotnet pack -c Release .\Providers\EFCore.BulkExtensions.PostgreSql\EFCore.BulkExtensions.PostgreSql.csproj
if errorlevel 1 exit /b 1
dotnet pack -c Release .\Providers\EFCore.BulkExtensions.MySql\EFCore.BulkExtensions.MySql.csproj
if errorlevel 1 exit /b 1



dotnet nuget push dist\EFCore.BulkExtensions.MIT.Abstractions.%VERSION%.nupkg -api-key %NUGET_API_KEY% --source %NUGET_SOURCE%
dotnet nuget push dist\EFCore.BulkExtensions.MIT.MySQL.%VERSION%.nupkg        -api-key %NUGET_API_KEY% --source %NUGET_SOURCE%
dotnet nuget push dist\EFCore.BulkExtensions.MIT.PostgreSql.%VERSION%.nupkg   -api-key %NUGET_API_KEY% --source %NUGET_SOURCE%
dotnet nuget push dist\EFCore.BulkExtensions.MIT.SQLite.%VERSION%.nupkg       -api-key %NUGET_API_KEY% --source %NUGET_SOURCE%
dotnet nuget push dist\EFCore.BulkExtensions.MIT.SqlServer.%VERSION%.nupkg    -api-key %NUGET_API_KEY% --source %NUGET_SOURCE%