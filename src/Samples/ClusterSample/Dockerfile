# Build context: src/Samples (see docker-compose.yml), so nuget.config and LocalPackages are in reach.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY nuget.config .
COPY LocalPackages/ LocalPackages/
COPY ClusterSample/ClusterSample.csproj ClusterSample/
RUN dotnet restore ClusterSample/ClusterSample.csproj
COPY ClusterSample/ ClusterSample/
RUN dotnet publish ClusterSample/ClusterSample.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "ClusterSample.dll"]
