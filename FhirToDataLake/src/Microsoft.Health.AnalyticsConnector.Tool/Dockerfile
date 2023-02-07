FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src

COPY ["NuGet.Config", "."]
COPY ["Directory.Build.props", "."]
COPY ["src/Microsoft.Health.AnalyticsConnector.Tool/Microsoft.Health.AnalyticsConnector.Tool.csproj", "src/Microsoft.Health.AnalyticsConnector.Tool/"]
COPY ["src/Microsoft.Health.AnalyticsConnector.Common/Microsoft.Health.AnalyticsConnector.Common.csproj", "src/Microsoft.Health.AnalyticsConnector.Common/"]
COPY ["src/Microsoft.Health.AnalyticsConnector.Core/Microsoft.Health.AnalyticsConnector.Core.csproj", "src/Microsoft.Health.AnalyticsConnector.Core/"]
COPY ["src/Microsoft.Health.AnalyticsConnector.DataClient/Microsoft.Health.AnalyticsConnector.DataClient.csproj", "src/Microsoft.Health.AnalyticsConnector.DataClient/"]
COPY ["src/Microsoft.Health.AnalyticsConnector.DataWriter/Microsoft.Health.AnalyticsConnector.DataWriter.csproj", "src/Microsoft.Health.AnalyticsConnector.DataWriter/"]
COPY ["src/Microsoft.Health.AnalyticsConnector.JobManagement/Microsoft.Health.AnalyticsConnector.JobManagement.csproj", "src/Microsoft.Health.AnalyticsConnector.JobManagement/"]
COPY ["src/Microsoft.Health.AnalyticsConnector.SchemaManagement/Microsoft.Health.AnalyticsConnector.SchemaManagement.csproj", "src/Microsoft.Health.AnalyticsConnector.SchemaManagement/"]
COPY ["src/Microsoft.Health.AnalyticsConnector.HealthCheck/Microsoft.Health.AnalyticsConnector.HealthCheck.csproj", "src/Microsoft.Health.AnalyticsConnector.HealthCheck/"]

RUN dotnet restore "src/Microsoft.Health.AnalyticsConnector.Tool/Microsoft.Health.AnalyticsConnector.Tool.csproj"

COPY . .
WORKDIR "/src/src/Microsoft.Health.AnalyticsConnector.Tool"
ARG BINARY_VERSION
RUN dotnet build "Microsoft.Health.AnalyticsConnector.Tool.csproj" -c Release -o /app/build
FROM build AS publish

ARG BINARY_VERSION
RUN dotnet publish "Microsoft.Health.AnalyticsConnector.Tool.csproj" -c Release -o /app/publish /p:UseAppHost=false
FROM base AS final

WORKDIR /app
COPY --from=publish /app/publish .
RUN echo "/app" >> /etc/ld.so.conf.d/app.conf && ldconfig

ENTRYPOINT ["dotnet", "Microsoft.Health.AnalyticsConnector.Tool.dll"]
