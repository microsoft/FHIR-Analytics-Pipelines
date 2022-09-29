#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["NuGet.Config", "."]
COPY ["src/Microsoft.Health.Fhir.Synapse.Tool/Microsoft.Health.Fhir.Synapse.Tool.csproj", "src/Microsoft.Health.Fhir.Synapse.Tool/"]
COPY ["src/Microsoft.Health.Fhir.Synapse.Common/Microsoft.Health.Fhir.Synapse.Common.csproj", "src/Microsoft.Health.Fhir.Synapse.Common/"]
COPY ["src/Microsoft.Health.Fhir.Synapse.Core/Microsoft.Health.Fhir.Synapse.Core.csproj", "src/Microsoft.Health.Fhir.Synapse.Core/"]
COPY ["src/Microsoft.Health.Fhir.Synapse.DataClient/Microsoft.Health.Fhir.Synapse.DataClient.csproj", "src/Microsoft.Health.Fhir.Synapse.DataClient/"]
COPY ["src/Microsoft.Health.Fhir.Synapse.DataWriter/Microsoft.Health.Fhir.Synapse.DataWriter.csproj", "src/Microsoft.Health.Fhir.Synapse.DataWriter/"]
COPY ["src/Microsoft.Health.Fhir.Synapse.JobManagement/Microsoft.Health.Fhir.Synapse.JobManagement.csproj", "src/Microsoft.Health.Fhir.Synapse.JobManagement/"]
COPY ["src/Microsoft.Health.Fhir.Synapse.SchemaManagement/Microsoft.Health.Fhir.Synapse.SchemaManagement.csproj", "src/Microsoft.Health.Fhir.Synapse.SchemaManagement/"]
COPY ["src/Microsoft.Health.Fhir.Synapse.HealthCheck/Microsoft.Health.Fhir.Synapse.HealthCheck.csproj", "src/Microsoft.Health.Fhir.Synapse.HealthCheck/"]
RUN dotnet restore "src/Microsoft.Health.Fhir.Synapse.Tool/Microsoft.Health.Fhir.Synapse.Tool.csproj"
COPY . .
WORKDIR "/src/src/Microsoft.Health.Fhir.Synapse.Tool"
RUN dotnet build "Microsoft.Health.Fhir.Synapse.Tool.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Microsoft.Health.Fhir.Synapse.Tool.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
RUN echo "/app" >> /etc/ld.so.conf.d/app.conf && ldconfig
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/app"
ENTRYPOINT ["dotnet", "Microsoft.Health.Fhir.Synapse.Tool.dll"]