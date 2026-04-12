FROM mcr.microsoft.com/dotnet/sdk:10.0-preview AS build
WORKDIR /src
COPY . .
RUN dotnet publish src/MicroStack/MicroStack.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0-preview
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:4566
EXPOSE 4566
HEALTHCHECK CMD curl -f http://localhost:4566/_ministack/health || exit 1
ENTRYPOINT ["dotnet", "MicroStack.dll"]
