FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
RUN apk add --no-cache clang binutils musl-dev build-base zlib-dev zlib-static
WORKDIR /src

# Copy props/project files first for layer caching
COPY Directory.Build.props ./
COPY MicroStack.slnx ./
COPY src/MicroStack/MicroStack.csproj src/MicroStack/
COPY src/MicroStack.UI.Client/MicroStack.UI.Client.csproj src/MicroStack.UI.Client/
COPY src/MicroStack.Admin.Contracts/MicroStack.Admin.Contracts.csproj src/MicroStack.Admin.Contracts/

RUN dotnet restore src/MicroStack/MicroStack.csproj -r linux-musl-x64

# Copy source and publish the API with its browser assets
COPY src/ src/
RUN dotnet publish src/MicroStack/MicroStack.csproj -c Release -r linux-musl-x64 -o /out/microstack

FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-alpine
WORKDIR /app
COPY --from=build /out/microstack/ /app/

ENV ASPNETCORE_HTTP_PORTS=4566
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
EXPOSE 4566
ENTRYPOINT ["/app/MicroStack"]
