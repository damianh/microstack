FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
RUN apk add --no-cache clang binutils musl-dev build-base zlib-dev zlib-static
WORKDIR /src

# Copy props/solution files first for layer caching
COPY Directory.Build.props ./
COPY src/MicroStack/MicroStack.csproj src/MicroStack/

RUN dotnet restore src/MicroStack/MicroStack.csproj -r linux-musl-x64

# Copy source and publish
COPY src/MicroStack/ src/MicroStack/
RUN dotnet publish src/MicroStack/MicroStack.csproj -c Release -r linux-musl-x64 -o /app --no-restore

FROM alpine:3.21
COPY --from=build /app /app
ENV ASPNETCORE_HTTP_PORTS=4566
EXPOSE 4566
ENTRYPOINT ["/app/MicroStack"]
