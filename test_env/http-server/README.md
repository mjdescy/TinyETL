# TinyETL HTTP Server Test Configurations

This directory contains a lightweight HTTP server for testing TinyETL's HTTP protocol with various authentication methods.

## Server Setup

The HTTP server is automatically started with docker-compose:

```bash
cd test_env
docker-compose up -d http-server
```

## Available Endpoints

### 1. Public Endpoint (No Authentication)
- **URL**: `http://localhost:8080/public/data.csv`
- **Auth**: None required
- **Data**: Sample user data CSV

### 2. Basic Authentication
- **URL**: `http://localhost:8080/basic-auth/users.csv`
- **Auth**: Basic Auth
- **Username**: `testuser`
- **Password**: `testpass`
- **Data**: Sample user data CSV

### 3. Bearer Token Authentication
- **URL**: `http://localhost:8080/bearer-auth/products.csv`
- **Auth**: Bearer Token
- **Token**: `test-bearer-token-12345`
- **Data**: Sample products CSV

### 4. Custom Headers
- **URL**: `http://localhost:8080/custom-headers/data.csv`
- **Auth**: None
- **Required Header**: `X-API-Version: v2`
- **Data**: Sample user data CSV

### 5. Multi-Authentication
- **URL**: `http://localhost:8080/multi-auth/data.csv`
- **Auth**: Basic Auth OR Bearer Token
- **Data**: Varies based on auth method

## Test Configurations

### Test 1: Public CSV (No Auth)

```yaml
version: 1
source:
  uri: "http://localhost:8080/public/data.csv"
  options: {}
target:
  uri: "output_public.json"
  options: {}
options:
  log_level: info
```

### Test 2: Basic Authentication

```yaml
version: 1
source:
  uri: "http://localhost:8080/basic-auth/users.csv"
  options:
    auth.basic.username: "testuser"
    auth.basic.password: "testpass"
target:
  uri: "output_basic_auth.json"
  options: {}
options:
  log_level: info
```

### Test 3: Bearer Token Authentication

```yaml
version: 1
source:
  uri: "http://localhost:8080/bearer-auth/products.csv"
  options:
    auth.bearer: "test-bearer-token-12345"
target:
  uri: "output_bearer_auth.json"
  options: {}
options:
  log_level: info
```

### Test 4: Custom Headers

```yaml
version: 1
source:
  uri: "http://localhost:8080/custom-headers/data.csv"
  options:
    header.X-API-Version: "v2"
    header.User-Agent: "TinyETL/0.9.0"
target:
  uri: "output_custom_headers.json"
  options: {}
options:
  log_level: info
```

## Running Tests

```bash
# Start the server
cd test_env
docker-compose up -d http-server

# Wait for server to be ready
docker-compose ps http-server

# Test with curl
curl http://localhost:8080/health

# Run TinyETL tests
cd ..
cargo test test_http_protocol

# Or manually test each endpoint
tinyetl run test_env/http-server/configs/public.yaml
tinyetl run test_env/http-server/configs/basic-auth.yaml
tinyetl run test_env/http-server/configs/bearer-auth.yaml
```

## Debugging

Check server logs:
```bash
docker-compose logs http-server
```

Test endpoints manually:
```bash
# Public endpoint
curl http://localhost:8080/public/data.csv

# Basic auth
curl -u testuser:testpass http://localhost:8080/basic-auth/users.csv

# Bearer token
curl -H "Authorization: Bearer test-bearer-token-12345" \
     http://localhost:8080/bearer-auth/products.csv

# Custom headers
curl -H "X-API-Version: v2" \
     http://localhost:8080/custom-headers/data.csv
```
