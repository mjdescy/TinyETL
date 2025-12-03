# Example 18: HTTP Source with Custom Headers and Authentication

This example demonstrates how to download data from HTTP/HTTPS endpoints with custom headers and authentication in TinyETL.

## What This Example Shows

1. **Custom HTTP Headers**: How to add custom headers to HTTP requests
2. **Basic Authentication**: Using username/password authentication
3. **Bearer Token Authentication**: Using token-based authentication
4. **Environment Variable Support**: Securely passing credentials via environment variables

## Protocol Options

TinyETL supports the following options for HTTP/HTTPS sources:

| Option | Description | Example |
|--------|-------------|---------|
| `header.<name>` | Custom HTTP header | `header.User-Agent: "TinyETL/0.9.0"` |
| `auth.basic.username` | Basic auth username | `auth.basic.username: "${API_USERNAME}"` |
| `auth.basic.password` | Basic auth password | `auth.basic.password: "${API_PASSWORD}"` |
| `auth.bearer` | Bearer token | `auth.bearer: "${API_TOKEN}"` |

## Configuration Example

```yaml
version: 1

source:
  uri: "https://api.example.com/data/export.csv"
  options:
    # Custom headers
    header.User-Agent: "TinyETL/0.9.0"
    header.Accept: "text/csv"
    header.X-API-Version: "v2"
    
    # Basic authentication
    auth.basic.username: "${API_USERNAME}"
    auth.basic.password: "${API_PASSWORD}"

target:
  uri: "output.csv"
  options: {}

options:
  batch_size: 1000
  log_level: info
```

## Running the Example

### Test with Public Data (No Authentication)

```bash
./run.sh
```

This will download sample data from a public CSV source to demonstrate the HTTP protocol with custom headers.

### With Basic Authentication

```bash
export API_USERNAME=your_username
export API_PASSWORD=your_password
tinyetl run config.yaml
```

### With Bearer Token

First, update your `config.yaml` to use bearer token:

```yaml
source:
  uri: "https://api.example.com/data"
  options:
    auth.bearer: "${API_TOKEN}"
```

Then run:

```bash
export API_TOKEN=your_token_here
tinyetl run config.yaml
```

## Security Best Practices

1. **Never hardcode credentials** in configuration files
2. **Use environment variables** for sensitive data
3. **Keep config files** out of version control if they contain sensitive information
4. **Use `.gitignore`** to exclude files with credentials

## Common Use Cases

- **REST API Data Export**: Download data exports from REST APIs
- **Authenticated Webhooks**: Process data from authenticated webhook endpoints  
- **Cloud Storage**: Download files from cloud storage with signed URLs
- **Data Feeds**: Subscribe to authenticated data feeds
- **Internal APIs**: Access internal company APIs with authentication

## Error Handling

If you encounter authentication errors:

- Verify your credentials are correct
- Check if the API requires specific headers
- Ensure environment variables are set
- Check the API documentation for authentication requirements

## Notes

- HTTP targets (uploading) are not currently supported
- File downloads are cached temporarily during processing
- Large files are downloaded completely before processing begins
