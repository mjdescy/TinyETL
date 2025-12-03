FROM python:3.11-slim

WORKDIR /app

# Install Flask, waitress (production WSGI server), and curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir flask==3.0.0 waitress==3.0.0

# Copy server code
COPY server.py /app/server.py

# Create data directory
RUN mkdir -p /app/data

# Expose port
EXPOSE 8680

# Run the server with waitress (production WSGI server)
CMD ["python", "-c", "import os; print('='*60); print('TinyETL HTTP Test Server Starting...'); print('='*60); print(f\"Basic Auth: {os.getenv('BASIC_AUTH_USERNAME', 'testuser')} / {os.getenv('BASIC_AUTH_PASSWORD', 'testpass')}\"); print(f\"Bearer Token: {os.getenv('BEARER_TOKEN', 'test-bearer-token-12345')}\"); print('='*60); print('Server listening on http://0.0.0.0:8680'); print('='*60); from waitress import serve; from server import app; serve(app, host='0.0.0.0', port=8680)"]
