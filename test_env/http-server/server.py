#!/usr/bin/env python3
"""
Lightweight HTTP server for testing TinyETL HTTP protocol with authentication.
Supports both Basic Auth and Bearer Token authentication.
"""

from flask import Flask, Response, request, jsonify
import os
import base64
from functools import wraps

app = Flask(__name__)

# Authentication credentials from environment
BASIC_AUTH_USERNAME = os.getenv('BASIC_AUTH_USERNAME', 'testuser')
BASIC_AUTH_PASSWORD = os.getenv('BASIC_AUTH_PASSWORD', 'testpass')
BEARER_TOKEN = os.getenv('BEARER_TOKEN', 'test-bearer-token-12345')

# Sample CSV data
SAMPLE_CSV = """id,name,email,age,city
1,Alice Johnson,alice@example.com,28,New York
2,Bob Smith,bob@example.com,35,Los Angeles
3,Carol Williams,carol@example.com,42,Chicago
4,David Brown,david@example.com,31,Houston
5,Eve Davis,eve@example.com,26,Phoenix
6,Frank Miller,frank@example.com,45,Philadelphia
7,Grace Wilson,grace@example.com,33,San Antonio
8,Henry Moore,henry@example.com,29,San Diego
9,Ivy Taylor,ivy@example.com,38,Dallas
10,Jack Anderson,jack@example.com,41,San Jose"""

PRODUCTS_CSV = """product_id,product_name,category,price,stock
101,Laptop,Electronics,999.99,50
102,Mouse,Electronics,29.99,200
103,Keyboard,Electronics,79.99,150
104,Monitor,Electronics,299.99,75
105,Desk Chair,Furniture,199.99,30
106,Standing Desk,Furniture,499.99,20
107,Coffee Maker,Appliances,89.99,100
108,Blender,Appliances,59.99,80
109,Headphones,Electronics,149.99,120
110,Webcam,Electronics,79.99,90"""


def check_basic_auth(username, password):
    """Check if username and password match."""
    return username == BASIC_AUTH_USERNAME and password == BASIC_AUTH_PASSWORD


def check_bearer_token(token):
    """Check if bearer token matches."""
    return token == BEARER_TOKEN


def require_basic_auth(f):
    """Decorator to require HTTP Basic Authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_basic_auth(auth.username, auth.password):
            return Response(
                'Authentication required\n',
                401,
                {'WWW-Authenticate': 'Basic realm="TinyETL Test Server"'}
            )
        return f(*args, **kwargs)
    return decorated_function


def require_bearer_token(f):
    """Decorator to require Bearer Token Authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return Response(
                'Bearer token required\n',
                401,
                {'WWW-Authenticate': 'Bearer realm="TinyETL Test Server"'}
            )
        
        token = auth_header[7:]  # Remove 'Bearer ' prefix
        if not check_bearer_token(token):
            return Response('Invalid token\n', 403)
        
        return f(*args, **kwargs)
    return decorated_function


@app.route('/health')
def health():
    """Health check endpoint (no auth required)."""
    return jsonify({
        'status': 'healthy',
        'service': 'tinyetl-http-server',
        'endpoints': {
            '/public/data.csv': 'Public CSV (no auth)',
            '/basic-auth/users.csv': 'Basic Auth CSV',
            '/bearer-auth/products.csv': 'Bearer Token CSV',
            '/custom-headers/data.csv': 'CSV with custom header validation'
        }
    })


@app.route('/public/data.csv')
def public_data():
    """Public endpoint - no authentication required."""
    return Response(SAMPLE_CSV, mimetype='text/csv')


@app.route('/basic-auth/users.csv')
@require_basic_auth
def basic_auth_users():
    """Endpoint requiring HTTP Basic Authentication."""
    return Response(SAMPLE_CSV, mimetype='text/csv')


@app.route('/bearer-auth/products.csv')
@require_bearer_token
def bearer_auth_products():
    """Endpoint requiring Bearer Token Authentication."""
    return Response(PRODUCTS_CSV, mimetype='text/csv')


@app.route('/custom-headers/data.csv')
def custom_headers_data():
    """Endpoint that validates custom headers."""
    # Check for required custom header
    api_version = request.headers.get('X-API-Version')
    user_agent = request.headers.get('User-Agent')
    
    if api_version != 'v2':
        return Response(
            'X-API-Version header must be "v2"\n',
            400
        )
    
    # Return CSV with custom header info in response
    response = Response(SAMPLE_CSV, mimetype='text/csv')
    response.headers['X-Server-Version'] = '1.0'
    response.headers['X-Received-User-Agent'] = user_agent or 'none'
    return response


@app.route('/multi-auth/data.csv')
def multi_auth_data():
    """Endpoint that accepts either Basic Auth OR Bearer Token."""
    # Try Basic Auth first
    auth = request.authorization
    if auth and check_basic_auth(auth.username, auth.password):
        return Response(SAMPLE_CSV, mimetype='text/csv')
    
    # Try Bearer Token
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
        if check_bearer_token(token):
            return Response(PRODUCTS_CSV, mimetype='text/csv')
    
    # No valid auth provided
    return Response(
        'Authentication required (Basic Auth or Bearer Token)\n',
        401,
        {'WWW-Authenticate': 'Basic realm="TinyETL Test Server", Bearer realm="TinyETL Test Server"'}
    )


@app.route('/')
def index():
    """Root endpoint with API information."""
    return jsonify({
        'service': 'TinyETL HTTP Test Server',
        'version': '1.0',
        'authentication': {
            'basic_auth': {
                'username': BASIC_AUTH_USERNAME,
                'password': '***hidden***'
            },
            'bearer_token': '***hidden***'
        },
        'endpoints': {
            'GET /health': 'Health check (no auth)',
            'GET /public/data.csv': 'Public CSV data (no auth)',
            'GET /basic-auth/users.csv': 'CSV with Basic Auth required',
            'GET /bearer-auth/products.csv': 'CSV with Bearer Token required',
            'GET /custom-headers/data.csv': 'CSV with custom header validation',
            'GET /multi-auth/data.csv': 'CSV accepting either auth method'
        }
    })


if __name__ == '__main__':
    print("=" * 60)
    print("TinyETL HTTP Test Server Starting...")
    print("=" * 60)
    print(f"Basic Auth Credentials: {BASIC_AUTH_USERNAME} / {BASIC_AUTH_PASSWORD}")
    print(f"Bearer Token: {BEARER_TOKEN}")
    print("=" * 60)
    print("Endpoints:")
    print("  Public:      http://localhost:8680/public/data.csv")
    print("  Basic Auth:  http://localhost:8680/basic-auth/users.csv")
    print("  Bearer Auth: http://localhost:8680/bearer-auth/products.csv")
    print("  Custom Hdrs: http://localhost:8680/custom-headers/data.csv")
    print("  Multi Auth:  http://localhost:8680/multi-auth/data.csv")
    print("=" * 60)
    
    # Don't use debug mode in container to avoid restart issues
    app.run(host='0.0.0.0', port=8680, debug=False)
