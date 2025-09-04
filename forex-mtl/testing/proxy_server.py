#!/usr/bin/env python3
"""
Logging Proxy Server for Forex-MTL Testing

Proxies all requests to a target server and logs:
- Timestamp
- Request details (method, path, headers, body)  
- Response code
- Latency

Provides GET /get_logs endpoint to retrieve logs as JSON.
"""

import json
import time
import os
from datetime import datetime
from typing import List, Dict, Any
from flask import Flask, request, Response, jsonify
import requests
from urllib.parse import urljoin, urlparse

app = Flask(__name__)

# Configuration
TARGET_URL = os.environ.get('TARGET_URL', 'http://localhost:8085')
PROXY_PORT = int(os.environ.get('PROXY_PORT', '8088'))

# Request logs storage
request_logs: List[Dict[str, Any]] = []

def log_request(method: str, path: str, headers: dict, body: str, 
                status_code: int, latency_ms: float, response_body: str = '') -> None:
    """Log request details to memory storage."""
    log_entry = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'method': method,
        'path': path,
        'headers': dict(headers),
        'body': body,
        'status_code': status_code,
        'latency_ms': round(latency_ms, 2),
        'response_body': response_body
    }
    request_logs.append(log_entry)
    print(f"[{log_entry['timestamp']}] {method} {path} -> {status_code} ({latency_ms:.2f}ms)")
    if response_body and status_code != 200:
        print(f"  Response: {response_body[:200]}...")

@app.route('/get_logs', methods=['GET'])
def get_logs():
    """Return all logged requests as JSON."""
    return jsonify({
        'total_requests': len(request_logs),
        'logs': request_logs
    })

@app.route('/clear_logs', methods=['POST'])
def clear_logs():
    """Clear all logs."""
    global request_logs
    count = len(request_logs)
    request_logs = []
    return jsonify({
        'message': f'Cleared {count} log entries'
    })

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS'])
def proxy(path):
    """Proxy all requests to target server and log metrics."""
    # Skip logging for internal endpoints
    if path in ['get_logs', 'clear_logs']:
        return jsonify({'error': 'Not found'}), 404
        
    start_time = time.time()
    
    # Build target URL
    target_url = urljoin(TARGET_URL.rstrip('/') + '/', path)
    if request.query_string:
        target_url += '?' + request.query_string.decode('utf-8')
    
    # Prepare request data
    headers = dict(request.headers)
    # Remove hop-by-hop headers
    headers.pop('Host', None)
    headers.pop('Content-Length', None)
    
    # Get request body
    body = ''
    if request.method in ['POST', 'PUT', 'PATCH']:
        if request.is_json:
            body = json.dumps(request.get_json())
        else:
            body = request.get_data(as_text=True)
    
    try:
        # Make proxied request
        response = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=request.get_data(),
            params=request.args,
            allow_redirects=False,
            timeout=30
        )
        
        # Calculate latency
        latency_ms = (time.time() - start_time) * 1000
        
        # Get response body for logging
        response_body = response.text if hasattr(response, 'text') else str(response.content)
        
        # Log the request
        log_request(
            method=request.method,
            path=request.full_path,
            headers=dict(request.headers),
            body=body,
            status_code=response.status_code,
            latency_ms=latency_ms,
            response_body=response_body
        )
        
        # Return response
        return Response(
            response.content,
            status=response.status_code,
            headers=dict(response.headers)
        )
        
    except requests.exceptions.RequestException as e:
        # Calculate latency even for errors
        latency_ms = (time.time() - start_time) * 1000
        
        # Log the failed request
        log_request(
            method=request.method,
            path=request.full_path,
            headers=dict(request.headers),
            body=body,
            status_code=502,  # Bad Gateway
            latency_ms=latency_ms,
            response_body=str(e)
        )
        
        return jsonify({
            'error': 'Proxy error',
            'message': str(e),
            'target_url': target_url
        }), 502

if __name__ == '__main__':
    print(f"Starting proxy server on port {PROXY_PORT}")
    print(f"Proxying requests to: {TARGET_URL}")
    print(f"Logs endpoint: http://localhost:{PROXY_PORT}/get_logs")
    
    app.run(
        host='0.0.0.0',
        port=PROXY_PORT,
        debug=False
    )