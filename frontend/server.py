import http.server
import socketserver
import os
import argparse

PORT = 8000

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=os.path.dirname(os.path.abspath(__file__)), **kwargs)

def main():
    parser = argparse.ArgumentParser(description="Food Tracker Frontend Server")
    parser.add_argument("--port", type=int, default=8000, help="Port to serve frontend (default: 8000)")
    args = parser.parse_args()

    with socketserver.TCPServer(("", args.port), Handler) as httpd:
        print(f"Frontend Status Board running at http://localhost:{args.port}")
        httpd.serve_forever()

if __name__ == "__main__":
    main()
