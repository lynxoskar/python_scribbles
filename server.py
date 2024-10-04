from tinystatus import main, monitor_services
import threading
import asyncio
import http.server
import socketserver
import logging

PORT = 8000

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Run the main function to generate initial status page


# Function to run monitor_services in a separate thread
def run_monitor():
    asyncio.run(monitor_services())

# Start the monitoring loop in a separate thread
monitor_thread = threading.Thread(target=run_monitor, daemon=True)
monitor_thread.start()

Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    logging.info(f"Serving at http://localhost:{PORT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        httpd.server_close()
        logging.info("Server closed")