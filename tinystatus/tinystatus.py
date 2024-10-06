import os
from dotenv import load_dotenv
import yaml
import asyncio
import aiohttp
import subprocess
import markdown
from jinja2 import Template
from datetime import datetime
import json
import logging

# Load environment variables
load_dotenv()

# Configuration
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', 30))
MAX_HISTORY_ENTRIES = int(os.getenv('MAX_HISTORY_ENTRIES', 100))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
CHECKS_FILE = os.getenv('CHECKS_FILE', 'checks.yaml')
INCIDENTS_FILE = os.getenv('INCIDENTS_FILE', 'incidents.md')
TEMPLATE_FILE = os.getenv('TEMPLATE_FILE', 'index.html.theme')
HISTORY_TEMPLATE_FILE = os.getenv('HISTORY_TEMPLATE_FILE', 'history.html.theme')
STATUS_HISTORY_FILE = os.getenv('STATUS_HISTORY_FILE', 'history.json')

# Service check functions
async def check_http(url, expected_code):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                return response.status == expected_code
        except:
            return False

async def check_ping(host):
    try:
        result = subprocess.run(['ping', '-c', '1', '-W', '2', host], capture_output=True, text=True)
        return result.returncode == 0
    except:
        return False

async def check_port(host, port): 
    try:
        _, writer = await asyncio.open_connection(host, port)
        writer.close()
        await writer.wait_closed()
        return True
    except:
        return False

async def run_checks(checks):
    results = []
    for check in checks:
        if check['type'] == 'http':
            status = await check_http(check['host'], check['expected_code'])
        elif check['type'] == 'ping':
            status = await check_ping(check['host'])
        elif check['type'] == 'port':
            status = await check_port(check['host'], check['port'])
        results.append({'name': check['name'], 'status': status})
    return results

# History management
def load_history():
    if os.path.exists(STATUS_HISTORY_FILE):
        with open(STATUS_HISTORY_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_history(history):
    with open(STATUS_HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)

def update_history(results):
    history = load_history()
    current_time = datetime.now().isoformat()
    for check in results:
        if check['name'] not in history:
            history[check['name']] = []
        history[check['name']].append({'timestamp': current_time, 'status': check['status']})
        history[check['name']] = history[check['name']][-MAX_HISTORY_ENTRIES:]
    save_history(history)

# Main monitoring loop
async def monitor_services():
    while True:
        try:
            with open(CHECKS_FILE, 'r') as f:
                checks = yaml.safe_load(f)

            with open(INCIDENTS_FILE, 'r') as f:
                incidents = markdown.markdown(f.read())

            with open(TEMPLATE_FILE, 'r') as f:
                template = Template(f.read())

            with open(HISTORY_TEMPLATE_FILE, 'r') as f:
                history_template = Template(f.read())

            results = await run_checks(checks)
            update_history(results)

            current_time = datetime.now()
            formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

            html = template.render(
                checks=results, 
                incidents=incidents, 
                last_updated=formatted_time
            )
            with open('index.html', 'w') as f:
                f.write(html)

            history_html = history_template.render(
                history=load_history(), 
                last_updated=formatted_time
            )
            with open('history.html', 'w') as f:
                f.write(history_html)

            logging.info(f"Status page and history updated at {formatted_time}")
            down_services = [check['name'] for check in results if not check['status']]
            if down_services:
                logging.warning(f"Services currently down: {', '.join(down_services)}")

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")

        await asyncio.sleep(CHECK_INTERVAL)

# Main function
def main():
    with open(CHECKS_FILE, 'r') as f:
        checks = yaml.safe_load(f)

    with open(INCIDENTS_FILE, 'r') as f:

        incidents = markdown.markdown(f.read())

    with open(TEMPLATE_FILE, 'r') as f:
        template = Template(f.read())

    results = asyncio.run(run_checks(checks))
    html = template.render(checks=results, incidents=incidents, last_updated=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    with open('index.html', 'w') as f:
        f.write(html)

if __name__ == "__main__":
    logging.basicConfig(level=getattr(logging, LOG_LEVEL), format='%(asctime)s - %(levelname)s - %(message)s')

    asyncio.run(monitor_services())  # Then start the monitoring loop
