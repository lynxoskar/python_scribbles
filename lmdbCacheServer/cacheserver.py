import hashlib
import aiohttp
import asyncio
from aiohttp import web
import lmdb
import logging
import aiohttp
from aiohttp import web

async def handle(request):

    # Extract the full URL from the incoming request
    url = str(request.url)  # This gives the complete original URL, including path and query parameters
    method = request.method  # The HTTP method (GET, POST, PUT, etc.)
    headers = request.headers.copy()  # Forward headers
    body = await request.read()  # Read the body for POST, PUT, etc.

    
    
    
    logger.info(f"Received {method} request to {url} with headers {headers} and body {body} req {request}")

    try:
        # Create an async session
        async with aiohttp.ClientSession() as session:
            # Make the outgoing request using the same method and data as the incoming request
            async with session.request(method, url, headers=headers, data=body) as resp:
                
                # Read the response body from the target server
                ## check with lmdb if the call i allready cached

                response_body = await resp.read()

                # Create a hash key from the URL, method, and body
                hash_key = hashlib.sha256(f"{url}{method}{body}".encode()).hexdigest()

                # Open the LMDB environment and database
                
                db = env.open_db(b'responses')

                with env.begin(write=True) as txn:
                    # Check if the response is already cached
                    cached_response = txn.get(hash_key.encode())
                    if cached_response:
                        logger.info(f"Cache hit for {hash_key}")
                        # Use zero-copy sendfile
                        return web.Response(body= web.FileResponse(cached_response), status=resp.status, headers=resp.headers)
                       
                    
                    # Cache the new response
                    txn.put(hash_key.encode(), response_body)
                    logger.info(f"Cached response for {hash_key}")
                
                return web.Response(body=response_body, status=resp.status, headers=resp.headers)
            
    except Exception as e:
        # Log and return a 500 error if something goes wrong
        logger.error(f"Error proxying request to {url}: {e}")
        return web.Response(status=500, text=f"Error: {str(e)}")



async def init_app():
    app = web.Application()
    app.router.add_route('*', '/{url:.*}', handle)
    return app

def main():
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init_app())
    logger.info("Starting server on port 8080")
    web.run_app(app, port=8080)
    

if __name__ == '__main__':
    main()