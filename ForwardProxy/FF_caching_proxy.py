import hashlib
import asyncio
from pathlib import Path
import time
import aiohttp
import aiofiles
import dataclasses

import logging
from aiohttp import web
import os
import tempfile
from urllib.parse import urlparse

import aiohttp_jinja2
import jinja2
from datetime import datetime


#set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



logger.info("Starting filecache environment in: {filedir}")
  
@dataclasses.dataclass
class GetCallResult: 
    headers: dict
    uri : str
    cachePath : str  
    number_of_requests: int = dataclasses.field(default_factory=lambda: 1)
    created_time: float = dataclasses.field(default_factory=lambda: asyncio.get_event_loop().time())   
    last_accessed_time: float = dataclasses.field(default_factory=lambda: asyncio.get_event_loop().time())

    def CacheHit(self):
        self.number_of_requests += 1
        self.last_accessed_time = asyncio.get_event_loop().time()
        logger.info(f"Cache hit n: {self.number_of_requests} for {self.uri}")
    
    def Stats(self):
        size_str = self.GetFileSize()
        minutes, seconds = divmod(int(asyncio.get_event_loop().time() - self.last_accessed_time), 60)    
        createdAt = datetime.fromtimestamp(self.created_time).strftime('%H:%M:%S')
        return {
            "uri": self.uri,
            "size": size_str,
            "number_of_requests": self.number_of_requests,
            "created_time": createdAt,
            "size" : self.GetFileSize(),
            "time_since_use": f"{minutes:02}:{seconds:02}"
            
        }

    def GetFileSize(self) -> str:
        file_size = os.path.getsize(self.cachePath)
        if file_size >= 1024 * 1024:
            size_str = f"{file_size / (1024 * 1024):.2f} MB"
        else:
            size_str = f"{file_size / 1024:.2f} KB"

        return size_str


@dataclasses.dataclass
class ConcurrentCall:
    uri: str
    number_of_requests: int
    wait_handle : asyncio.Condition

    def NewCall(self) -> asyncio.Condition:
        self.number_of_requests += 1
        return self.wait_handle

    def Stats(self):
        return {"uri": self.uri, "number_of_requests": self.number_of_requests}

concurrentCalls: dict[str, ConcurrentCall] = {}
storedData: dict[str, GetCallResult] = {}


CACHE_DIR = os.getenv('FFPROXY_CACHE_PATH', 'cache')
PORT = os.getenv('FFPROXY_PORT', 8080)
TIMEOUT = os.getenv('FFPROXY_TIMEOUT', 60)
LIFETIME = os.getenv('FFPROXY_LIFETIME', "CartaBlanca")

# Ensure the  path is valid and directories are created
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

if(LIFETIME == "CartaBlanca"):
    logger.info("Cache lifetime set to Carta Blanca")
    for filename in os.listdir(CACHE_DIR):
        file_path = os.path.join(CACHE_DIR, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                logger.info(f"Deleting: {file_path}")
                os.unlink(file_path)         
        except Exception as e:
            logger.error(f'Failed to delete {file_path}. Reason: {e}')

# else loadfromdisk

def generate_key(url, method) -> str:
    #need to ensure that the key here is a valid filename   
    # Create a hash of the URL, method, and body to ensure the key is a valid filename
    key_string = f"{url}_{method}"
    return hashlib.md5(key_string.encode()).hexdigest()

async def save_payload_to_cache(request : web.Request) -> web.Response:
    """
    Asynchronously saves the payload from a request to a cache and forwards the request to the specified URL.
    Args:
        request (web.Request): The incoming HTTP request.
    Returns:
        web.Response: The HTTP response from the forwarded request, with additional headers indicating cache status.
    Raises:
        ValueError: If the URL returned in the response body is invalid.
    Notes:
        - The request body is temporarily stored in a file before being forwarded.
        - The response body is expected to contain a URL, which is used to generate a cache key.
        - The cache key is used to store the payload in a cache directory.
        - Additional headers are added to the response to indicate whether the payload was cached.
    Dependencies:
        - aiofiles
        - aiohttp
        - tempfile
        - os
        - urllib.parse.urlparse
        - logging
    """
    
    url = request.url

    # Create a temporary file and write the request body to it
    with tempfile.NamedTemporaryFile(delete=False, delete_on_close=True) as temp_file : 
    
        logger.info(f"Storing copy of payload: {temp_file.name}")
        async with aiofiles.open(temp_file.name, 'wb') as temp_file:
            await temp_file.write(await request.read())
    
        # make the call
        async with aiohttp.ClientSession() as session:
            async with session.post(url, body=request.body, headers=request.headers) as response:
                response.raise_for_status()

                body = await response.read()

                if body is not None:               
                    try:
                        result_url = str(body, 'utf-8')
                        parsed_url = urlparse(result_url)
                        if not all([parsed_url.scheme, parsed_url.netloc]):
                            raise ValueError("Invalid URL")

                        cache_key = generate_key(parsed_url, "GET")

                        cache_path = os.path.join(CACHE_DIR, cache_key)
                        
                        os.rename(temp_file.name, cache_path)

                        storedData[cache_key] = GetCallResult(headers=response.headers, uri=url, cachePath=cache_path)

                        combinedHeaders = {**response.headers, **{"X-FFPROXY-Cache": "MISS"}}

                        return web.Response(status=response.status, headers=combinedHeaders)    

                    except Exception as e:
                        logger.info("No valide returned URL for post request {url}")
                
                
                logger.info(f"No result url returned from host. wont store payload")                  
                return web.Response(status=response.status, body=body, headers= response.headers | {'X-FFPROXY-Cache': "No cached entry. Missing get endpoint"})

async def main_dispatcher(request) -> aiohttp.ClientResponse:

    url = request.url
    method = request.method
    logger.info(f"Fetching {url} ")



    async with aiohttp.ClientSession() as session:

        if request.method == "GET":
            return await get_from_cache_or_source(request)
        
        elif request.method == "POST":
            return await save_payload_to_cache(request)
     

        elif request.method == "DELETE":
            async with session.delete(url, headers=request.headers) as response:
                return response

        elif request.method == "PATCH":
            async with session.patch(url, data=request.body, headers=request.headers) as response:
                return response

        elif request.method == "HEAD":
            async with session.head(url, headers=request.headers) as response:            
                return response

        elif request.method == "OPTIONS":
            async with session.options(url, headers=request.headers) as response:                   
                return response

        else:
            raise web.HTTPMethodNotAllowed(request.method, ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])

async def delete_entry(request) -> web.Response:
    """
    Deletes an entry from the cache based on the incoming request.
    Args:
        request (aiohttp.web.Request): The incoming HTTP request.
    Returns:
        aiohttp.web.Response: The response indicating the success of the deletion.
    """
    cache_key = request.match_info['cacheKey']
    # cache_key = generate_key(uri, "GET")
 
    if cache_key in storedData:
        os.remove(storedData[cache_key].cachePath)
        del storedData[cache_key]
        return web.Response(status=204)
    else:
        raise web.HTTPNotFound(reason="Entry not found in cache")
 
async def get_from_cache_or_source(request):

    url = request.url
    method = request.method
    headers = request.headers

    cache_key = generate_key(url, method)
    
    ## happy path - check if the response is already in the cache
    if cache_key in storedData:
        
        found = storedData[cache_key]
        found.CacheHit()
        
        return web.FileResponse(found.cachePath, headers=found.headers)
          

    ## unhappy path - fetch from upstream but debounced, only one request will fetch from upstream
    if cache_key not in concurrentCalls:
        
        logger.info(f"Cache miss for {cache_key} - fetching {url} from upstream")
        
        cuncurrentCall = ConcurrentCall(uri=url, number_of_requests=0, wait_handle=asyncio.Condition())
        
        concurrentCalls[cache_key] = cuncurrentCall.wait_handle


        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=request.headers) as response:
                response.raise_for_status()
                    
                logger.info(f"{cache_key} : {url} has been fetched from upstream storing to disk")

                #minizing memfootprint by streaming the response to disk
                async with aiofiles.open(f"{CACHE_DIR}/{cache_key}", 'wb') as f:
                    async for chunk in response.content.iter_chunked(4 * 1024):
                        await f.write(chunk)
        
        stored = GetCallResult(headers=response.headers, uri=url, cachePath=f"{CACHE_DIR}/{cache_key}")

        storedData[cache_key] = stored

        async with concurrentCalls[cache_key]:
            concurrentCalls[cache_key].notify_all()

        del concurrentCalls[cache_key]

        return web.FileResponse(stored.cachePath, headers=stored.headers)

    # debouncing requests - only one request will fetch from upstream
    else:
        signal = concurrentCalls[cache_key].NewCall()
        
        logger.info(f"Waiting for {cache_key} to be fetched from upstream from {url}")    
        
        async with signal:           
            await signal.wait()
            
            logger.info(f"Done waiting for {url} to be fetched from upstream") 
            
            stored = storedData[cache_key]

            stored.CacheHit()

            return web.FileResponse(stored.cachePath, headers=stored.headers)

@aiohttp_jinja2.template('stats.html')
async def get_stats(request) -> web.Response:
    """
    Retrieves statistics about the cache.
    Args:
        request (aiohttp.web.Request): The incoming HTTP request.
    Returns:
        aiohttp.web.Response: The response containing the cache statistics.
    """
    return {"stats": [entry.Stats() for entry in storedData.values()]}

    
@aiohttp_jinja2.template('index.html')
async def get_index(request) -> web.Response:
    return {"stats": [entry.Stats() for entry in storedData.values()]}



    




@web.middleware
async def logging_middleware(request, handler):
    
    start_time = asyncio.get_event_loop().time()

    logger.info(f"Incoming request: {request.method} {request.url} {request.headers}")

    response = await handler(request)
    
    end_time = asyncio.get_event_loop().time()
    duration = end_time - start_time

    logger.info(f"Request {request.method} {request.url} completed in {duration:.4f} seconds")
    
    return response

@web.middleware
async def error_handling_middleware(request, handler):
    try:
        response = await handler(request)
        return response
    except aiohttp.ClientError as e:
        logger.error(f"Upstream error: {e}")
        return web.Response(status=502, text="Bad Gateway: Upstream server error: {e}")
    except asyncio.TimeoutError:
        logger.error("Upstream request timed out")
        return web.Response(status=504, text="Gateway Timeout: Upstream server did not respond in time")
    except web.HTTPException as e:
        logger.error(f"HTTP error: {e}")
        return web.Response(status=e.status, text=e.reason)
    except FileNotFoundError:
        logger.error(f"File not found")
        return web.Response(status=404, text="Not Found")
    except web.HTTPNotFound:
        logger.error(f"Resource not found")
        return web.Response(status=404, text="Not Found")
    except IOError as e:
        logger.error(f"IO error: {e}")
        return web.Response(status=500, text="Internal IO exception")   
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return web.Response(status=500, text="Internal Server Error")


async def init_app() -> web.Application:
    app = web.Application(middlewares=[logging_middleware, error_handling_middleware])

    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('templates'))


    app.router.add_static('/static/', path=Path('static'), name='style.css')

    app.router.add_route('GET', '/cache', get_index)
    app.router.add_route('GET', '/cache/stats', get_stats)
    app.router.add_route('DELETE', '/cache/', delete_entry)

    app.router.add_route('*', '/{tail:.*}', main_dispatcher)

    return app

if __name__ == "__main__":   
    web.run_app(init_app(), host='0.0.0.0', port=PORT)



