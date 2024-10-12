import hashlib
import asyncio
import aiohttp
import aiofiles
from asyncio import asyncfiles

import logging
from aiohttp import web
import os


#set up logging and lmdb db
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

concurrentCalls = {};
completedCalls = {};

# Get the LMDB path from environment variables or use a default path
filedir = os.getenv('CACHE_PATH', 'cache')

logger.info("Starting filecache environment in: {lmdb_path}")

# Ensure the  path is valid and directories are created
if not os.path.exists(filedir):
    os.makedirs(filedir)





def generate_key(url, method):
    #need to ensure that the key here is a valid filename   
    # Create a hash of the URL, method, and body to ensure the key is a valid filename
    key_string = f"{url}_{method}"
    return hashlib.md5(key_string.encode()).hexdigest()


async def get_from_cache_or_source(request):

    url = request.url
    method = request.method
    headers = request.headers

    cache_key = generate_key(url, method)
    
    ## happy path - check if the response is already in the cache
    if cache_key in completedCalls:
        
        headers = completedCalls[cache_key]['headers']
        path = completedCalls[cache_key]['path']
        logger.info(f"Cache hit for {cache_key}")
        
        return web.FileResponse(path, headers=headers)
          

    ## unhappy path - fetch from upstream but debounced, only one request will fetch from upstream
    if cache_key not in concurrentCalls:
        
        logger.info(f"Cache miss for {cache_key} - fetching from upstream")
        
        condition = asyncio.Condition()
        
        concurrentCalls[cache_key] = condition

        response = await fetch_from_upstream(request)

        logger.info(f"{cache_key} has been fetched from upstream")
        
        #minizing memfootprint by streaming the response to disk
        async with  aiofiles.open(f"{filedir}/{cache_key}", 'wb') as f:
            async for chunk in response.content.item_chunked(4  * 1024):
                await f.write(chunk)
        
        completedCalls[cache_key] = {'headers': response.headers, 'path': f"{filedir}/{cache_key}"}

        concurrentCalls[cache_key].notify_all()

        del concurrentCalls[cache_key]

        return web.FileResponse(path, headers=headers)

    # debouncing requests - only one request will fetch from upstream
    else:
        condition = concurrentCalls[cache_key]
        
        logger.info(f"Waiting for {cache_key} to be fetched from upstream")    
        
        async with condition:
            
            await condition.wait()
            
            logger.info(f"Done waiting for {cache_key} to be fetched from upstream") 
            
            headers = completedCalls[cache_key]['headers']
            path = completedCalls[cache_key]['path']
            logger.info(f"Cache hit for {cache_key}")
        
            return web.FileResponse(path, headers=headers)
            

async def fetch_from_upstream(request):

    url = request.url

    logger.info(f"Fetching {url} ")

    async with aiohttp.ClientSession() as session:

        if request.method == "GET":
            async with session.get(url, headers=request.header) as response:
                response.raise_for_status()
                return response
        
        elif request.method == "POST":
            async with session.post(url, body=request.body, headers=request.headers) as response:
                response.raise_for_status()
                return response
        
        elif request.method == "PUT":
            async with session.put(url, headers=request.headers) as response:
                response.raise_for_status()
                return response
        
        
        else:
            raise web.HTTPMethodNotAllowed(request.method, ["GET", "POST", "PUT"])





        
