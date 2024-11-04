# Running with rust runtime 



class RApp:
    def __rsgi_init__(self, loop):
        
        loop.run_until_complete(some_async_init_task())

    async def __rsgi__(self, scope, protocol):
        