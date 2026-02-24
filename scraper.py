import asyncio, logging
from time import time
from bs4 import BeautifulSoup
from collections import Counter
from httpx import AsyncClient


logger = logging.getLogger('scraper')





class Scraper:
    def __init__(self, 
                 timeout : int = 30,
                 proxies : list = None, 
                 requests_per_second : int = 5,
                 proxy_usage_limit : int = 10):
        """
        Initializes the core class.

        :param proxy: Optional proxy server for requests (string, e.g., 'http://proxy:port').
        :param requests_per_second: Maximum number of requests allowed per second.
        """
        
        self.session = AsyncClient(follow_redirects=True)
        self.timeout = timeout
        self.requests_per_second = requests_per_second
        self.proxy_usage_limit = proxy_usage_limit
        
        self.request_timestamps = []
        self.proxies = proxies or []
        self.current_proxy_index = 0
        
        self.requests_per_second = requests_per_second
        self.proxy_usage_counter = Counter()


    def _get_next_proxy(self):
        """
        Rotates to the next available proxy based on usage limits.

        :return: The next proxy URL or None if proxies are not set.
        """
        if not self.proxies:
            return None

        # Rotate proxies until a valid one is found
        while True:
            proxy = self.proxies[self.current_proxy_index]
            if self.proxy_usage_counter[proxy] < self.proxy_usage_limit:
                self.proxy_usage_counter[proxy] += 1
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
                return proxy
            else:
                self.proxy_usage_counter[proxy] = 0
                self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)

    async def _throttle(self):
        """
        Ensures that the number of requests does not exceed the allowed rate.
        """
        current_time = time()
        self.request_timestamps = [
            t for t in self.request_timestamps if current_time - t < 1
        ]

        if len(self.request_timestamps) >= self.requests_per_second:
            await asyncio.sleep(1 - (current_time - self.request_timestamps[0]))

    async def get(self, url, params = None, headers = None , **kwargs):
        """
        Sends a GET request while respecting the rate limit.

        :param url: The URL to request.
        :param params: Query parameters to include in the request.
        :param headers: Optional headers to include.
        :return: Response object from the request.
        """
        await self._throttle()
        self.request_timestamps.append(time())
        try:
            return  await self.session.get(url, params=params,  headers=headers, timeout=self.timeout, **kwargs)
        except Exception as e:
            logger.error(f"Error getting {url}: {e}")
            return None

    async def post(self, url, data=None, headers=None, **kwargs):
        """
        Sends a POST request while respecting the rate limit.

        :param url: The URL to request.
        :param data: Payload to include in the POST request.
        :param headers: Optional headers to include.
        :return: Response object from the request.
        """
        await self._throttle()
        self.request_timestamps.append(time())
        try:
            return  await self.session.post(url, data = data, headers=headers, timeout=self.timeout, **kwargs)
        except Exception as e:
            logger.error(f"Error posting {url}: {e}")
            return None

    async def get_soup(self, url : str, params = {} , headers = {}, **kwargs):
        response = await self.get(url, params , headers, **kwargs)
        return BeautifulSoup(response.content, 'html.parser') if response else None
    
    async def post_soup(self, url : str, data = {}, headers = {}, **kwargs):
        response = await self.post(url, data , headers, **kwargs)
        return BeautifulSoup(response.content, 'html.parser') if response else None
    
    async def get_json(self, url : str, params = {}, headers = {}, **kwargs):
        
        response = await self.get(url, params , headers, **kwargs)
        return response.json() if response else None
    
    async def post_json(self, url : str, data = {} , headers = {}, **kwargs):
        response  =  await self.post(url, data , headers, **kwargs)
        return response.json() if response else None

    async def close(self):
        """
        Closes the session.
        """
        await self.session.close()
    
    async def get_text(self, url: str, params = {}, headers = {}, **kwargs) -> str:
        response =  await self.get(url, params, headers, **kwargs)
        return response.text if response else None
