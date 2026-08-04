import time
import random
import logging
from typing import Callable, Any, Optional

logger = logging.getLogger("aethelnet.osint.proxy")

class ProxyManager:
    """
    Aethelnet OSINT Proxy Manager.
    Provides resilient search requests by rotating User-Agents and
    applying intelligent exponential backoff and jitter to bypass
    Rate Limits (HTTP 429) from engines like DuckDuckGo and Brave Search.
    """
    
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
    ]
    
    def __init__(self, max_retries: int = 4, base_delay: float = 2.0):
        self.max_retries = max_retries
        self.base_delay = base_delay

    def get_random_headers(self) -> dict:
        """Returns HTTP headers with a rotating, randomized User-Agent."""
        return {
            "User-Agent": random.choice(self.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
    
    def execute_with_resilience(self, fetch_func: Callable[[dict], Any]) -> Optional[Any]:
        """
        Executes a network call (like `requests.get`) wrapped in a backoff strategy.
        Pass in a lambda or function that takes `headers` as an argument.
        
        Example:
            manager = ProxyManager()
            response = manager.execute_with_resilience(lambda h: requests.get("https://html.duckduckgo.com/html/", headers=h))
        """
        retries = 0
        while retries <= self.max_retries:
            headers = self.get_random_headers()
            try:
                # Attempt the fetch function
                response = fetch_func(headers)
                
                # If using requests, check if it's a 429
                if hasattr(response, 'status_code'):
                    if response.status_code == 429:
                        raise ValueError(f"Rate limited (429 Too Many Requests)")
                    if response.status_code == 403:
                        raise ValueError(f"Forbidden (403). Captcha or IP block.")
                        
                return response
                
            except Exception as e:
                retries += 1
                if retries > self.max_retries:
                    logger.error(f"ProxyManager: Max retries ({self.max_retries}) exhausted. Final error: {str(e)}")
                    return None
                    
                # Calculate backoff with jitter (e.g. 2s, 4s, 8s + random milliseconds)
                sleep_time = (self.base_delay ** retries) + random.uniform(0.1, 1.0)
                logger.warning(f"ProxyManager: Request failed ({str(e)}). Retrying in {sleep_time:.2f}s... (Attempt {retries}/{self.max_retries})")
                time.sleep(sleep_time)
