import os
import rpyc
import redis
import re
from rpyc.utils.server import ThreadedServer

redis_client = redis.Redis(host='redis', port=6379, db=0)

class WordCountService(rpyc.Service):
    def exposed_word_count(self, fileName, keyword):
        cache_key = f"{fileName}:{keyword}"
        cached_result = redis_client.get(cache_key)
        
        if cached_result:
            print(f"Cache hit for {cache_key}")
            return int(cached_result)
        
        print(f"Cache miss for {cache_key}")
        
        try:
            with open(f"/server/{fileName}.txt", "r") as file:
                text = file.read()
        except FileNotFoundError:
            return f"Text file {fileName} not found"

        pattern = r'\b' + re.escape(keyword) + r'\b'
        count = len(re.findall(pattern, text,re.IGNORECASE))
        
        redis_client.set(cache_key, count)
        
        return count
    
    def exposed_ping(self):
        return True 

if __name__ == "__main__":
    port = int(os.getenv('SERVER_PORT', 18812)) 
    server = ThreadedServer(WordCountService, port=port, hostname='0.0.0.0')
    print(f"Server started on port 18812")
    server.start()

