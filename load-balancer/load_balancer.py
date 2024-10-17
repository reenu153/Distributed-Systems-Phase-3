import asyncio
import websockets
import rpyc
import random
import os

SERVERS = [
    {"host": "wordcount_server_1", "port": 18812, "connections": 0, "healthy": True, "conn": None},
    {"host": "wordcount_server_2", "port": 18813, "connections": 0, "healthy": True, "conn": None},
    {"host": "wordcount_server_3", "port": 18814, "connections": 0, "healthy": True, "conn": None}
]

def setup_connections():
        for server in SERVERS:
            try:
                server["conn"] = rpyc.connect(server["host"], server["port"])
                server["healthy"] = True
                print(f"Connected to {server['host']}:{server['port']}")
            except Exception as e:
                server["healthy"] = False
                print(f"Failed to connect to {server['host']}:{server['port']} : {e}")

async def poll_server_health():
        while True:
            for server in SERVERS:
                if server["conn"]:
                    try:
                        server["conn"].root.ping()
                        server["healthy"] = True
                        print(f"Server {server['host']}:{server['port']} is healthy")
                    except Exception as e:
                        server["healthy"] = False
                        print(f"Server {server['host']}:{server['port']} is down - {e}")
            await asyncio.sleep(5)

round_robin_index = 0 

def select_server_round_robin():
        global round_robin_index
        healthy_servers = [server for server in SERVERS if server["healthy"]]

        if not healthy_servers:
            raise Exception("No healthy servers available")

        server = healthy_servers[round_robin_index % len(healthy_servers)]
        round_robin_index += 1
        return server

def select_server_least_connections():
        healthy_servers = [server for server in SERVERS if server["healthy"]]

        if not healthy_servers:
            raise Exception("No healthy servers available")

        return min(healthy_servers, key=lambda s: s["connections"])


async def distribute_load(fileName, keyword):
        load_balancing_algo = os.getenv('LOAD_BALANCING_ALGORITHM',"ROUND_ROBIN")
        if load_balancing_algo == "ROUND_ROBIN":
            server = select_server_round_robin()
        elif load_balancing_algo == "LEAST_CONNECTIONS":
            server = select_server_least_connections()

        try:
            print(f"Routing request to " + server["host"] + " listening on port " + str(server["port"]))
            server["connections"] += 1
            word_count = server["conn"].root.exposed_word_count(fileName, keyword)
            server["connections"] -= 1
            print(f"Connection to "+server["host"]+" on "+str(server["port"])+" closed")
            return word_count

        except Exception as e:
            server["connections"] -= 1
            print(f"Server {server['host']}:{server['port']} failed: {e}")
            server["healthy"] = False

        

async def handle_client(websocket, path):
        try:
            request = await websocket.recv()
            print(f"Received request: {request}")
            fileName, keyword = request.split(",")
            word_count= await distribute_load(fileName,keyword)
            await websocket.send(str(word_count))
            print(f"Sent result: {word_count}")  

        except Exception as e:
            print(f"Error: {e}")
            await websocket.send(f"Error: {str(e)}")

async def main():
        setup_connections()
        asyncio.create_task(poll_server_health())
        async with websockets.serve(handle_client, "load_balancer", 8765):
            print(f"Load balancer web socket server started.")
            await asyncio.Future() 

if __name__ == "__main__":
        asyncio.run(main())
