import argparse
import asyncio
import websockets

parser = argparse.ArgumentParser(description='Word Count Client')
parser.add_argument('keyword', type=str, help='Keyword to count in the text')
parser.add_argument('filename', type=str, help='Name of the text file')

args = parser.parse_args()

async def send_word_count_request():
    fileName= args.filename or "text1"
    keyword= args.keyword or "queen"
    try:
        uri = "ws://load_balancer:8765"
        async with websockets.connect(uri) as websocket:
            request = f"{fileName},{keyword}"
            await websocket.send(request)
            result = await websocket.recv()
            print(f"Word count result: {result}")
    except Exception as e:
        print(f"An error occurred: {e}")
    return 0
    
if __name__ == "__main__":
    asyncio.run(send_word_count_request())
