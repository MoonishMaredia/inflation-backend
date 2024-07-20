import asyncio
import httpx
import time

API_URL = "http://0.0.0.0:10000/api/v1/timeseries"  # Update with your actual API endpoint

# Sample request payload. Update with the actual payload your API expects
sample_payload = {
    "chartType": 'time-series',
    "seriesType": 'Level',
    "yearStart": '2001',
    "monthStart": '06',
    "yearEnd": '2023',
    "monthEnd": '01',
    "seriesIds": ['CUUR0000SA0', 'CUUR0000SAF', 'CUUR0000SAH', 'CUUR0000SAM','CUUR0000SAA','CUUR0000SAR']
}

headers = {
    "Origin": "https://localhost:3000"
}

# async def send_request(client, payload):
#     response = await client.post(API_URL, json=payload, headers=headers)
#     return response

# async def pressure_test():
#     async with httpx.AsyncClient() as client:
#         tasks = [send_request(client, sample_payload) for _ in range(20)]
#         responses = await asyncio.gather(*tasks)
#         for response in responses:
#             print(f"Status code: {response.status_code}, Response: {response.json()}")

async def send_request(client, payload):
    start_time = time.time()
    response = await client.post(API_URL, json=payload, headers=headers)
    end_time = time.time()
    return end_time - start_time

async def pressure_test():
    async with httpx.AsyncClient() as client:
        tasks = [send_request(client, sample_payload) for _ in range(50)]
        response_times = await asyncio.gather(*tasks)
        average_response_time = sum(response_times) / len(response_times)
        print(f"Average response time: {average_response_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(pressure_test())
