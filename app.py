from fastapi import FastAPI
from fastapi import Request
import json
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from models import TimeSeriesRequest, CompareRequest

# adding Folder to the system path
import os
import sys
api_path = os.path.abspath('./API')
sys.path.append(api_path)
from get_results import get_final_data

app = FastAPI()

origins = [
    "http://localhost:3000"
    , "https://localhost:3000"
     
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    print("Hello")
    return {"Hello": "Mundo"}

@app.post("/api/v1/timeseries")
async def getTimeSeriesData(request: Request, inputRequest: TimeSeriesRequest):
    results = get_final_data(request_type="time-series", input_args = inputRequest)
    return results

@app.post("/api/v1/compare")
async def getCompareData(request: Request, inputRequest: CompareRequest):
    print(inputRequest)
    results = get_final_data(request_type="compare", input_args = inputRequest)
    return {"message":"Eventually this will have compare data"}