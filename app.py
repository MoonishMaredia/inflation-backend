from fastapi import FastAPI
from fastapi import Request
import json
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from models import TimeSeriesRequest, CompareRequest

# importing sys
import sys

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
    print(inputRequest)
    return {"message":"Eventually this will have time series data"}

@app.post("/api/v1/compare")
async def getCompareData(request: Request, inputRequest: CompareRequest):
    print(inputRequest)
    return {"message":"Eventually this will have compare data"}