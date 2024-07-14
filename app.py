from fastapi import FastAPI
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from models import TimeSeriesRequest, CompareRequest, UpdateRquest

# adding Folder to the system path
import os
import sys
api_path = os.path.abspath('./API')
sys.path.append(api_path)
from fetch_data import get_max_date
from get_results import get_final_data
from update_data import update_db

app = FastAPI()

origins = [
    "http://localhost:3000"
    , "https://localhost:3000"
    , "http://www.api.us-cpi.com"
    , "https://www.api.us-cpi.com"
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
    results = get_final_data(request_type="compare", input_args = inputRequest)
    return results

@app.post("/api/v1/updateDB")
async def updateInflationTables(request: Request, updateRequest: UpdateRquest):
    add_date_string = updateRequest.addDate
    return_msg = update_db(add_date_string)
    return return_msg

@app.get("/api/v1/maxDate")
async def getMaxDate():
    max_date_string = get_max_date(db_name="inflation_database.db", tablename="inflation_index")
    return max_date_string