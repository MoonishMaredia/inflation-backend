from fastapi import FastAPI
from fastapi import Request
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from models import TimeSeriesRequest, CompareRequest, UpdateRquest
from config import Settings

# adding Folder to the system path
import os
import sys
api_path = os.path.abspath('./API')
sys.path.append(api_path)
from fetch_data import get_max_date
from get_results import get_final_data
from update_data import update_db
import logging

app = FastAPI()
settings = Settings()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
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
    try:
        logger.info(f"Received time series request: {inputRequest}")
        results = get_final_data(request_type="time-series", input_args = inputRequest)
        return results
    except Exception as e:
        logger.error(f"Error processing time series request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/compare")
async def getCompareData(request: Request, inputRequest: CompareRequest):
    try:
        logger.info(f"Received time series request: {inputRequest}")
        results = get_final_data(request_type="compare", input_args = inputRequest)
        return results
    except Exception as e:
        logger.error(f"Error processing time series request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/updateDB")
async def updateInflationTables(request: Request, updateRequest: UpdateRquest):
    try:
        logger.info(f"Received time series request: {updateRequest}")
        add_date_string = updateRequest.addDate
        return_msg = update_db(add_date_string)
        return return_msg
    except Exception as e:
        logger.error(f"Error processing time series request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/maxDate")
async def getMaxDate():
    try:
        logger.info("Received max date request")
        max_date_string = get_max_date(db_name=settings.DATABASE_NAME, tablename="inflation_index")
        return max_date_string
    except Exception as e:
        logger.error(f"Error processing max date request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))