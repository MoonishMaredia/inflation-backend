from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from models import TimeSeriesRequest, CompareRequest, UpdateRequest
from config import Settings
from dependencies import get_db

# adding Folder to the system path
import os
import sys
api_path = os.path.abspath('./API')
sys.path.append(api_path)
from get_results import get_final_data
from update_data import update_db
from fetch_data import get_max_date
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
    logger.info("Root endpoint accessed")
    return {"Hello": "Mundo"}

@app.post("/api/v1/timeseries")
async def getTimeSeriesData(request: Request, inputRequest: TimeSeriesRequest, db=Depends(get_db)):
    try:
        logger.info(f"Received time series request: {inputRequest}")
        results = await get_final_data(db, request_type="time-series", input_args=inputRequest)
        return results
    except Exception as e:
        logger.error(f"Error processing time series request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/compare")
async def getCompareData(request: Request, inputRequest: CompareRequest, db=Depends(get_db)):
    try:
        logger.info(f"Received compare request: {inputRequest}")
        results = await get_final_data(db, request_type="compare", input_args=inputRequest)
        return results
    except Exception as e:
        logger.error(f"Error processing compare request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/updateDB")
async def updateInflationTables(request: Request, updateRequest: UpdateRequest, db=Depends(get_db)):
    try:
        logger.info(f"Received update request: {updateRequest}")
        add_date_string = updateRequest.addDate
        return_msg = await update_db(db, add_date_string)
        return return_msg
    except Exception as e:
        logger.error(f"Error processing update request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/maxDate")
async def getMaxDate(db=Depends(get_db)):
    try:
        logger.info("Received max date request")
        max_date_string = await get_max_date(db, tablename="inflation_index")
        return max_date_string
    except Exception as e:
        logger.error(f"Error processing max date request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
