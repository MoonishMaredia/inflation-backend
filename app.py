from contextlib import closing

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from API.get_results import get_final_data, parse_month
from API.update_data import connect, update_db
from config import settings
from models import CompareRequest, TimeSeriesRequest, UpdateRquest

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"Hello": "Mundo"}


def results(kind, request):
    try:
        return get_final_data(kind, request)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.post("/api/v1/timeseries")
def time_series(request: TimeSeriesRequest):
    return results("time-series", request)


@app.post("/api/v1/compare")
def compare(request: CompareRequest):
    return results("compare", request)


@app.post("/api/v1/updateDB")
def refresh(request: UpdateRquest):
    try:
        return update_db(request.addDate)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.get("/api/v1/maxDate")
def max_date():
    with closing(connect(mode="ro")) as db:
        return db.execute("SELECT max(date) FROM inflation_index").fetchone()[0]


@app.get("/api/v1/weights")
def weights(month: str = ""):
    with closing(connect(mode="ro")) as db:
        if month:
            if len(month) != 7 or month[4] != "-":
                raise HTTPException(422, "Expected YYYY-MM")
            try:
                selected = parse_month(month[:4], month[5:])
            except ValueError as exc:
                raise HTTPException(422, str(exc)) from exc
        else:
            selected = db.execute("SELECT max(date) FROM inflation_index").fetchone()[0]
        values = dict(db.execute("SELECT series_id,weight FROM weights_table WHERE date=?", (selected,)))
    return {"date": selected, "weights": values}
