"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded; real environment variables win)

Layout of this file
  1. configuration, collections, indexes
  2. time / rounding helpers and the business rules R1-R5
  3. request models and small validation helpers
  4. serialisers (Mongo document -> API JSON)
  5. query / pipeline builders (shared by the real endpoints and /admin/explain)
  6. endpoints
"""
import calendar
import json
import logging
import math
import os
import re
import threading
import time
from contextlib import asynccontextmanager
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()  # does not override variables that are already set in the environment
log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration, collections, indexes
# --------------------------------------------------------------------------- #
# tz_aware=True: PyMongo hands back aware UTC datetimes instead of naive ones, so
# naive/aware arithmetic mistakes cannot happen.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    tzinfo=timezone.utc,
    serverSelectionTimeoutMS=5000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
attendance = db["attendance_logs"]

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]  # R6
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

GRACE_SECONDS = 10 * 60  # R2
MAX_SKIP = 10**12  # beyond this a page is certainly empty; avoids BSON int overflow

# (keys, options). Names are explicit so create_index is idempotent.
EMPLOYEE_INDEXES = [
    ([("emp_code", 1)], {"name": "emp_code_unique", "unique": True}),
    ([("department", 1), ("emp_code", 1)], {"name": "dept_emp_code"}),
    ([("department", 1), ("joined_on", 1)], {"name": "dept_joined_on"}),
    ([("joined_on", 1)], {"name": "joined_on"}),
]
ATTENDANCE_INDEXES = [
    # natural key: one record per employee per day (race-proof punch-in), employee monthly, lookups
    ([("emp_code", 1), ("date", 1)], {"name": "emp_date_unique", "unique": True}),
    # list sorted date desc / emp_code asc, month and range scans for the analytics
    ([("date", -1), ("emp_code", 1)], {"name": "date_desc_emp"}),
    # list filtered by status without an in-memory sort
    ([("status", 1), ("date", -1), ("emp_code", 1)], {"name": "status_date_emp"}),
    # punch-out: latest record of an employee with punch_in <= t
    ([("emp_code", 1), ("punch_in", -1)], {"name": "emp_punch_in"}),
]
LEADERBOARD_HINT = {"date": -1, "emp_code": 1}


def ensure_indexes() -> None:
    for keys, opts in EMPLOYEE_INDEXES:
        employees.create_index(keys, **opts)
    for keys, opts in ATTENDANCE_INDEXES:
        attendance.create_index(keys, **opts)


def _index_retry_worker() -> None:
    for _ in range(60):
        time.sleep(2)
        try:
            ensure_indexes()
            return
        except PyMongoError:
            continue
    log.error("could not create indexes")


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError:
        log.exception("index creation failed at startup; retrying in background")
        threading.Thread(target=_index_retry_worker, daemon=True).start()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 2. Time, rounding and the business rules
# --------------------------------------------------------------------------- #
def now_utc() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)  # R1: whole seconds


def from_ms(ms: int) -> datetime:
    """Epoch milliseconds -> aware UTC datetime, truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def to_ms(dt: Optional[datetime]) -> Optional[int]:
    dt = aware(dt)
    return None if dt is None else (dt - EPOCH) // timedelta(milliseconds=1)


def hm_seconds(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 3600 + int(m) * 60


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return hm_seconds(shift_end) <= hm_seconds(shift_start)


def attendance_date(ts: datetime, shift_start: str, shift_end: str) -> Date:
    """R1: IST calendar date of the punch-in; overnight shifts belong to the day they started."""
    ist = ts.astimezone(IST)
    day = ist.date()
    seconds_of_day = ist.hour * 3600 + ist.minute * 60 + ist.second
    if is_overnight(shift_start, shift_end) and seconds_of_day < hm_seconds(shift_end):
        day -= timedelta(days=1)
    return day


def _at(day: Date, hhmm: str) -> datetime:
    h, m = map(int, hhmm.split(":"))
    return datetime(day.year, day.month, day.day, h, m, tzinfo=IST)


def shift_start_dt(day: Date, shift_start: str) -> datetime:
    return _at(day, shift_start)


def shift_end_dt(day: Date, shift_start: str, shift_end: str) -> datetime:
    end = _at(day, shift_end)
    return end + timedelta(days=1) if is_overnight(shift_start, shift_end) else end


def compute_late_minutes(punch_in: datetime, day: Date, shift_start: str) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then floor minutes since shift start."""
    secs = int((punch_in - shift_start_dt(day, shift_start)).total_seconds())
    return secs // 60 if secs > GRACE_SECONDS else 0


def compute_overtime_minutes(punch_out: datetime, day: Date, shift_start: str, shift_end: str) -> int:
    """R3: floor minutes after shift end, counted only if >= 30."""
    secs = int((punch_out - shift_end_dt(day, shift_start, shift_end)).total_seconds())
    minutes = secs // 60
    return minutes if minutes >= 30 else 0


def work_hours_hundredths(punch_in: datetime, punch_out: datetime) -> int:
    """R4 in integer arithmetic: round-half-up(seconds / 36) == hundredths of an hour. No float rounding."""
    secs = int((punch_out - punch_in).total_seconds())
    return (2 * secs + 36) // 72


def derive_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime], emp: dict, day: Date) -> dict:
    """The four API-computed fields for a record in its final state (R2-R5)."""
    if status not in PRESENCE or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    late = compute_late_minutes(punch_in, day, emp["shift_start"])
    if punch_out is None:
        return {"work_hours": None, "late_minutes": late, "overtime_minutes": 0, "half_day": False}
    hundredths = work_hours_hundredths(punch_in, punch_out)
    return {
        "work_hours": hundredths / 100,
        "late_minutes": late,
        "overtime_minutes": compute_overtime_minutes(punch_out, day, emp["shift_start"], emp["shift_end"]),
        "half_day": hundredths < 450,  # R5, on the rounded value
    }


def half_up(value: Fraction, places: int) -> float:
    """R8 half-up rounding on an exact Fraction (Python's round() is banker's)."""
    scale = 10**places
    return math.floor(value * scale + Fraction(1, 2)) / scale


def count_working_days(first: Date, last: Date) -> int:
    """R7: Mon-Fri days in [first, last]."""
    n, d = 0, first
    while d <= last:
        n += d.weekday() < 5
        d += timedelta(days=1)
    return n


# --------------------------------------------------------------------------- #
# 3. Request models and validation helpers
# --------------------------------------------------------------------------- #
# strict=True rejects floats, strings and bools; the bounds reject "seconds-looking" values.
EpochMs = Annotated[int, Field(strict=True, ge=100_000_000_000, le=4_102_444_800_000)]

_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_MONTH_RE = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])")


def unprocessable(loc: list, msg: str) -> HTTPException:
    return HTTPException(422, detail=[{"loc": loc, "msg": msg, "type": "value_error"}])


def parse_date(value: str) -> Date:
    if not _DATE_RE.fullmatch(value):
        raise ValueError("date must be YYYY-MM-DD")
    return Date.fromisoformat(value)


def date_param(value: str, loc: list) -> Date:
    try:
        return parse_date(value)
    except ValueError:
        raise unprocessable(loc, "must be a valid date, YYYY-MM-DD")


def month_bounds(month: str) -> tuple[Date, Date]:
    if not _MONTH_RE.fullmatch(month) or int(month[:4]) < 1:
        raise unprocessable(["query", "month"], "must be YYYY-MM")
    y, m = int(month[:4]), int(month[5:])
    return Date(y, m, 1), Date(y, m, calendar.monthrange(y, m)[1])


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=r"^([01][0-9]|2[0-3]):[0-5][0-9]$")
    shift_end: str = Field(default="18:30", pattern=r"^([01][0-9]|2[0-3]):[0-5][0-9]$")
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, v: str) -> str:
        parse_date(v)
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


# --------------------------------------------------------------------------- #
# 4. Serialisers (never expose _id; instants become epoch ms)
# --------------------------------------------------------------------------- #
def employee_out(d: dict) -> dict:
    return {
        "emp_code": d["emp_code"],
        "name": d["name"],
        "email": d["email"],
        "department": d["department"],
        "shift_start": d["shift_start"],
        "shift_end": d["shift_end"],
        "joined_on": d["joined_on"],
        "created_at": to_ms(d.get("created_at")),
    }


def _history_value(key: str, v: Any) -> Any:
    return to_ms(v) if key in ("punch_in", "punch_out") else v


def history_out(h: dict) -> dict:
    changes = {
        k: {"from": _history_value(k, v.get("from")), "to": _history_value(k, v.get("to"))}
        for k, v in (h.get("changes") or {}).items()
    }
    return {"at": to_ms(h.get("at")), "by": h.get("by"), "reason": h.get("reason"), "changes": changes}


def attendance_out(d: dict) -> dict:
    return {
        "emp_code": d["emp_code"],
        "date": d["date"],
        "status": d["status"],
        "punch_in": to_ms(d.get("punch_in")),
        "punch_out": to_ms(d.get("punch_out")),
        "work_hours": d.get("work_hours"),
        "late_minutes": d.get("late_minutes") or 0,
        "overtime_minutes": d.get("overtime_minutes") or 0,
        "half_day": bool(d.get("half_day") or False),
        "history": [history_out(h) for h in (d.get("history") or [])],
    }


# --------------------------------------------------------------------------- #
# 5. Query and pipeline builders
#    The real endpoints and /admin/explain both call these, so the explained
#    query is by construction the query that runs.
# --------------------------------------------------------------------------- #
ATTENDANCE_SORT = {"date": -1, "emp_code": 1}


def build_attendance_find(emp_code, date_from, date_to, status, page, page_size):
    d_from = date_param(date_from, ["query", "date_from"]) if date_from else None
    d_to = date_param(date_to, ["query", "date_to"]) if date_to else None
    if d_from and d_to and d_from > d_to:
        raise unprocessable(["query", "date_from"], "date_from must be <= date_to")
    q: dict = {}
    if emp_code:
        q["emp_code"] = emp_code
    if d_from or d_to:
        q["date"] = {}
        if d_from:
            q["date"]["$gte"] = d_from.isoformat()
        if d_to:
            q["date"]["$lte"] = d_to.isoformat()
    if status:
        q["status"] = status
    return q, (page - 1) * page_size, page_size


# --- expression fragments over an attendance_logs document -------------------
_DOW = {"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}}  # Sunday = 1
IS_WEEKDAY = {"$and": [{"$gte": [_DOW, 2]}, {"$lte": [_DOW, 6]}]}
IS_PRESENT = {"$in": ["$status", PRESENCE]}
DAY_WEIGHT = {"$cond": [{"$eq": [{"$ifNull": ["$half_day", False]}, True]}, 0.5, 1]}  # R5
PRESENT_ANY_DAY = {"$cond": [IS_PRESENT, DAY_WEIGHT, 0]}
PRESENT_WEEKDAY = {"$cond": [{"$and": [IS_PRESENT, IS_WEEKDAY]}, DAY_WEIGHT, 0]}  # R7
LATE_FLAG = {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}
LATE_MINUTES = {"$ifNull": ["$late_minutes", 0]}
OVERTIME_MINUTES = {"$ifNull": ["$overtime_minutes", 0]}
IS_LEAVE = {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}
IS_ON_DUTY = {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]}
HAS_WORK_HOURS = {"$and": [IS_PRESENT, {"$isNumber": "$work_hours"}]}


def hu(expr: Any, places: int) -> dict:
    """Half-up rounding inside the pipeline. MongoDB's $round is banker's rounding, so it is not used.
    The 1e-9 absorbs binary-float noise so exact halves (e.g. 0.12345) round up. null stays null."""
    scale = 10**places
    return {"$divide": [{"$floor": {"$add": [{"$multiply": [expr, scale]}, 0.5 + 1e-9]}}, scale]}


def monthly_pipeline(emp_code: str, start: Date, end: Date) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "present_days": {"$sum": PRESENT_WEEKDAY},
                "leave_days": {"$sum": IS_LEAVE},
                "late_count": {"$sum": LATE_FLAG},
                "total_late_minutes": {"$sum": LATE_MINUTES},
                "total_overtime_minutes": {"$sum": OVERTIME_MINUTES},
            }
        },
    ]


def summary_pipeline(start: Date, end: Date, department: Optional[str]) -> tuple[list, dict]:
    """Starts from `employees` so people with zero logs still count (R9), then joins their month of logs."""
    match: dict = {"joined_on": {"$lte": end.isoformat()}}
    if department:
        match["department"] = department
    hint = {"department": 1, "joined_on": 1} if department else {"joined_on": 1}
    per_employee = [
        {"$match": {"date": {"$gte": start.isoformat(), "$lte": end.isoformat()}}},
        {
            "$group": {
                "_id": None,
                "present_days": {"$sum": PRESENT_WEEKDAY},
                "work_sum": {"$sum": {"$cond": [HAS_WORK_HOURS, "$work_hours", 0]}},
                "work_n": {"$sum": {"$cond": [HAS_WORK_HOURS, 1, 0]}},
                "late_count": {"$sum": LATE_FLAG},
                "late_minutes": {"$sum": LATE_MINUTES},
                "leave_count": {"$sum": IS_LEAVE},
                "on_duty_count": {"$sum": IS_ON_DUTY},
            }
        },
    ]
    zero = lambda f: {"$sum": {"$ifNull": [f"$s.{f}", 0]}}  # noqa: E731
    pipeline = [
        {"$match": match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": per_employee,
                "as": "logs",
            }
        },
        {"$project": {"department": 1, "s": {"$ifNull": [{"$arrayElemAt": ["$logs", 0]}, {"$literal": {}}]}}},
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": zero("present_days"),
                "work_sum": zero("work_sum"),
                "work_n": zero("work_n"),
                "late_count": zero("late_count"),
                "total_late_minutes": zero("late_minutes"),
                "leave_count": zero("leave_count"),
                "on_duty_count": zero("on_duty_count"),
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": 1,
                "present_days": 1,
                "avg_work_hours": {
                    "$cond": [{"$gt": ["$work_n", 0]}, hu({"$divide": ["$work_sum", "$work_n"]}, 2), None]
                },
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
    ]
    return pipeline, hint


def leaderboard_pipeline(start: Date, end: Date, limit: int, department: Optional[str]) -> list:
    pipeline: list = [
        {"$match": {"date": {"$gte": start.isoformat(), "$lte": end.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "e"}},
        {"$unwind": "$e"},  # logs of unknown employees are dropped
    ]
    if department:
        pipeline.append({"$match": {"e.department": department}})  # rank inside the department only
    pipeline += [
        # $rank = standard competition ranking (1, 2, 2, 4)
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies after ranking
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {
            "$project": {
                "_id": 0,
                "rank": 1,
                "emp_code": "$_id",
                "name": "$e.name",
                "department": "$e.department",
                "total_late_minutes": 1,
                "late_count": 1,
            }
        },
    ]
    return pipeline


def trend_pipeline(department: str, d_from: Date, d_to: Date, codes: list[str]) -> list:
    f, t = d_from.isoformat(), d_to.isoformat()
    return [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": f, "$lte": t}}},
        {"$project": {"_id": 0, "date": 1, "present": PRESENT_ANY_DAY, "late": LATE_FLAG}},
        # Two zero-valued anchor rows (first and last day) so $densify always spans the full range,
        # even when a department has no logs at all in it.
        {
            "$unionWith": {
                "coll": "employees",
                "pipeline": [
                    {"$match": {"department": department}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "_id": 0,
                            "date": {"$literal": [f, t]},
                            "present": {"$literal": 0},
                            "late": {"$literal": 0},
                        }
                    },
                    {"$unwind": "$date"},
                ],
            }
        },
        {"$group": {"_id": "$date", "present_count": {"$sum": "$present"}, "late_count": {"$sum": "$late"}}},
        {"$set": {"dt": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        {"$sort": {"dt": 1}},
        {"$densify": {"field": "dt", "range": {"step": 1, "unit": "day", "bounds": "full"}}},
        {
            "$set": {
                "date": {"$dateToString": {"date": "$dt", "format": "%Y-%m-%d"}},
                "present_count": {"$ifNull": ["$present_count", 0]},
                "late_count": {"$ifNull": ["$late_count", 0]},
                "dow": {"$dayOfWeek": "$dt"},
            }
        },
        {"$set": {"is_working_day": {"$and": [{"$gte": ["$dow", 2]}, {"$lte": ["$dow", 6]}]}}},
        {
            "$lookup": {
                "from": "employees",
                "let": {"d": "$date"},
                "pipeline": [
                    {"$match": {"department": department, "$expr": {"$lte": ["$joined_on", "$$d"]}}},
                    {"$count": "n"},
                ],
                "as": "hc",
            }
        },
        {"$set": {"headcount": {"$ifNull": [{"$arrayElemAt": ["$hc.n", 0]}, 0]}}},
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        hu({"$divide": ["$present_count", "$headcount"]}, 4),
                        None,
                    ]
                }
            }
        },
        # window = this row and up to 6 before it, inside the range only; $avg skips nulls
        {"$setWindowFields": {"sortBy": {"dt": 1}, "output": {"avg7": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}}}},
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": 1,
                "moving_avg_7d": hu("$avg7", 4),
            }
        },
    ]


def trend_inputs(department: str, from_: str, to: str) -> tuple[Date, Date, list[str]]:
    d_from = date_param(from_, ["query", "from"])
    d_to = date_param(to, ["query", "to"])
    if d_to < d_from:
        raise unprocessable(["query", "to"], "to must be >= from")
    if (d_to - d_from).days + 1 > 92:
        raise unprocessable(["query", "to"], "range must not exceed 92 days")
    codes = [e["emp_code"] for e in employees.find({"department": department}, {"_id": 0, "emp_code": 1})]
    if not codes:
        raise HTTPException(404, "unknown department")
    return d_from, d_to, codes


# --------------------------------------------------------------------------- #
# 6. Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "MongoDB is not reachable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_utc()
    try:
        employees.insert_one(doc)  # the unique index is the guarantee, not a prior find
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q = {"department": department} if department else {}
    skip = (page - 1) * page_size
    items = []
    if skip <= MAX_SKIP:
        items = list(employees.find(q, {"_id": 0}).sort("emp_code", 1).skip(skip).limit(page_size))
    total = employees.count_documents(q)
    return {"items": [employee_out(e) for e in items], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(404, "employee not found")
    ts = from_ms(body.punched_at) if body.punched_at is not None else now_utc()
    day = attendance_date(ts, emp["shift_start"], emp["shift_end"])
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derive_fields(body.status, ts, None, emp, day),
        "history": [],
    }
    try:
        attendance.insert_one(doc)  # unique (emp_code, date): exactly one concurrent request wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return attendance_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = employees.find_one({"emp_code": body.emp_code})
    if not emp:
        raise HTTPException(404, "employee not found")
    out = from_ms(body.punched_at) if body.punched_at is not None else now_utc()
    # latest record that started at or before the punch-out time; works across midnight
    rec = attendance.find_one({"emp_code": body.emp_code, "punch_in": {"$lte": out}}, sort=[("punch_in", -1)])
    if not rec:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    pin = aware(rec["punch_in"])
    if out <= pin:
        raise unprocessable(["body", "punched_at"], "punched_at must be after punch_in")
    if out - pin > timedelta(hours=24):
        raise unprocessable(["body", "punched_at"], "punched_at is more than 24 hours after punch_in")
    fields = derive_fields(rec["status"], pin, out, emp, Date.fromisoformat(rec["date"]))
    fields["punch_out"] = out
    # compare-and-set: only the request that still sees an open record can close it
    res = attendance.update_one({"_id": rec["_id"], "punch_out": None}, {"$set": fields})
    if res.matched_count == 0:
        raise HTTPException(409, "already punched out")
    rec.update(fields)
    return attendance_out(rec)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q, skip, limit = build_attendance_find(emp_code, date_from, date_to, status, page, page_size)
    items = []
    if skip <= MAX_SKIP:
        items = list(attendance.find(q, {"_id": 0}).sort(list(ATTENDANCE_SORT.items())).skip(skip).limit(limit))
    total = attendance.estimated_document_count() if not q else attendance.count_documents(q)
    return {"items": [attendance_out(d) for d in items], "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    day = date_param(date, ["path", "date"])
    emp = employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(404, "employee not found")
    rec = attendance.find_one({"emp_code": emp_code, "date": day.isoformat()})
    if not rec:
        raise HTTPException(404, "no record for that date")

    old_in, old_out = aware(rec.get("punch_in")), aware(rec.get("punch_out"))
    new_status = body.status or rec["status"]
    if new_status in ("ABSENT", "LEAVE"):
        if body.punch_in is not None or body.punch_out is not None:
            raise unprocessable(["body"], "ABSENT/LEAVE records cannot carry punch times")
        new_in = new_out = None
    else:
        new_in = from_ms(body.punch_in) if body.punch_in is not None else old_in
        new_out = from_ms(body.punch_out) if body.punch_out is not None else old_out
        if new_in is None:
            raise unprocessable(["body", "punch_in"], "a presence status requires a punch_in")
        if body.punch_in is not None and attendance_date(new_in, emp["shift_start"], emp["shift_end"]) != day:
            raise unprocessable(["body", "punch_in"], "punch_in must stay on the record's attendance date")
        if new_out is not None and (new_out <= new_in or new_out - new_in > timedelta(hours=24)):
            raise unprocessable(["body", "punch_out"], "punch_out must be after punch_in and within 24 hours")

    new_vals = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
                **derive_fields(new_status, new_in, new_out, emp, day)}
    old_vals = {
        "status": rec["status"], "punch_in": old_in, "punch_out": old_out,
        "work_hours": rec.get("work_hours"), "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0, "half_day": bool(rec.get("half_day") or False),
    }
    changes = {k: {"from": old_vals[k], "to": new_vals[k]} for k in old_vals if old_vals[k] != new_vals[k]}
    if not changes:
        raise unprocessable(["body"], "request changes nothing")

    entry = {"at": now_utc(), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    history = rec.get("history")
    # Compare-and-set on everything we read: if another PATCH (history length) or a punch-out
    # touched the record in between, nothing matches and the loser gets 409 - no lost history.
    cas = {
        "_id": rec["_id"],
        "status": rec["status"],
        "punch_in": rec.get("punch_in"),
        "punch_out": rec.get("punch_out"),
        "history": None if history is None else {"$size": len(history)},
    }
    if history is None:
        update = {"$set": {**new_vals, "history": [entry]}}
    else:
        update = {"$set": new_vals, "$push": {"history": entry}}
    updated = attendance.find_one_and_update(cas, update, return_document=ReturnDocument.AFTER)
    if updated is None:
        raise HTTPException(409, "record was modified concurrently; retry")
    return attendance_out(updated)


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(...)):
    start, end = month_bounds(month)
    emp = employees.find_one({"emp_code": emp_code})
    if not emp:
        raise HTTPException(404, "employee not found")
    row = next(attendance.aggregate(monthly_pipeline(emp_code, start, end)), None) or {}
    working = count_working_days(max(start, Date.fromisoformat(emp["joined_on"])), end)
    present = float(row.get("present_days", 0))
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working,
        "present_days": present,
        "leave_days": row.get("leave_days", 0),
        "late_count": row.get("late_count", 0),
        "total_late_minutes": row.get("total_late_minutes", 0),
        "total_overtime_minutes": row.get("total_overtime_minutes", 0),
        "attendance_pct": half_up(Fraction(present) * 100 / working, 2) if working else None,
    }


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(...), department: Optional[str] = None):
    start, end = month_bounds(month)
    pipeline, hint = summary_pipeline(start, end, department)
    return {"month": month, "items": list(employees.aggregate(pipeline, hint=hint))}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: str = Query(...), limit: int = Query(10, ge=1, le=50), department: Optional[str] = None):
    start, end = month_bounds(month)
    pipeline = leaderboard_pipeline(start, end, limit, department)
    return {"month": month, "items": list(attendance.aggregate(pipeline, hint=LEADERBOARD_HINT))}


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: str = Query(..., alias="from"), to: str = Query(...)):
    d_from, d_to, codes = trend_inputs(department, from_, to)
    return {"department": department, "items": list(attendance.aggregate(trend_pipeline(department, d_from, d_to, codes)))}


# ---- explain ---------------------------------------------------------------
def _explain(command: dict) -> dict:
    out = db.command({"explain": command, "verbosity": "executionStats"})
    return json.loads(json_util.dumps(out, json_options=json_util.RELAXED_JSON_OPTIONS))


def _explain_aggregate(coll: str, pipeline: list, hint: Optional[dict] = None) -> dict:
    cmd: dict = {"aggregate": coll, "pipeline": pipeline, "cursor": {}}
    if hint:
        cmd["hint"] = hint
    return _explain(cmd)


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = None,
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    def need(**named):
        for name, value in named.items():
            if not value:
                raise unprocessable(["query", name], "field required")

    if endpoint == "attendance_list":
        q, skip, lim = build_attendance_find(emp_code, date_from, date_to, status, page, page_size)
        explain = _explain({"find": "attendance_logs", "filter": q, "sort": ATTENDANCE_SORT,
                            "skip": min(skip, MAX_SKIP), "limit": lim})
        coll = "attendance_logs"
    elif endpoint == "employee_monthly":
        need(emp_code=emp_code, month=month)
        start, end = month_bounds(month)
        explain = _explain_aggregate("attendance_logs", monthly_pipeline(emp_code, start, end))
        coll = "attendance_logs"
    elif endpoint == "department_summary":
        need(month=month)
        start, end = month_bounds(month)
        pipeline, hint = summary_pipeline(start, end, department)
        explain = _explain_aggregate("employees", pipeline, hint)
        coll = "employees"
    elif endpoint == "late_leaderboard":
        need(month=month)
        start, end = month_bounds(month)
        explain = _explain_aggregate("attendance_logs", leaderboard_pipeline(start, end, limit, department), LEADERBOARD_HINT)
        coll = "attendance_logs"
    else:
        need(department=department, **{"from": from_, "to": to})
        d_from, d_to, codes = trend_inputs(department, from_, to)
        explain = _explain_aggregate("attendance_logs", trend_pipeline(department, d_from, d_to, codes))
        coll = "attendance_logs"
    return {"endpoint": endpoint, "collection": coll, "explain": explain}