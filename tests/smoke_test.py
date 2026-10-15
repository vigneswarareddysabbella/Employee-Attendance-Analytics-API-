"""
Live smoke test. NOT part of the submission - keep it outside the repo.

    pip install httpx
    uvicorn app.main:app --port 8000        # in another terminal, pointing at a scratch database
    python smoke_test.py

Uses random department names / employee codes in March 2031, so it is safe to re-run.
"""
import random
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
c = httpx.Client(base_url=BASE, timeout=30)
IST = timezone(timedelta(hours=5, minutes=30))
DEPT = f"Dept{random.randint(10**5, 10**6)}"
used = set()


def ms(d: date, h: int, m: int = 0, s: int = 0, ms_extra: int = 0) -> int:
    t = datetime(d.year, d.month, d.day, h, m, s, tzinfo=IST)
    return int(t.timestamp()) * 1000 + ms_extra


def emp(joined="2031-01-01", dept=None, start="09:30", end="18:30"):
    while True:
        code = f"EMP{random.randint(100000, 999999)}"
        if code not in used:
            used.add(code)
            break
    r = c.post("/employees", json={"emp_code": code, "name": code, "email": f"{code}@x.io",
                                   "department": dept or DEPT, "joined_on": joined,
                                   "shift_start": start, "shift_end": end})
    assert r.status_code == 201, r.text
    return code


def ok(cond, msg):
    print(("PASS " if cond else "FAIL ") + msg)
    if not cond:
        ok.failed += 1


ok.failed = 0

# first Monday of March 2031 and the Saturday after it
mon = next(date(2031, 3, d) for d in range(1, 8) if date(2031, 3, d).weekday() == 0)
sat = mon + timedelta(days=5)

# --- R2 boundaries, ms truncation -------------------------------------------------
a, b = emp(), emp()
r = c.post("/attendance/punch-in", json={"emp_code": a, "punched_at": ms(mon, 9, 40, 0, 900)}).json()
ok(r["late_minutes"] == 0 and r["punch_in"] % 1000 == 0, "09:40:00.900 -> 0 late, truncated to whole seconds")
r = c.post("/attendance/punch-in", json={"emp_code": b, "punched_at": ms(mon, 9, 40, 1)}).json()
ok(r["late_minutes"] == 10 and r["date"] == mon.isoformat(), "09:40:01 -> 10 late")

# --- races -----------------------------------------------------------------------
race = emp()
with ThreadPoolExecutor(12) as ex:
    codes = list(ex.map(lambda _: c.post("/attendance/punch-in", json={"emp_code": race, "punched_at": ms(mon, 9, 0)}).status_code, range(12)))
ok(codes.count(201) == 1 and codes.count(409) == 11, f"12 parallel punch-ins -> one 201 ({codes.count(201)}), rest 409")
with ThreadPoolExecutor(12) as ex:
    codes = list(ex.map(lambda _: c.post("/attendance/punch-out", json={"emp_code": race, "punched_at": ms(mon, 12, 30)}).status_code, range(12)))
ok(codes.count(200) == 1 and codes.count(409) == 11, "12 parallel punch-outs -> exactly one 200")
rec = c.get("/attendance", params={"emp_code": race}).json()["items"][0]
ok(rec["work_hours"] == 3.5 and rec["half_day"] is True, "3.5 h -> half day")
ok(c.post("/attendance/punch-out", json={"emp_code": a, "punched_at": ms(mon, 9, 40)}).status_code == 422, "punch-out <= punch-in -> 422")
ok(c.post("/attendance/punch-in", json={"emp_code": "EMP999999"}).status_code == 404, "unknown employee -> 404")

# --- overnight shift ---------------------------------------------------------------
night = emp(start="22:00", end="06:00")
r = c.post("/attendance/punch-in", json={"emp_code": night, "punched_at": ms(mon, 21, 55)}).json()
r2 = c.post("/attendance/punch-out", json={"emp_code": night, "punched_at": ms(mon + timedelta(days=1), 6, 40)}).json()
ok(r["date"] == mon.isoformat() and r2["overtime_minutes"] == 40 and r2["work_hours"] == 8.75, "overnight: date, overtime 40, 8.75 h")
n2 = emp(start="22:00", end="06:00")
r = c.post("/attendance/punch-in", json={"emp_code": n2, "punched_at": ms(mon + timedelta(days=1), 3, 0)}).json()
ok(r["date"] == mon.isoformat(), "03:00 punch on a night shift belongs to the previous day")

# --- regularize + concurrent history ---------------------------------------------
reg = emp()
c.post("/attendance/punch-in", json={"emp_code": reg, "punched_at": ms(mon, 10, 5)})
c.post("/attendance/punch-out", json={"emp_code": reg, "punched_at": ms(mon, 19, 10)})
r = c.patch(f"/attendance/{reg}/{mon}", json={"punch_in": ms(mon, 9, 28), "reason": "biometric glitch", "regularized_by": "hr"})
j = r.json()
ok(r.status_code == 200 and j["late_minutes"] == 0 and set(j["history"][0]["changes"]) == {"punch_in", "late_minutes", "work_hours"}, "PATCH recomputes and logs only real changes")
ok(isinstance(j["history"][0]["changes"]["punch_in"]["from"], int), "history punch_in values are epoch ms")
ok(c.patch(f"/attendance/{reg}/{mon}", json={"punch_in": ms(mon, 9, 28), "reason": "same again", "regularized_by": "hr"}).status_code == 422, "no-op PATCH -> 422")
with ThreadPoolExecutor(8) as ex:
    res = list(ex.map(lambda i: c.patch(f"/attendance/{reg}/{mon}", json={"punch_out": ms(mon, 18, i), "reason": "parallel edit", "regularized_by": f"u{i}"}).status_code, range(8)))
hist = c.get("/attendance", params={"emp_code": reg}).json()["items"][0]["history"]
ok(len(hist) == 1 + res.count(200), f"8 parallel PATCHes: history length {len(hist)} == 1 + successes {res.count(200)}")

# --- leaderboard ties, summary headcount, trend --------------------------------------
LB = f"LB{random.randint(10**5, 10**6)}"
lates = {}
for mins in (60, 30, 30, 15):                      # expected ranks 1, 2, 2, 4
    e = emp(dept=LB)
    c.post("/attendance/punch-in", json={"emp_code": e, "punched_at": ms(mon, 9 + (30 + mins) // 60, (30 + mins) % 60)})
    lates[e] = mins
lb = c.get("/analytics/leaderboard/late", params={"month": "2031-03", "department": LB, "limit": 2}).json()["items"]
ok([x["rank"] for x in lb] == [1, 2, 2], f"limit=2 with a tie at rank 2 returns 3 rows, ranks {[x['rank'] for x in lb]}")
ok(lb[1]["emp_code"] < lb[2]["emp_code"], "tied rows ordered by emp_code")
lb = c.get("/analytics/leaderboard/late", params={"month": "2031-03", "department": LB, "limit": 10}).json()["items"]
ok([x["rank"] for x in lb] == [1, 2, 2, 4], "ranks 1,2,2,4 (competition ranking)")

SUM = f"Sum{random.randint(10**5, 10**6)}"
s1, s2, s3 = emp(dept=SUM), emp(dept=SUM), emp(dept=SUM, joined="2031-03-20")
emp(dept=SUM, joined="2031-04-01")                  # joins after the month: must not count
c.post("/attendance/punch-in", json={"emp_code": s1, "punched_at": ms(mon, 9, 30)})
c.post("/attendance/punch-out", json={"emp_code": s1, "punched_at": ms(mon, 18, 30)})
c.post("/attendance/punch-in", json={"emp_code": s2, "punched_at": ms(sat, 9, 30)})   # weekend record
sm = c.get("/analytics/departments/summary", params={"month": "2031-03", "department": SUM}).json()["items"]
ok(len(sm) == 1 and sm[0]["headcount"] == 3, f"headcount 3 (s2/s3 have no weekday logs, April joiner excluded): {sm}")
ok(sm[0]["present_days"] == 1 and sm[0]["avg_work_hours"] == 9.0, "weekend record excluded from present_days; avg over records with work_hours")
m = c.get(f"/analytics/employees/{s3}/monthly", params={"month": "2031-03"}).json()
ok(m["working_days"] == 8 and m["present_days"] == 0 and m["attendance_pct"] == 0.0, f"mid-month joiner (Thu Mar 20 -> Mar 31): working_days {m['working_days']} == 8")
m = c.get(f"/analytics/employees/{s2}/monthly", params={"month": "2031-03"}).json()
ok(m["present_days"] == 0 and m["late_count"] == 0, "weekend punch gives no present day")

tr = c.get(f"/analytics/departments/{SUM}/trend", params={"from": "2031-03-01", "to": "2031-03-31"}).json()["items"]
ok(len(tr) == 31, "trend: one row per calendar day")
wk = [x for x in tr if not x["is_working_day"]]
ok(all(x["attendance_rate"] is None for x in wk), "weekend rows have null rate")
row = next(x for x in tr if x["date"] == mon.isoformat())
ok(row["present_count"] == 1 and row["headcount"] == 2 and row["attendance_rate"] == 0.5, f"working-day rate 1/2: {row}")
ok(c.get("/analytics/departments/NoSuchDept/trend", params={"from": "2031-03-01", "to": "2031-03-05"}).status_code == 404, "unknown department -> 404")

# --- explain -----------------------------------------------------------------------
checks = [
    ("attendance_list", {}), ("attendance_list", {"status": "PRESENT"}),
    ("attendance_list", {"date_from": "2031-03-01", "date_to": "2031-03-31"}),
    ("employee_monthly", {"emp_code": a, "month": "2031-03"}),
    ("department_summary", {"month": "2031-03"}), ("department_summary", {"month": "2031-03", "department": SUM}),
    ("late_leaderboard", {"month": "2031-03"}),
    ("department_trend", {"department": SUM, "from": "2031-03-01", "to": "2031-03-31"}),
]
for ep, params in checks:
    txt = c.get(f"/admin/explain/{ep}", params=params).text
    ok("IXSCAN" in txt and "COLLSCAN" not in txt, f"explain {ep} {params}: IXSCAN and no COLLSCAN")

print(f"\n{ok.failed} failure(s)")
sys.exit(1 if ok.failed else 0)
