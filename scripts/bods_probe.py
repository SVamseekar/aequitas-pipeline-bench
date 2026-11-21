"""Feasibility probe: stream BODS GTFS stop_times from the zip without extracting it.

Computes, for one reference weekday, per-stop departures, first/last departure and
evening (>=19:00) departures. Reports time, peak RSS and disk usage. Not a rebuild.
"""
import json, os, resource, shutil, sys, time, zipfile
from datetime import date, timedelta
import pandas as pd

zip_path = sys.argv[1]
limit_chunks = int(sys.argv[2]) if len(sys.argv) > 2 else 0
out = {}
t0 = time.time()
z = zipfile.ZipFile(zip_path)
out["zip_bytes"] = os.path.getsize(zip_path)
out["members"] = {i.filename: i.file_size for i in z.infolist()}

cal = pd.read_csv(z.open("calendar.txt"), dtype=str)
cd = pd.read_csv(z.open("calendar_dates.txt"), dtype=str)
# reference date: first Wednesday on or after the earliest start_date that is >= feed's min date
start = pd.to_datetime(cal["start_date"]).min().date()
ref = max(start, date.today())
while ref.weekday() != 2:
    ref += timedelta(days=1)
refs = ref.strftime("%Y%m%d")
active = cal[(cal.start_date <= refs) & (cal.end_date >= refs) & (cal.wednesday == "1")].service_id
svc = set(active)
add = cd[(cd.date == refs) & (cd.exception_type == "1")].service_id
rem = cd[(cd.date == refs) & (cd.exception_type == "2")].service_id
svc = (svc | set(add)) - set(rem)
out["ref_date"] = refs
out["n_services_active"] = len(svc)
t1 = time.time()
trips = pd.read_csv(z.open("trips.txt"), usecols=["trip_id", "service_id", "route_id"], dtype=str)
trips = trips[trips.service_id.isin(svc)]
active_trips = set(trips.trip_id)
out["n_trips_total_active"] = len(active_trips)
out["t_calendar_trips_s"] = round(time.time() - t1, 1)

agg = {}
rows = 0
t2 = time.time()
reader = pd.read_csv(z.open("stop_times.txt"), usecols=["trip_id", "departure_time", "stop_id"],
                     dtype=str, chunksize=2_000_000)
for i, ch in enumerate(reader):
    rows += len(ch)
    ch = ch[ch.trip_id.isin(active_trips) & ch.departure_time.notna()]
    hh = ch.departure_time.str.slice(0, 2).astype(int)
    mm = ch.departure_time.str.slice(3, 5).astype(int)
    mins = hh * 60 + mm
    g = pd.DataFrame({"stop_id": ch.stop_id, "m": mins, "ev": (mins >= 19 * 60).astype(int)}).groupby("stop_id").agg(
        n=("m", "size"), first=("m", "min"), last=("m", "max"), ev=("ev", "sum"))
    for sid, r in g.iterrows():
        a = agg.get(sid)
        if a is None:
            agg[sid] = [r.n, r["first"], r["last"], r.ev]
        else:
            a[0] += r.n; a[1] = min(a[1], r["first"]); a[2] = max(a[2], r["last"]); a[3] += r.ev
    if limit_chunks and i + 1 >= limit_chunks:
        break
out["stop_times_rows_read"] = rows
out["t_stop_times_s"] = round(time.time() - t2, 1)
res = pd.DataFrame.from_dict(agg, orient="index", columns=["departures", "first_min", "last_min", "evening"])
res.index.name = "stop_id"
res.to_parquet("bods_stop_departures.parquet")
out["n_stops_with_departures"] = len(res)
out["total_departures"] = int(res.departures.sum())
out["stops_evening_isolated"] = int((res["last_min"] < 19 * 60).sum())
out["output_parquet_bytes"] = os.path.getsize("bods_stop_departures.parquet")
out["peak_rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024 if sys.platform == "darwin" else 1024), 1)
out["disk_free_gb_end"] = round(shutil.disk_usage(".").free / 1e9, 1)
out["t_total_s"] = round(time.time() - t0, 1)
print(json.dumps(out, indent=1))
json.dump(out, open("bods_probe_result.json", "w"), indent=1)
