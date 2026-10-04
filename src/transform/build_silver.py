import pandas as pd
import numpy as np
from pathlib import Path


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

BASE = Path("data")

REALTIME_PATH = BASE / "processed/trip_updates.csv"

TRIPS_PATH = BASE / "raw/gtfs_static/trips.txt"
ROUTES_PATH = BASE / "raw/gtfs_static/routes.txt"
STOPS_PATH = BASE / "raw/gtfs_static/stops.txt"
DIRECTIONS_PATH = BASE / "raw/gtfs_static/directions.txt"

OUTPUT_PATH = BASE / "processed/silver_trip_stop.csv"


# ---------------------------------------------------------
# Load data
# ---------------------------------------------------------

print("Loading datasets...")

realtime = pd.read_csv(REALTIME_PATH)

trips = pd.read_csv(
    TRIPS_PATH,
    dtype={"trip_id": str, "route_id": str}
)

routes = pd.read_csv(
    ROUTES_PATH,
    dtype={"route_id": str}
)

stops = pd.read_csv(
    STOPS_PATH,
    dtype={"stop_id": str}
)

directions = pd.read_csv(
    DIRECTIONS_PATH,
    dtype={
        "route_id": str,
        "direction_id": str
    }
)


# ---------------------------------------------------------
# Standardize IDs
# ---------------------------------------------------------

realtime["trip_id"] = realtime["trip_id"].astype(str)

realtime["stop_id"] = realtime["stop_id"].astype(str)

realtime["route_id"] = (
    realtime["route_id"]
    .astype("Int64")
    .astype(str)
    .replace("<NA>", np.nan)
)

realtime["direction_id"] = (
    realtime["direction_id"]
    .astype("Int64")
    .astype(str)
    .replace("<NA>", np.nan)
)


# ---------------------------------------------------------
# Join realtime → trips
# ---------------------------------------------------------

print("Joining trips...")

trip_columns = [
    "trip_id",
    "route_id",
    "service_id",
    "direction_id",
    "trip_headsign"
]

trip_lookup = trips[trip_columns].drop_duplicates("trip_id")

df = realtime.merge(
    trip_lookup,
    on="trip_id",
    how="left",
    suffixes=("_realtime", "_scheduled")
)


# ---------------------------------------------------------
# Resolve route_id
# ---------------------------------------------------------

df["route_id"] = (
    df["route_id_realtime"]
    .fillna(df["route_id_scheduled"])
)


# ---------------------------------------------------------
# Resolve direction_id
# ---------------------------------------------------------

df["direction_id"] = (
    df["direction_id_realtime"]
    .fillna(df["direction_id_scheduled"])
)


# ---------------------------------------------------------
# Join routes
# ---------------------------------------------------------

print("Joining routes...")

route_columns = [
    "route_id",
    "route_short_name",
    "route_long_name",
    "route_type"
]

df = df.merge(
    routes[route_columns],
    on="route_id",
    how="left"
)


# ---------------------------------------------------------
# Join stops
# ---------------------------------------------------------

print("Joining stops...")

stop_columns = [
    "stop_id",
    "stop_name",
    "stop_lat",
    "stop_lon",
    "zone_id"
]

df = df.merge(
    stops[stop_columns],
    on="stop_id",
    how="left"
)


# ---------------------------------------------------------
# Join direction names
# ---------------------------------------------------------

print("Joining directions...")

direction_lookup = directions[
    [
        "route_id",
        "direction_id",
        "direction"
    ]
].drop_duplicates(
    ["route_id", "direction_id"]
)

df = df.merge(
    direction_lookup,
    on=["route_id", "direction_id"],
    how="left"
)


# ---------------------------------------------------------
# Convert delay to minutes
# ---------------------------------------------------------

df["arrival_delay_minutes"] = (
    df["arrival_delay_seconds"] / 60
)

df["departure_delay_minutes"] = (
    df["departure_delay_seconds"] / 60
)


# ---------------------------------------------------------
# Business classifications
# ---------------------------------------------------------

df["delay_category"] = np.select(
    [
        df["arrival_delay_seconds"].isna(),
        df["arrival_delay_seconds"] < 0,
        df["arrival_delay_seconds"] < 300,
        df["arrival_delay_seconds"] < 600,
        df["arrival_delay_seconds"] >= 600
    ],
    [
        "Unknown",
        "Early",
        "On Time",
        "Delayed",
        "Severely Delayed"
    ],
    default="Unknown"
)


df["on_time_flag"] = (
    df["arrival_delay_seconds"].between(0, 299)
).astype(int)


df["late_flag"] = (
    df["arrival_delay_seconds"] >= 300
).astype(int)


df["severe_delay_flag"] = (
    df["arrival_delay_seconds"] >= 600
).astype(int)


df["early_flag"] = (
    df["arrival_delay_seconds"] < 0
).astype(int)


# ---------------------------------------------------------
# Data quality flags
# ---------------------------------------------------------

df["missing_delay_flag"] = (
    df["arrival_delay_seconds"].isna()
).astype(int)


df["extreme_delay_flag"] = (
    df["arrival_delay_seconds"].abs() > 3600
).astype(int)


# ---------------------------------------------------------
# Time dimensions
# ---------------------------------------------------------

df["observed_at"] = pd.to_datetime(
    df["observed_at"],
    utc=True
)

df["service_date"] = (
    df["observed_at"]
    .dt.tz_convert("America/Vancouver")
    .dt.date
)

df["observation_hour"] = (
    df["observed_at"]
    .dt.tz_convert("America/Vancouver")
    .dt.hour
)


# ---------------------------------------------------------
# Select final Silver columns
# ---------------------------------------------------------

silver_columns = [
    "service_date",
    "observed_at",

    "route_id",
    "route_short_name",
    "route_long_name",
    "route_type",

    "trip_id",
    "service_id",
    "direction_id",
    "direction",
    "trip_headsign",

    "stop_id",
    "stop_name",
    "stop_lat",
    "stop_lon",
    "zone_id",
    "stop_sequence",

    "arrival_delay_seconds",
    "departure_delay_seconds",

    "arrival_delay_minutes",
    "departure_delay_minutes",

    "delay_category",

    "on_time_flag",
    "late_flag",
    "severe_delay_flag",
    "early_flag",

    "missing_delay_flag",
    "extreme_delay_flag",

    "observation_hour"
]


silver = df[silver_columns].copy()


# ---------------------------------------------------------
# Save
# ---------------------------------------------------------

OUTPUT_PATH.parent.mkdir(
    parents=True,
    exist_ok=True
)

silver.to_csv(
    OUTPUT_PATH,
    index=False
)


# ---------------------------------------------------------
# Validation
# ---------------------------------------------------------

print()
print("=" * 60)
print("SILVER DATASET CREATED")
print("=" * 60)

print(f"Rows: {len(silver):,}")
print(f"Columns: {len(silver.columns)}")

print()
print("Missing values:")
print(
    silver.isna()
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

print()
print("Delay categories:")
print(
    silver["delay_category"]
    .value_counts()
)

print()
print("Routes:")
print(
    silver["route_short_name"]
    .nunique()
)

print()
print("Stops:")
print(
    silver["stop_id"]
    .nunique()
)

print()
print(f"Saved to: {OUTPUT_PATH}")