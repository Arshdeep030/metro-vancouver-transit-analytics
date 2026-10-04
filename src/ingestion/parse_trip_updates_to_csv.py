import pandas as pd
from google.transit import gtfs_realtime_pb2
from datetime import datetime, timezone


FEED_PATH = "data/raw/gtfs_realtime/trip_updates/trip_updates.pb"
OUTPUT_PATH = "data/processed/trip_updates.csv"


feed = gtfs_realtime_pb2.FeedMessage()

with open(FEED_PATH, "rb") as f:
    feed.ParseFromString(f.read())


feed_timestamp = feed.header.timestamp

records = []

for entity in feed.entity:

    if not entity.HasField("trip_update"):
        continue

    trip = entity.trip_update.trip

    route_id = trip.route_id if trip.route_id else None
    direction_id = trip.direction_id if trip.HasField("direction_id") else None

    for stop_update in entity.trip_update.stop_time_update:

        arrival_delay = None
        departure_delay = None

        if stop_update.HasField("arrival"):
            arrival_delay = stop_update.arrival.delay

        if stop_update.HasField("departure"):
            departure_delay = stop_update.departure.delay

        records.append({
            "feed_timestamp": feed_timestamp,
            "observed_at": datetime.fromtimestamp(
                feed_timestamp,
                tz=timezone.utc
            ),
            "trip_id": trip.trip_id,
            "route_id": route_id,
            "direction_id": direction_id,
            "stop_id": stop_update.stop_id,
            "stop_sequence": stop_update.stop_sequence,
            "arrival_delay_seconds": arrival_delay,
            "departure_delay_seconds": departure_delay
        })


df = pd.DataFrame(records)

df.to_csv(OUTPUT_PATH, index=False)

print(f"Created {len(df):,} records")
print(f"Saved to {OUTPUT_PATH}")
print()
print(df.head())