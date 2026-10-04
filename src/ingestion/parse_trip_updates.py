from google.transit import gtfs_realtime_pb2


FEED_PATH = "data/raw/gtfs_realtime/trip_updates/trip_updates.pb"


feed = gtfs_realtime_pb2.FeedMessage()

with open(FEED_PATH, "rb") as f:
    feed.ParseFromString(f.read())


print("Feed timestamp:", feed.header.timestamp)
print("Number of entities:", len(feed.entity))


for entity in feed.entity[:5]:

    print("=" * 60)

    if entity.HasField("trip_update"):

        trip_update = entity.trip_update

        print("Trip ID:", trip_update.trip.trip_id)
        print("Route ID:", trip_update.trip.route_id)

        for stop_update in trip_update.stop_time_update[:5]:

            print("Stop ID:", stop_update.stop_id)

            if stop_update.HasField("arrival"):
                print(
                    "Arrival delay:",
                    stop_update.arrival.delay,
                    "seconds"
                )

            if stop_update.HasField("departure"):
                print(
                    "Departure delay:",
                    stop_update.departure.delay,
                    "seconds"
                )