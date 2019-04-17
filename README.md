Meetup Rsvp Big Data Pipeline

1. Get feed from Meetup about RSVPs and Publish to Kafka
2. Subscribe to Kafka and Append to HDFS
3. Read from HDFS and run analysis

```scala
val newDf = flatDf
    .select($"rsvp_id", 
        from_unixtime($"mtime"/1000)
            .cast(TimestampType).as("timestamp"))

val rsvpsPerMin = newDf
    .withWatermark("timestamp", "1 minutes")
    .groupBy(
    window($"timestamp", "1 minutes")
    ).count()

val speedQuery = rsvpsPerMin
    .writeStream
    .format("parquet")
    .option("checkpointLocation", "/tmp/meetupchk04")
    .option("path", "/tmp/data/MeetupRsvpStreamingQuery")
    .start()

speedQuery.awaitTermination()

```