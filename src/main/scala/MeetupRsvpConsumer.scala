import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MeetupRsvpConsumer {
  def main(args: Array[String]): Unit = {

    // Setup Contexts
    val spark = SparkSession.builder()
      .master("local")
      .appName("MeetupRsvpConsumer")
      .getOrCreate()

    // Streaming Kafka subscriber
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "test")
      .load()

    //df.printSchema()
    // Schema of messages in kafka:
    /*
    root
    |-- key: binary (nullable = true)
    |-- value: binary (nullable = true)
    |-- topic: string (nullable = true)
    |-- partition: integer (nullable = true)
    |-- offset: long (nullable = true)
    |-- timestamp: timestamp (nullable = true)
    |-- timestampType: integer (nullable = true)
    */

    import spark.implicits._

    // Cast key and value attributes of kafka message from binary to string type:
    val data = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String,String)]

    // Schema for value attribute of kafka message:
    val schema=new StructType()
      .add("event",new StructType()
        .add("event_id",StringType)
        .add("event_name",StringType)
        .add("event_url",StringType)
        .add("time",LongType)
      )
      .add("group",new StructType()
        .add("group_city",StringType)
        .add("group_country",StringType)
        .add("group_id",LongType)
        .add("group_name",StringType)
        .add("group_lon",DoubleType)
        .add("group_lat",DoubleType)
        .add("group_state",StringType)
        .add("group_topics",ArrayType(new StructType()
          .add("topic_name",StringType)
          .add("urlkey",StringType)
        ))
        .add("group_urlname",StringType)
      )
      .add("guests",LongType)
      .add("member",new StructType()
        .add("member_id",LongType)
        .add("photo",StringType)
        .add("member_name",StringType)
      )
      .add("mtime", LongType)
      .add("response",StringType)
      .add("rsvp_id",StringType)
      .add("venue",new StructType()
        .add("venue_name",StringType)
        .add("lon",DoubleType)
        .add("lat",DoubleType)
        .add("venue_id",LongType)
      )
      .add("visibility",StringType)

    // parse value attribute of kafka's message according to above schema:
    val values = data
      .select(
        from_json(col("value"), schema).alias("parsed_value")
      )

    // flattening
    val flatDf = values
      .selectExpr("parsed_value.venue",
        "parsed_value.visibility",
        "parsed_value.response",
        "parsed_value.guests",
        "parsed_value.member",
        "parsed_value.rsvp_id",
        "parsed_value.mtime",
        "parsed_value.event",
        "parsed_value.group"
      )

    //flatDf.printSchema()
    //flatDf.show(10)

    val newDf = flatDf
      .select($"rsvp_id", from_unixtime($"mtime"/1000).cast(TimestampType).as("timestamp"))

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

    // Persist dataframe in HDFS:
    val query = flatDf
      .writeStream
      .format("parquet")
      //.outputMode("append")
      .option("checkpointLocation", "/tmp/meetupchk03")
      .option("path", "/tmp/data/MeetupRsvpDf")
      .start()

    query.awaitTermination()
    speedQuery.awaitTermination()

    // To run this file:
    // spark-submit --class MeetupRsvpConsumer --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --master local[*] ./meetuprsvpCons.jar
  }
}
