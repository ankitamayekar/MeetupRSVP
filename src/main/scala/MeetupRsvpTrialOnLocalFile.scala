import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MeetupRSVP {
  def main(args: Array[String]): Unit = {

    // Setup Contexts
    val spark = SparkSession.builder()
      .master("local")
      .appName("MeetupRSVP")
      .getOrCreate()

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    import spark.implicits._

    val df = spark.read.textFile("src/main/resources/rsvp.json")
    df.printSchema()
    //df.show()

    val schema=new StructType().add("event",new StructType()
      .add("event_id",StringType)
      .add("event_name",StringType)
      .add("event_url",StringType)
      .add("time",LongType))
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
          .add("urlkey",StringType)))
      .add("group_urlname",StringType))
      .add("guests",LongType)
      .add("member",new StructType()
        .add("member_id",LongType)
        .add("photo",StringType)
        .add("member_name",StringType))
      .add("mtime",LongType)
      .add("response",StringType)
      .add("rsvp_id",StringType)
      .add("venue",new StructType()
        .add("venue_name",StringType)
        .add("lon",DoubleType)
        .add("lat",DoubleType)
        .add("venue_id",LongType))
      .add("visibility",StringType)

    val values = df
      .select(from_json(col("value"), schema).alias("parsed_value"))

    val flatdf = values
      .selectExpr(
        "parsed_value.venue",
        "parsed_value.visibility",
        "parsed_value.response",
        "parsed_value.guests",
        "parsed_value.member",
        "parsed_value.rsvp_id",
        "parsed_value.mtime",
        "parsed_value.event",
        "parsed_value.group")

    //flatdf.show(10)
    /*

    ////////////////////////////////////   statewise list of number of events.

    val stateListOfEvent = flatdf.select($"event.event_id",$"group.group_state")
      .groupBy($"group_state")
      .agg(countDistinct($"event_id").alias("numOfEvents"))
      .filter($"group_state" =!= "null").orderBy(desc("numOfEvents"))
    stateListOfEvent.show(50)

*/
    //////////////////////////////////      most popular group topic & least poplular group topics

    val mostPopularEvent = flatdf.select($"rsvp_id",$"event.event_id",$"event.event_name",$"response")
      .filter($"response" === "yes").alias("YES")
      .groupBy($"event_id",$"event_name")
      .agg(countDistinct("rsvp_id").alias("numRsvp"))
      .orderBy(desc("numRsvp"))
    mostPopularEvent.show(10)

    ///////////////////////////////////////////////////////////////////////////////////////////////////
    val LeastPopularEvent = flatdf.select($"rsvp_id",$"event.event_id",$"event.event_name",$"response")
      .filter($"response" === "no").alias("No")
      .groupBy($"event_id",$"event_name")
      .agg(countDistinct("rsvp_id").alias("numRsvp"))
      .orderBy(desc("numRsvp"))
    LeastPopularEvent.show(10)

/*
    /////////////////////////////////// events with max guest invited
    val maxGuest = flatdf.select($"event.event_name",$"event.event_id",$"guests",$"group.group_city",$"group.group_state",$"group.group_country")
      .groupBy($"event_id",$"event_name",$"group_country")
      .agg(sum("guests" ).alias("TotalGuests")).orderBy(desc("TotalGuests"))
    maxGuest.show()

*/
    ///////////////////////// coordinates of map on venues of all the events
    val venuesdf=flatdf.select($"venue.venue_name",$"venue.lat",$"venue.lon").distinct()
    venuesdf.show()

    /*
    /////////////////////////// popular time
    //val populatTime = flatdf.select($"rsvp_id",$"mtime",$"group.group_city",$"group.group_state",$"group.group_country")
    //populatTime.show()
    */
  }
}