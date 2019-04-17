import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{countDistinct, desc, sum}

object MeetupRsvpBatchAnalysis {
  def main(args: Array[String]): Unit = {

    // Setup Contexts
    val spark = SparkSession.builder()
      .master("local")
      .appName("MeetupRsvpBatchAnalysis")
      .getOrCreate()

    // Read from HDFS:
    val df = spark.read.parquet("/tmp/data/MeetupRsvpDf")
    df.printSchema()
    df.show()

    import spark.implicits._

    /**********************   state-wise list of number of events ****************************/

    val stateListOfEvent = df
      .select($"event.event_id", $"group.group_state")
      .groupBy($"group_state")
      .agg(countDistinct($"event_id").alias("numOfEvents"))
      .filter($"group_state" =!= "null")
      .orderBy(desc("numOfEvents"))

    stateListOfEvent.show(50)

    stateListOfEvent
      .write
      .format("csv")
      .option("path", "/tmp/data/MeetupAnalysis/StatewiseListOfNumEvents.csv")
      .option("header", "true")
      .mode("overwrite")
      .save()

    // hdfs dfs -getmerge /tmp/data/MeetupAnalysis/StatewiseListOfNumEvents.csv/part-*.csv temp/StatewiseListOfNumEvents.csv
    // scp -P 2222 root@localhost:/root/temp/StatewiseListOfNumEvents.csv StatewiseListOfNumEvents.csv



    /******************************************************************************************/

    /********************** most popular event-topic *******************************************/

    val mostPopularEvent = df
      .select($"rsvp_id", $"event.event_id", $"event.event_name", $"response")
      .filter($"response" === "yes").alias("YES")
      .groupBy($"event_id", $"event_name")
      .agg(countDistinct("rsvp_id").alias("numRsvp"))
      .orderBy(desc("numRsvp"))

    mostPopularEvent.show(10)

    mostPopularEvent
      .write
      .format("csv")
      .option("path", "/tmp/data/MeetupAnalysis/mostPopularEvent.csv")
      .option("header", "true")
      .mode("overwrite")
      .save()

    // hdfs dfs -getmerge /tmp/data/MeetupAnalysis/mostPopularEvent.csv/part-*.csv temp/mostPopularEvent.csv
    // scp -P 2222 root@localhost:/root/temp/mostPopularEvent.csv mostPopularEvent.csv

    /******************************************************************************************/

    /********************** least popular event-topics **************************************/

    val LeastPopularEvent = df
      .select($"rsvp_id", $"event.event_id", $"event.event_name", $"response")
      .filter($"response" === "no").alias("No")
      .groupBy($"event_id",$"event_name")
      .agg(countDistinct("rsvp_id").alias("numRsvp"))
      .orderBy(desc("numRsvp"))

    LeastPopularEvent.show()

    LeastPopularEvent
      .write
      .format("csv")
      .option("path", "/tmp/data/MeetupAnalysis/LeastPopularEvent.csv")
      .option("header", "true")
      .mode("overwrite")
      .save()


    // hdfs dfs -getmerge /tmp/data/MeetupAnalysis/LeastPopularEvent.csv/part-*.csv temp/LeastPopularEvent.csv
    // scp -P 2222 root@localhost:/root/temp/LeastPopularEvent.csv LeastPopularEvent.csv


    /******************************************************************************************/

    /********************** events with max guest invited ************************************/

    val maxGuest = df
      .select($"event.event_name", $"event.event_id", $"guests", $"group.group_country")
      .groupBy($"event_id", $"event_name", $"group_country")
      .agg(sum("guests" ).alias("TotalGuests"))
      .orderBy(desc("TotalGuests"))

    maxGuest.show()

    maxGuest
      .write
      .format("csv")
      .option("path", "/tmp/data/MeetupAnalysis/MaxGuest.csv")
      .option("header", "true")
      .mode("overwrite")
      .save()

    // hdfs dfs -getmerge /tmp/data/MeetupAnalysis/MaxGuest.csv/part-*.csv temp/MaxGuest.csv
    // scp -P 2222 root@localhost:/root/temp/MaxGuest.csv MaxGuest.csv

    /******************************************************************************************/

    /********************** all the venues where event take place  ****************************/
    val venues=df
      .select($"venue.venue_name",$"venue.lat",$"venue.lon")
      .distinct()

    venues.show()

    venues
      .write
      .format("csv")
      .option("path", "/tmp/data/MeetupAnalysis/Venues.csv")
      .option("header", "true")
      .mode("overwrite")
      .save()


    // hdfs dfs -getmerge /tmp/data/MeetupAnalysis/Venues.csv/part-*.csv temp/Venues.csv
    // scp -P 2222 root@localhost:/root/temp/Venues.csv Venues.csv

    /******************************************************************************************/


    // To run this file:
    // spark-submit --class MeetupRsvpBatchAnalysis --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --master local[*] ./meetuprsvpCons.jar

    // Copy analysis-result files from HDFS to Sandbox -
    // hdfs dfs -getmerge /tmp/data/MeetupAnalysis/stateListOfEvent.csv/part-*.csv temp/stateListOfEvent.csv

    // Copy from Sandbox to VM -
    // Run command from VM -
    // scp -P 2222 root@localhost:/root/temp/SatewiseNumEvents.csv StatewiseNumEvents.csv

    // Open analysis-result csv files as tables in Hive -
    // CREATE TABLE temp_statewiseevents (state STRING, num_events INT) STORED AS TEXTFILE
    // LOAD DATA INPATH 'hdfs:///tmp/data/MeetupAnalysis/StatewiseListOfNumEvents.csv' OVERWRITE INTO TABLE temp_statewiseevents
    // SELECT * FROM temp_statewiseevents
  }
}
