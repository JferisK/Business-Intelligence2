package de.jakob_kroemer;
import org.apache.spark.api.java.*;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class Main {
  public static void main(String[] args) {
	
	String logFile 				= "./NY-02_short.csv"; // Should be some file on your system
    SparkConf conf 				= new SparkConf().setMaster("local").setAppName("Simple Application");
    JavaSparkContext sc 		= new JavaSparkContext(conf);
    JavaRDD<String> logData 	= sc.textFile(logFile).cache();
    
    /**
    0 	= medallion
    1 	= hack_license
    2 	= vendor_id
    3 	= rate_code
    4 	= store_and_fwd_flag
    5 	= pickup_datetime
    6 	= dropoff_datetime
    7 	= passenger_count
    8 	= trip_time_in_secs
    9 	= trip_distance
    10 	= pickup_longitude
   	11 	= pickup_latitude
    12 	= dropoff_longitude
   	13 	= dropoff_latitude
   	14 	= medallion
    15 	= hack_license
    16 	= vendor_id
    17 	= pickup_datetime
    18 	= payment_type
    19 	= fare_amount
    20 	= surcharge
    21 	= mta_tax
    22	= tip_amount
    23 	= tolls_amount
    24 	= total_amount 
    */
    JavaRDD<String[]> splitData = logData.map(new Function<String, String[]>() {
        public String[] call(String s) { 
        	String[] array = s.split(","); 
        	return array;
        }
    });
    
    //Q3 Q3 = new Q3(logData);

    System.out.println("Test" + splitData.toString());
  }
}