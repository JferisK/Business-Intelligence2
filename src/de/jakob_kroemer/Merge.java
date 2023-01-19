package de.jakob_kroemer;
import org.apache.spark.api.java.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Merge {
	public static JavaRDD<String> merge(String file1,String file2) {
		
	    SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    long data1count = sc.textFile(file1).count();
	    System.out.println(data1count);
	    long data2count = sc.textFile(file2).count();
	    System.out.println(data2count);
	    JavaRDD<String> data1 = sc.textFile(file1).mapPartitionsWithIndex((index, iterator) -> {
            if (index == 0 || index == data1count-1) {
                iterator.next();
            }
            return iterator;
        }, true).cache();
	    JavaRDD<String> data2 = sc.textFile(file2).mapPartitionsWithIndex((index, iterator) -> {
            if (index == 0 || index == data2count-1 ) {
                iterator.next();
            }
            return iterator;
        }, true).cache();
	    
	   
	    
	    //filter for incorrect lines
		JavaRDD<String> filteredData1 = data1.filter(new Function<String,Boolean>(){
			public Boolean call(String s) {
				return s.split(",").length == 14;
			}
		});
		
		JavaRDD<String> filteredData2 = data2.filter(new Function<String,Boolean>(){
			public Boolean call(String s) {
				return s.split(",").length == 11;
			}
		});
	    
	    PairFunction<String, String, String> keyData = new PairFunction<String,String, String>() { 
	    	public Tuple2<String, String> call(String s) { 
	    		String[]attributes = s.split(","); // key , value 
	    		//System.out.println(Arrays.toString(attributes));
	    		return new Tuple2(attributes[0]+":"+attributes[1]+":"+attributes[5],attributes[2]+","+attributes[3]+","+attributes[4]+","+attributes[5]+","
	    				+attributes[6]+","+attributes[7]+","+attributes[8]+","+attributes[9]+","+attributes[10]+","
	    		+attributes[11]+","+attributes[12]+","+attributes[13]); 
	    		} 
	    	};
	   
	    PairFunction<String, String, String> keyFare = new PairFunction<String,String, String>() { 
	    	public Tuple2<String, String> call(String s) { 
	    		String[]attributes = s.split(","); // key , value 
	    		return new Tuple2(attributes[0]+":"+attributes[1]+":"+attributes[3],attributes[2]+","+attributes[3]+","+attributes[4]+","+attributes[5]+","
	    		+attributes[6]+","+attributes[7]+","+attributes[8]+","+attributes[9]+","+
	    		attributes[10]);
	    	}
	    };
		  
		  
		  JavaPairRDD<String,String> pairs1 = filteredData1.mapToPair(keyData);
		  JavaPairRDD<String,String> pairs2 = filteredData2.mapToPair(keyFare);
		  
		  //medallion:hack_license:pickup_datetime,vendor_id,rate_code,store_and_fwd_flag,pickup_datetime,dropoff_datetime,passenger_count,
		  //trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,vendor_id,pickup_datetime,
		  //pickup_datetime,payment_type,fare_amount,surchange,mta_tax,tip_amount,tolls_amount,total_amount
		  JavaPairRDD<String,Tuple2<String, String>> result = pairs1.join(pairs2);
		  System.out.println(result.count()); 
		  
		  Function flatten = new Function<Tuple2<String,Tuple2<String,String>>,String>(){ 
			  public String call(Tuple2<String,Tuple2<String,String>> a) { 
				  return (a._1+","+a._2._1+","+a._2._2);} };
		  
		  
		  JavaRDD<String> output = result.map(flatten);
		 
	    
	    return output;
	  }
}

