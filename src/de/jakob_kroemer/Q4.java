/**
 * 
 */
package de.jakob_kroemer;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author osboxes
 *
 */
public class Q4 implements Serializable{
	
	JavaRDD<String> logData;
	
	public Q4(JavaRDD<String> logData) {
		this.logData = logData;
		System.out.println("Berechne Q4");
	};
	

	
	  public void calcResult() {
		  
			// filtering data where seconds==0
		    JavaRDD<String> filteredData = logData.filter(new Function<String, Boolean>() {
		        public Boolean call(String s) { 
		        	String[] attributes = s.split(",");
		        	double seconds = Double.parseDouble(attributes[7]);
		        	return seconds !=0 ; }
		      });

		  
		  // creating a pair from trip_start seconds as a key and trip_distance/trip_time (also known as speed :) ) as value
	    PairFunction<String, String, Double> pair = new PairFunction<String, String, Double>() {

	        public Tuple2<String, Double> call(String s) {

	                       String[] attributes = s.split(",");

	                       double seconds = Double.parseDouble(attributes[7]);

	                       double miles = Double.parseDouble(attributes[8]);

	                       double hours = seconds / 3600.0;
	                       
	                       return new Tuple2(s.split(",")[4].substring(11, 13), (miles / hours));
	                       
	        }

	};
	    	
    JavaPairRDD<String, Double> pairs = filteredData.mapToPair(pair);
    
    //count each values per key
    JavaPairRDD<String, Tuple2<Double, Integer>> valueCount = pairs.mapValues(value -> new Tuple2<Double,Integer>(value,1));

    //add values by reduceByKey
    JavaPairRDD<String, Tuple2<Double, Integer>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) ->  new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
    for(int i =0; i < reducedCount.collect().size(); ++i) {
	System.out.println(reducedCount.collect().get(i)._1 + " :: " +reducedCount.collect().get(i)._2._2); 
	}
    
    //calculate average
    PairFunction<Tuple2<String, Tuple2<Double, Integer>>,String,Double> getAverageByKey = (tuple) -> {
    	Tuple2<Double, Integer> val = tuple._2;
    	double total = val._1;
    	double count = val._2;
    	Tuple2<String, Double> avgTip = new Tuple2<String, Double>(tuple._1, total / count);
    	return avgTip;
    };
    JavaPairRDD<String, Double> avgTip = reducedCount.mapToPair(getAverageByKey);
    for(int i =0; i < avgTip.collect().size(); ++i) {
    	System.out.println(avgTip.collect().get(i)._1 + " :: " +avgTip.collect().get(i)._2); 
    	}
    System.out.print("Q4 Done!");
	  }
	}
