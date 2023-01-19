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
		System.out.println("Brechne Q4");
	};
	
	
	
	  public void calcResult() {
		  

		
		String logFile = "/home/osboxes/data/NY_medium2_UTF8.csv"; // Should be some file on your system
	    SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> logData = sc.textFile(logFile).cache();

	    PairFunction<String, String, Double> pair = new PairFunction<String, String, Double>() {

	        public Tuple2<String, Double> call(String s) {

	                       String[] attributes = s.split(",");

	                       double seconds = Double.parseDouble(attributes[8]);

	                       double miles = Double.parseDouble(attributes[9]);

	                       double hours = seconds / 3600.0;
	                       
	                       if(hours==0) {
	                    	   return new Tuple2(s.split(",")[5].substring(11, 13), 0.0);
	                       }
	                       return new Tuple2(s.split(",")[5].substring(11, 13), (miles / hours));
	                       
	        }

	};
	    	
		JavaPairRDD<String, Double> pairs = logData.mapToPair(pair);
	    
		//group by hour
		
		JavaPairRDD<String, Iterable<Double>> hours = pairs.groupByKey();
		
		Function<Iterable<Double>,Double> average = new Function <Iterable<Double>, Double>(){
			public Double call(Iterable<Double> valuesForKey) {
				double speed=0;
				int counter=0;
				for(double i : valuesForKey) {
					speed += i;
					counter++;
				}
				double avg = speed/counter;
				return avg;
			}
		};
		
		JavaPairRDD<String, Double> avgSpeed = hours.mapValues(average);
			for(int i=0; i< avgSpeed.collect().size(); i++) {
				System.out.println(avgSpeed.collect().get(i)._1+"::"+avgSpeed.collect().get(i)._2);
			}
	  }
	}
