/**
 * 
 */
package de.jakob_kroemer;

/**
 * @author osboxes
 *
 */
import org.apache.spark.api.java.*;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.DoubleFunction;
import scala.Tuple2;
import org.apache.spark.api.java.function.FlatMapFunction;


public class Q2 implements Serializable{ 
	
	public Q2() {
		System.out.println("Brechne Q2");
	};
	
	
	public void calcResult() {
		
		
		String logFile = "/home/osboxes/data/NY_medium2_UTF8.csv"; 
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//JavaRDD<String> logData = sc.textFile(logFile).cache();
		JavaRDD<String> logData = sc.textFile(logFile);
		
		JavaRDD<Double> pickup_lat = logData.map(new Function<String, Double>() {
			
			public Double call(String s) {
				String[] attributes = s.split(",");
				String total = attributes[11];
				return Double.parseDouble(total);
				}
			});
				
	    long filterednorth = pickup_lat.filter(new Function <Double, Boolean>(){
	    	public Boolean call(Double i) 
	    	{return i>40.782005;}}).count(); // 40.782005 Mittelpunkt vom Centralpark
	    
	    long filteredsouth = pickup_lat.filter(new Function <Double, Boolean>(){
	    	public Boolean call(Double i) 
	    	{return i<=40.782005;}}).count();
	    
	    
	    System.out.println(filterednorth + " Fahrten starten im Norden");
	    System.out.println(filteredsouth + " Fahrten starten im Sueden");

		
		}
	}

