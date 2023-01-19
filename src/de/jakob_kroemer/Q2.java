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
	
	JavaRDD<String> logData;
	
	public Q2(JavaRDD<String> logData) {
		this.logData = logData;
		System.out.println("Berechne Q2");
	};
	
	
	public void calcResult() {
		JavaRDD<Double> pickup_lat = logData.map(new Function<String, Double>() {
			
			public Double call(String s) {
				String[] attributes = s.split(",");
				String total = attributes[10];
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

	    System.out.print("Q2 Done!");
		}
	}

