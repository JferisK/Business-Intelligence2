/**
 * 
 */
package de.jakob_kroemer;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.spark.api.java.function.DoubleFunction;

/**
 * @author jferisk
 *
 */
public class Q7 implements Serializable{
	/**
	Ermittle ob und wie das Trinkgeld von der Fahrdistanz abhängt.
	*/
	JavaRDD<String> logData;
	
	public Q7(JavaRDD<String> logData) {
		this.logData = logData;
		System.out.println("Berechne Q7");
	};
	
	public void calcResult() {	
		
		//create pairRDD with key: distance and value: totalTips
		PairFunction<String, Long, Double> pair = new PairFunction<String, Long, Double>() {
	        public Tuple2<Long, Double> call(String s) {
		        String[] attributes = s.split(",");
		        String distance = attributes[8];
		        if(distance.charAt(0)=='.') {
		        	distance = "0"+distance;
		        }
		        
		        long distanceRounded =  Math.round(Double.parseDouble(distance)/10)*10;
		       	return new Tuple2(distanceRounded, Double.parseDouble(attributes[attributes.length-3]));               
	        }
		};
		    	
	    JavaPairRDD<Long, Double> pairs = logData.mapToPair(pair);
	    
	    //count each values per key
	    JavaPairRDD<Long, Tuple2<Double, Integer>> valueCount = pairs.mapValues(value -> new Tuple2<Double,Integer>(value,1));
	
	    //add values by reduceByKey
	    JavaPairRDD<Long, Tuple2<Double, Integer>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) ->  new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
	    
	    //print count
	    for(int i =0; i < reducedCount.collect().size(); ++i) {
	    	System.out.println("Anzahl Daten:"+reducedCount.collect().get(i)._1 + " :: " +reducedCount.collect().get(i)._2._2); 
		}
	    
	    //calculate average
	    PairFunction<Tuple2<Long, Tuple2<Double, Integer>>,Long,Double> getAverageByKey = (tuple) -> {
	    	Tuple2<Double, Integer> val = tuple._2;
	    	double total = val._1;
	    	double count = val._2;
	    	Tuple2<Long, Double> avgTip = new Tuple2<Long, Double>(tuple._1, total / count);
	    	return avgTip;
	    };
	    
	    JavaPairRDD<Long, Double> avgTip = reducedCount.mapToPair(getAverageByKey);
	    
	    //print average
	    for(int i =0; i < avgTip.collect().size(); ++i) {
	    	System.out.println("Durchschnitt Trinkgeld: "+avgTip.collect().get(i)._1 + " :: " +avgTip.collect().get(i)._2); 
	    }
		System.out.print("Q7 Done!");
	}
	
}

