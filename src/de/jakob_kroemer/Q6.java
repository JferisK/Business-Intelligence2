/**
 * 
 */
package de.jakob_kroemer;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.DoubleFunction;

/**
 * @author osboxes
 *
 */
public class Q6 implements Serializable{
	
	/**
	Ermittle ob und wie das Trinkgeld von der Uhrzeit abhängt.
	*/
	public Q6() {
		System.out.println("Brechne Q6");
	};
	
	public void calcResult() {
		String logFile 				= "/home/osboxes/eclipse-workspace/Business-Intelligence2/src/NY-02_short.csv"; // Should be some file on your system
		SparkConf conf 				= new SparkConf().setMaster("local").setAppName("Simple Application");
		JavaSparkContext sc 		= new JavaSparkContext(conf);
		JavaRDD<String> logData 	= sc.textFile(logFile).cache();
	  
		JavaRDD<String> tipAmount = logData.map(new Function<String, String>() {
			public String call(String s) { 
				String[] attributes = s.split(",");
				String total = "\n"+"Zeit:" + attributes[5].substring(11) + "," +  attributes[22];
				return total;
			}
		});

		ArrayList<JavaDoubleRDD> results = new ArrayList<>();
		for (int i = 0; i < 24; i++) {
			String number = i < 10 ? "0" + i : i + "";
			
			JavaRDD<String> timeFilter = tipAmount.filter(new Function<String, Boolean>() {
				public Boolean call(String s) { 
					return s.contains("Zeit:" + number + ":") ; 
				}
			});
		 
			JavaRDD<Double> avgTip = timeFilter.map(new Function<String, Double>() {
				public Double call(String s) { 
					String[] attributes = s.split(",");
					String total = attributes[1];
					return Double.parseDouble(total);
				}
			});
		 
			JavaDoubleRDD result = avgTip.mapToDouble(new DoubleFunction<Double>() {
				public double call(Double x) {
					return (double) x;
				}
			});
			results.add(result);
		}
		
		for (int i = 0; i < 24; i++) {
			System.out.println("Trinkgeld zwischen " + i + "-" + (i+1) + " Uhr: " + results.get(i).mean());
		}
	}
	
}

