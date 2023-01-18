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
 * @author jferisk
 *
 */
public class Q6 implements Serializable{
	
	JavaRDD<String> logData;
	
	/**
	Ermittle ob und wie das Trinkgeld von der Uhrzeit abhängt.
	*/
	public Q6(JavaRDD<String> logData) {
		this.logData = logData;
		System.out.println("Brechne Q6");
	};
	
	public void calcResult() {	  
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
			System.out.println("Trinkgeld zwischen " 
								+ i 
								+ "-" 
								+ (i+1) 
								+ " Uhr: " 
								+ String.format("%.2f$", results.get(i).mean()));
		}
	}
	
}

