/**
 * 
 */
package de.jakob_kroemer;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;

/**
 * @author osboxes
 *
 */
public class Q3 {

	JavaRDD<String> data;
	
	/**
	Gib mir Informationen über die Durchschnittsgeschwindigkeit des Verkehrs in New York
	*/
	public Q3(JavaRDD<String> data) {
		this.data = data;
	}
	
	public double getAvgSpeed() {
		double ret = 0;
		
		return ret;
	}

}
