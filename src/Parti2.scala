import scala.io.Source;
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import spire.random.Size
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ListBuffer
import java.util.Calendar
import scala.util.control.Breaks._
import scala.math.Ordering

class Parti2 {
  
  val conf = new SparkConf().setAppName("Spark Scala WordCount Example").setMaster("local[1]") 
	val sc = new SparkContext(conf) 
  
  def question1(path : String){
    
		val list = sc.textFile(path).map(line => line.split(",")).map(fields => (fields(2),fields(4))).map(word => (word,1)).reduceByKey(_+_)
		var taille = list.count().toInt
		var currentMinute = Calendar.getInstance().get(Calendar.MINUTE)
		var endTime = 0
		if(currentMinute >= 55  && currentMinute <= 59){
			endTime = 5 - (60- currentMinute)
		}
		else{
			endTime = currentMinute +  5
		}
		for ( line <- list.take(taille)){
		  
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Ordinateur Source: " +line._1._1  + " | Ordinateur Destination: " +line._1._2  + "| Nombre de Connexion: " + line._2)
			  }
			  
			  else break
		
		}
  }
  
  
  def question2(path: String){
    
		val list = sc.textFile(path).map(line => line.split(",")).map(fields => (fields(2),fields(4), fields(6))).map(word => (word,1)).reduceByKey(_+_)
		
		val list2 = sc.textFile(path).map(line => line.split(",")).map(fields => (fields(2),fields(4), fields(3))).map(word => (word,1)).reduceByKey(_+_)
		
		val list3 = sc.textFile(path).map(line => line.split(",")).map(fields => (fields(2),fields(4), fields(5))).map(word => (word,1)).reduceByKey(_+_)
		
	  var currentMinute = Calendar.getInstance().get(Calendar.MINUTE)
		var endTime = 0
		if(currentMinute >= 55  && currentMinute <= 59){
			endTime = 5 - (60- currentMinute)
		}
		else{
			endTime = currentMinute +  5
		}
		
		var taille = list.count().toInt
			for ( line <- list.take(taille)){
			  
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Ordinateur Source: " +line._1._1  + " | Ordinateur Destination: " +line._1._2  + " | Nom de Protocol: " + line._1._3 +" | Count : " + line._2)
			  }else break;
		}
		endTime = endTime + 5;
		var taille2 = list2.count().toInt
			for ( line <- list2.take(taille)){
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		    println("Ordinateur Source: " +line._1._1  + " | Ordinateur Destination: " +line._1._2  + " | Port Source : " + line._1._3 +" | Count: " + line._2)
			  }
			  else break;
		}
		
		endTime = endTime + 5;
			var taille3 = list3.count().toInt
			for ( line <- list3.take(taille)){
			  
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		  println("Ordinateur Source: " +line._1._1  + " | Ordinateur Destination: " +line._1._2  + " | Port Destination : " + line._1._3 +" | Count: " + line._2)
			  }else break;
		}
  }
}



