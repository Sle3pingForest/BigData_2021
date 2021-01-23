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
import java.lang.System

import java.util.Scanner;

class Parti3 {
  val conf = new SparkConf().setAppName("Spark Scala WordCount Example").setMaster("local[1]") 
	val sc = new SparkContext(conf) 
  
  def question1(path : String){
    
		val list = sc.textFile(path).map(line => line.split(","))
		val sd = list.map(fields => (fields(2),fields(4))).map(word => (word,1)).reduceByKey(_+_)
		val sps = list.map(fields => (fields(2),fields(3))).map(word => (word,1)).reduceByKey(_+_)
		val spd = list.map(fields => (fields(2),fields(5))).map(word => (word,1)).reduceByKey(_+_)
		val sp = list.map(fields => (fields(2),fields(6))).map(word => (word,1)).reduceByKey(_+_)
		val psd  = list.map(fields => (fields(3),fields(4))).map(word => (word,1)).reduceByKey(_+_)
		val pspd  = list.map(fields => (fields(3),fields(5))).map(word => (word,1)).reduceByKey(_+_)
		val psp  = list.map(fields => (fields(3),fields(6))).map(word => (word,1)).reduceByKey(_+_)
		val pdp  = list.map(fields => (fields(5),fields(6))).map(word => (word,1)).reduceByKey(_+_)
		val pdd  = list.map(fields => (fields(5),fields(4))).map(word => (word,1)).reduceByKey(_+_)
		
	  val scc = new Scanner(System.in);
		var rep= 0
		do{ 
		 System.out.println("1: Nombre occurences Pc source -> Pc dest");
		 System.out.println("2: Nombre occurences Pc source -> Port source");
		 System.out.println("3: Nombre occurences Pc source -> Port dest");
		 System.out.println("4: Nombre occurences Pc source -> Protocol");
		 System.out.println("5: Nombre occurences Port source -> Pc dest");
		 System.out.println("6: Nombre occurences Port source -> Port dest");
		 System.out.println("7: Nombre occurences Port source -> Protocol");
		 System.out.println("8: Nombre occurences Port dest -> Pc destt");
		 System.out.println("9: Nombre occurences Port dest -> Protocol");
		 System.out.println("Veuillez saisir un chiffre entre 1-9");
		 rep= scc.nextInt();
	 }
	 while(rep < 0 | rep >=10) ;
		var currentMinute = Calendar.getInstance().get(Calendar.MINUTE)
		var endTime = 0
		if(currentMinute >= 55  && currentMinute <= 59){
			endTime = 5 - (60- currentMinute)
		}
		else{
			endTime = currentMinute +  5
		}
		
		var taille = list.count().toInt
		if(rep == 1){
		for (line <- sd.take(taille)){
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Ordinateur Source: " +line._1._1  + " | Ordinateur Destination: " +line._1._2  + "| Nombre de Connexion: " + line._2)
			  }
			  else break
		}
		}
		if(rep == 2){
		for (line <- sps.take(taille)){
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Ordinateur Source: " +line._1._1  + " | Port Source: " +line._1._2  + "| Nombre de Connexion: " + line._2)
			  }
			  else break
		}
		}
	  if(rep == 3){
		for (line <- spd.take(taille)){
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Ordinateur Source: " +line._1._1  + " | Port Dest: " +line._1._2  + "| Nombre de Connexion: " + line._2)
			  }
			  else break
		}
		}
	  if(rep == 4){
		for (line <- sp.take(taille)){
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Ordinateur Source: " +line._1._1  + " | Protocol: " +line._1._2  + "| Nombre de Connexion: " + line._2)
			  }
			  else break
		}
		}
	  if(rep == 5){
		for (line <- psd.take(taille)){
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Port Source: " +line._1._1  + " | PC Dest: " +line._1._2  + "| Nombre de Connexion: " + line._2)
			  }
			  else break
		}
		}
	  if(rep == 6){
		for (line <- pspd.take(taille)){
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Port Source: " +line._1._1  + " | Port Dest: " +line._1._2  + "| Nombre de Connexion: " + line._2)
			  }
			  else break
		}
		}
	  if(rep == 7){
		for (line <- psp.take(taille)){
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Port Source: " +line._1._1  + " | Port Dest: " +line._1._2  + "| Nombre de Connexion: " + line._2)
			  }
			  else break
		}
		}
	  if(rep == 8){
		for (line <- pdd.take(taille)){
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Port Dest : " +line._1._1  + " | Pc Dest: " +line._1._2  + "| Nombre de Connexion: " + line._2)
			  }
			  else break
		}
		}
	  
	  if(rep == 9){
		for (line <- pdp.take(taille)){
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
		      println("Port Dest: " +line._1._1  + " | Protocol: " +line._1._2  + "| Nombre de Connexion: " + line._2)
			  }
			  else break
		}
		}
  }
}