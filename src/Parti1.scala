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
import org.apache.spark.sql.SparkSession
import org.apache.spark.api.java.JavaRDD;

class Parti1  extends java.io.Serializable{

	val arrayTimeStamp = ArrayBuffer[String]()
			val arrayDuree = ArrayBuffer[Integer]()
			val arrayPcsource = ArrayBuffer[String]()
			val arrayPortSource = ArrayBuffer[String]()
			val arrayPcDest = ArrayBuffer[String]()
			val arrayPortDest = ArrayBuffer[String]()
			val arrayProtocol = ArrayBuffer[String]()
			val arrayPackageNumber = ArrayBuffer[Integer]()
			val arrayOctetNumber = ArrayBuffer[Integer]()

	val conf = new SparkConf().setAppName("Spark Scala WordCount Example").setMaster("local[1]") 
	val sc = new SparkContext(conf)

	def quest1_readFile(path : String){ 
		//read file
   	
		val bufferedSource = Source.fromFile(path)
				for ( line <- bufferedSource.getLines){
					val l = line.split(",")
					if(l(0) != "?") arrayTimeStamp += l(0) else arrayTimeStamp += "O";
					if(l(1) != "?") arrayDuree += l(1).toInt else arrayDuree += 0;
					if(l(1) != "?") arrayPcsource += l(2) else arrayPcsource += "0";
					if(l(3) != "?") arrayPortSource += l(3) else  arrayPortSource += "0";
					if(l(4) != "?") arrayPcDest += l(4) else arrayPcDest += "0";
					if(l(5) != "?") arrayPortDest += l(5) else arrayPortDest += "0";
					if(l(6) != "?") arrayProtocol +=  l(6) else arrayProtocol += "0";
					if(l(7) != "?") arrayPackageNumber += l(7).toInt else arrayPackageNumber += 0;
					if(l(8) != "?") arrayOctetNumber += l(8).toInt else arrayOctetNumber +=0;
				}

	}


	// convert array to set  = > no duplication of values so we have the frequency
	def quest2_occurences(){

			    val occ_PortSource = arrayPortSource.toSet
					val occ_PortDest = arrayPortDest.toSet
					val occ_Protocol = arrayProtocol.toSet

					println( "Le nombre d'occurrences port source: " + occ_PortSource.size);
					println( "Le nombre d'occurrences port Desttination: " + occ_PortDest.size);
					println( "Le nombre d'occurrences Protocol: " + occ_Protocol.size);
	
		  
		
	}


	//get sum, mean, min max  of type collum numeric
	def quest3_getCalcul(){

	  //recupere le temps du systeme, et ajouter 5 min
		var currentMinute = Calendar.getInstance().get(Calendar.MINUTE)
				var endTime = 0
				if(currentMinute >= 55  && currentMinute <= 59){
					endTime = 5 - (60- currentMinute)
				}
				else{
					endTime = currentMinute +  5
				}
			
			    var minDuree = arrayDuree.min
					var maxDuree = arrayDuree.max
					var minPaquet = arrayPackageNumber.min
					var maxPaquet = arrayPackageNumber.max
					var minOctet = arrayOctetNumber.min
					var maxOctet = arrayOctetNumber.max

					var sumDuree :Long = 0
					var sumPaquet = 0
					var sumOctet = 0
					for (i <-0 to arrayDuree.size - 1){
					  
    			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){
    						sumDuree += arrayDuree(i).toLong
    						sumPaquet += arrayPackageNumber(i)
    						sumOctet += arrayOctetNumber(i)
    
    					}
    			  else break
					}
		
			    var meanDuree = sumDuree/arrayDuree.size
					var meanPaquet = sumPaquet/arrayPackageNumber.size
					var meanOctet = sumOctet/arrayOctetNumber.size

					println( "DUREE DE CONNEXION*********************************************" )
					println( "Min: " + minDuree  + ", Max: " + maxDuree + ", Sum: " + sumDuree + ", Mean: "+ meanDuree )
					println( "NOMBRE DE PAQUET***********************************************" ) 
					println( "Min: " + minPaquet + ", Max: " + maxPaquet+ ", Sum: " + sumPaquet + ", Mean: "+ meanPaquet )
					println( "NOMBRE d'OCTET************************************************* ATTENTION ERROR WHEN COMPILE UNDER 32 BITE , THE SUM > 2 BILLIONS" ) 
					println( "Min: " + minOctet  + ", Max: " + maxOctet + ", Sum: " + sumOctet+ ", Mean: "+ meanOctet)
		
			
	}

	def quest5(path : String){
		var currentMinute = Calendar.getInstance().get(Calendar.MINUTE)
				var endTime = 0
				if(currentMinute >= 55  && currentMinute <= 59){
					endTime = 5 - (60- currentMinute)
				}
				else{
					endTime = currentMinute +  5
				} 
					var list = sc.textFile(path).map(line => line.split(",")).map(fields => (fields(2),(fields(1),fields(7),fields(8)))).groupByKey()

					var taille = list.count().toInt
					for ( line <- list.take(taille)){
					  
			  if(Calendar.getInstance().get(Calendar.MINUTE) <= endTime){

						var nbdeConnexion = 0 

						val arrayDuree = ListBuffer[Integer]()
						val arrayOctet= ListBuffer[Integer]()
						val arrayPaquet = ListBuffer[Integer]()
						var v =   line._2.toArray //Convert to array : CompactBuffer((6,4,2749), (6,6,2980), (6,6,2972)) 
						var totalDuree = 0
						var totalPaquet = 0
						var totalOctet = 0
						var meanDure = 0
						var meanPaquet =0
						var meanOctet = 0
						nbdeConnexion = v.size
						for (j <- 0 to v.size -1){
							arrayDuree += v(j)._1.toInt
									arrayOctet += v(j)._2.toInt
									arrayPaquet +=v(j)._3.toInt

									totalDuree += v(j)._1.toInt
									totalPaquet += v(j)._2.toInt
									totalOctet += v(j)._3.toInt
									meanDure = totalDuree/v.size
									meanPaquet = totalPaquet/v.size
									meanOctet =  totalOctet/v.size
						}	 
						println("***** Ordinateur source: " + line._1 +" *****")  
						println( "Nombre de Connection :"+ nbdeConnexion+ 
                     "| Duree Min: " + arrayDuree.min + 
						         ", Duree Max: " + arrayDuree.max + 
						         ", DureTotal: "+ totalDuree + 
						         ", DureMoyen: " + meanDure +
						         "| Octet Min: "+ arrayOctet.min + 
						         ", Octet Max: "+ arrayOctet.max + 
						         ", OctetMoyen:" + meanOctet+
						         ", OctetTotal: " +  totalOctet +
						         "| Paquet Min:" + arrayPaquet.min + 
						         ", Paquet Max: " + arrayPaquet.max + 
						         ", PaquetMoyen:"+ meanPaquet + 
						         ", PaquetTotal: "+ totalPaquet )	 
					}		
			  
			  else break
			  }
	}


	def quest6_7(path : String){
		val list = sc.textFile(path).map(line => line.split(",")).map(fields => (fields(2),(fields(3),fields(4),fields(5), fields(6)))).groupByKey()
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
						val transf = sc.parallelize(line._2.toSeq, 1) // parallelize pour conserver la notion RDD sinon le reduckey ne marche pas

								val countsPortSource = transf.flatMap(v => v._1.split(",")).map(word => (word,1))
								val countsDest = transf.flatMap(v => v._2.split(",")).map(word => (word,1))
								val countsPortDest = transf.flatMap(v => v._3.split(",")).map(word => (word,1))
								val countsProtocol = transf.flatMap(v => v._4.split(",")).map(word => (word,1))


								val freqDest = countsDest.reduceByKey(_ + _)
								val freqPS = countsPortSource.reduceByKey(_ + _)
								val freqPD = countsPortDest.reduceByKey(_ + _)
								val freqProtocol = countsProtocol.reduceByKey(_ + _) 


						val top10Dest = freqDest.map(_.swap).top(10);
						val top10PS = freqPS.map(_.swap).top(10);
						val top10PD = freqPD.map(_.swap).top(10);
						val top10Protocol = freqProtocol.map(_.swap).top(10);
						print("Ordinateur Source: " + line._1 )
						print("| Top 10 PortSource: ")
						for(line <- top10PS){ print(line) }
						print("| Top 10 Ordianteur Dest: ")
						for(line <- top10Dest){ print(line) }
						print("| Top 10 PortDest: ")
						for(line <- top10PD){ print(line) }
						print("| Top 10 Protocol: "  )
						for(line <- top10Protocol){ println(line) }
					}
			  else break

		}
	}

}