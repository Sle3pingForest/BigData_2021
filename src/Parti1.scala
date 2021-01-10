import scala.io.Source;
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import spire.random.Size
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ListBuffer

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


	def quest1_readFile(path : String){ 
		//read file

		val bufferedSource = Source.fromFile(path)
				for ( line <- bufferedSource.getLines){
					println(line.toUpperCase)

					val l = line.split(",")
					arrayTimeStamp += l(0)
					arrayDuree += l(1).toInt
					arrayPcsource += l(2)
					arrayPortSource += l(3)
					arrayPcDest += l(4)
					arrayPortDest += l(5)
					arrayProtocol +=  l(6)
					arrayPackageNumber += l(7).toInt
					arrayOctetNumber += l(8).toInt
				}

	}


	// convert array to set  = > no duplication of values so we have the frequency
	def quest2_occurences(){
		val occ_PortSource = arrayPortSource.toSet
				val occ_PortDest = arrayPortDest.toSet
				val occ_Protocol = arrayProtocol.toSet

				println( "Occurence porte Source: " + occ_PortSource.size  + ", Taille of array porte source:  " + arrayPortSource.size)
				println( "Occurence porte Dest: " + occ_PortDest.size  + ", Taille of array porte dest:  " + arrayPortDest.size)
				println( "Occurence porte Protocol: " + occ_Protocol.size  + ", Taille of array protocol:  " + arrayProtocol.size)
	}


	//get sum, mean, min max  of type collum numeric
	def quest3_getCalcul(){
		//here var = varible , NOT use VAL 
		var minDuree = arrayDuree.min
				var maxDuree = arrayDuree.max
				var minPaquet = arrayPackageNumber.min
				var maxPaquet = arrayPackageNumber.max
				var minOctet = arrayOctetNumber.min
				var maxOctet = arrayOctetNumber.max

				var sumDuree :Long = 1
				var sumPaquet = 0
				var sumOctet = 0
				for (i <-0 to arrayDuree.size - 1){
					sumDuree += arrayDuree(i).toLong
							sumPaquet += arrayPackageNumber(i)
							sumOctet += arrayOctetNumber(i)

				}

		var meanDuree = sumDuree/arrayDuree.size
				var meanPaquet = sumPaquet/arrayPackageNumber.size
				var meanOctet = sumOctet/arrayOctetNumber.size


				println( "Durre Min, Max ,Sum , Mean: " + minDuree  + ", " + maxDuree + ", " + sumDuree + ", "+ meanDuree )
				println( "Paquet Min, Max ,Sum , Mean: " + minPaquet + ", " + maxPaquet+ ", " + sumPaquet + ", "+ meanPaquet )
				println( "Octer Min, Max ,Sum , Mean: " + minOctet  + ", " + maxOctet + ", " + sumOctet+ ", "+ meanOctet)

	}

	def quest5(path : String){
		//var list = sc.textFile(path).map(line => line.split(",")).map(fields => (fields(0),fields(1),fields(2),fields(3),fields(4),fields(5),fields(6),fields(7),fields(8)))

		val conf = new SparkConf().setAppName("Spark Scala WordCount Example").setMaster("local[1]") 
				val sc = new SparkContext(conf) 
				var list = sc.textFile(path).map(line => line.split(",")).map(fields => (fields(2),(fields(1),fields(7),fields(8)))).groupByKey()

				var taille = list.count().toInt
				for ( line <- list.take(taille)){
					println(line)

					var nbdeConnexion = 0 

					val arrayDuree = ListBuffer[Integer]()
					val arrayOctet= ListBuffer[Integer]()
					val arrayPaquet = ListBuffer[Integer]()
					var v =   line._2.toArray //Convert to array
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


					println( "Ordinateur source: " + line._1  + ", Duree Max:" + arrayDuree.max + ", Duree Min:" + 
							arrayDuree.min + ", Octet Min: "+ arrayOctet.min + ", Octet Max: "+ arrayOctet.max + ", Paquet Min:" + 
							arrayPaquet.min + ", Octet Max: " + arrayPaquet.max + ", DureTotal: "+ totalDuree + ", PaquetTotal: "+ 
							totalPaquet +", OctetTotal: " +  totalOctet +", DureMoyen: " + meanDure +",PaquetMoyen:"+ meanPaquet + ", OctetMoyen:" + meanOctet)	 
				}
	}

}