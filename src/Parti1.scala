import scala.io.Source;
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class Parti1 {

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
	
	  
	      var list = sc.textFile(path).map(line => line.split(",")).map(fields => (fields(0),fields(1),fields(2),fields(3),fields(4),fields(5),fields(6),fields(7),fields(8)))
	      println(list.count().toInt)
	      
	      println(list.take(50)(0))
	      println(list.take(1)(0))
	     for ( j <- 1 to list.count().toInt){
      			arrayTimeStamp += list.take(j)(0)._1
      			arrayDuree += list.take(j)(0)._2.toInt
      			arrayPcsource  += list.take(j)(0)._3
      			arrayPortSource += list.take(j)(0)._4
      			arrayPcDest  += list.take(j)(0)._5
      			arrayPortDest += list.take(j)(0)._6
      			arrayProtocol  += list.take(j)(0)._7
      			arrayPackageNumber  += list.take(j)(0)._8.toInt
      			arrayOctetNumber  += list.take(j)(0)._9.toInt
			
  			
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
	 	  
	 	  
	 	  
	 	  
	
}