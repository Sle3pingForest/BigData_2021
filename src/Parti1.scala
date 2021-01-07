import scala.io.Source;
import scala.collection.mutable.ArrayBuffer

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


			def quest1_readFile(path : String){ 
		//read file
  		val bufferedSource = Source.fromFile(path)
  				for ( line <- bufferedSource.getLines){
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
	 	  
	 	  
	 	  
	 	  
	 	  
	 	  
	 	  
	 	  
}