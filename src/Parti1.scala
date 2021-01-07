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
    
    println(arrayTimeStamp.length)
    for( x <- arrayOctetNumber ){
      println(x)
    }
  }
}