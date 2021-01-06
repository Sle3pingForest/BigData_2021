import scala.io.Source;

class Parti1 {
  
  def quest1_readFile(path : String){ 
    //read file
    val bufferedSource = Source.fromFile(path)
    for ( line <- bufferedSource.getLines){
      println(line.toUpperCase)
    }
  }
  
}