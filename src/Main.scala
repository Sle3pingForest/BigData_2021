import org.apache.log4j.Logger
import org.apache.log4j.Level


object Main {
  //Disable info spark from consol
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def main( args: Array[String]){
    val test = new Parti1();
    //test.quest1_readFile("data/echantillon-flows-100000.txt")
    //test.quest2_occurences()
    //test.quest3_getCalcul()
    //test.quest5("data/echantillon-flows-100000.txt")
    //test.quest6_7("data/echantillon-flows-100000.txt")
    }
}