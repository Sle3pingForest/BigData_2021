import org.apache.log4j.Logger
import org.apache.log4j.Level


object Main {
  //Disable info spark from consol
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def main( args: Array[String]){
    /**
     * PARTI I
     */
    //val test = new Parti1();
    //question 1 2 3 sont dependant
    //test.quest1_readFile("data/echantillon-flows-100000.txt")
    //test.quest2_occurences()
    //test.quest3_getCalcul()
    
    //question 5 et (6-7) sont independant
    //test.quest5("data/echantillon-flows-100000.txt")
    
    //test.quest6_7("data/echantillon-flows-100000.txt")
    
    
    /**
     * PARTIE II
     */
    val parti2 = new Parti2();
    parti2.question1("data/echantillon-flows-50.txt")
    println("**********")
    parti2.question2("data/echantillon-flows-50.txt")
    }
}