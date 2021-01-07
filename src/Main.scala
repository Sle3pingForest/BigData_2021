

object Main {
  def main( args: Array[String]){
    val test = new Parti1();
    test.quest1_readFile("data/echantillon-flows-50.txt")
    test.quest2_occurences()
  }
}