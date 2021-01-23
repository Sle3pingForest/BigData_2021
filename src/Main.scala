import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.util.Scanner;

object Main {
	//Disable info spark from consol
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)

	def main( args: Array[String]){

		val scf = new Scanner(System.in);
		var choixf = "";

		do{ 
			System.out.println("Entrer le chemin du fichier de donné meme niveau que le programme");
			choixf= scf.nextLine();
		}
		while( choixf == "")

			val sc = new Scanner(System.in);
		var choix = 0;

		do{ 
			System.out.println("Choix entre: Partie I = 1 | Partie II = 2 | Partie II et IV = 3 ");
			System.out.println("Veuillez saisir 1, 2 ou 3 : ");
			choix= sc.nextInt();
		}
		while(choix !=1 && choix !=2  && choix !=3) ;
		/**
		 * PARTI I
		 */
		if(choix == 1){
			val test = new Parti1(); 
			val scq = new Scanner(System.in);
			var choixq = 0;

			do{ 
				System.out.println("Choix des question:");
				System.out.println("Veuillez saisir 1  = Quest 1_2_3 | 2 = Quest 5  |   3 = Quest 6_7");
				choixq= sc.nextInt();
			}
			while(choixq !=1 && choixq !=2  && choixq !=3) ;
			if(choixq == 1){
				test.quest1_readFile(choixf.toString())
				test.quest2_occurences()
				test.quest3_getCalcul()}
			if(choixq == 2){
				test.quest5(choixf.toString())
			}
			if(choixq == 3){
				test.quest6_7(choixf.toString())
			}
		}

		/**
		 * PARTIE II "data/echantillon-flows-100000.txt"
		 */
		if(choix == 2){

			val parti2 = new Parti2();
			val scq2 = new Scanner(System.in);
			var choixq2 = 0;

			do{ 
				System.out.println("Choix des question:");
				System.out.println("Veuillez saisir 1  = Quest 1_| 2 = Quest 2 |");
				choixq2= sc.nextInt();
			}
			while(choixq2 !=1 && choixq2 !=2) ;
			if (choixq2 == 1) parti2.question1(choixf.toString())
			if (choixq2 == 2 )parti2.question2(choixf.toString())
		}

		if(choix == 3){
			/**
			 * PARTIE III
			 */
			val partie3 = new Parti3();
			partie3.question1(choixf.toString())
		}

	}
}