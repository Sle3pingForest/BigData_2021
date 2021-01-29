package ScalaProject.scala;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * Intialisation de project !
 *
 */
public class App 
{
	
	/********************Definition ***********/
	static JavaSparkContext sc ; 
	public SparkConf conf;
	public List <String> sources ; 
	public List <String> destination ; 
	public List <String> nbPaquet ; 
	public List <String> time ; 
	public List <String> delai ; 
	public List <String> octet ; 
	public List <String> portSource ;
	public List <String> portDest ; 
	public List <String> protocol ; 
	public List <String> connexion ; 
	public List <String> nbArc ; 
	public List <String> inOutDegre ; 
	public int moy ; 
	Graph<String, String> graph;
	 Graph<String, String> graph2;
	 
	 
	 
	 
	 /****************Getter Setter***************************/
	public int getMoy() {
		return moy;
	}

	public void setMoy(int moy) {
		this.moy = moy;
	}

	public int getSum() {
		return sum;
	}

	public void setSum(int sum) {
		this.sum = sum;
	}

	public int getMin() {
		return min;
	}

	public void setMin(int min) {
		this.min = min;
	}

	public int getStdv() {
		return stdv;
	}

	public void setStdv(int stdv) {
		this.stdv = stdv;
	}

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}



	public int sum ; 
	public int min ; 
	public int stdv;
	public int max ;
	

	//Constructeur
	public App(){
		sources = new ArrayList<String>();
		destination = new ArrayList<String>();
		nbPaquet = new ArrayList<String>();
		time = new ArrayList<String>();
		delai= new ArrayList<String>();
		octet= new ArrayList<String>();
		portSource = new ArrayList<String>();
		portDest= new ArrayList<String>();
		protocol= new ArrayList<String>();
		connexion = new ArrayList<String>();
	    nbArc= new ArrayList<String>();
	    System.setProperty("hadoop.home.dir", "\\");
		
		 conf = new SparkConf().setAppName("Spark java").setMaster("local") ;
	      sc  = new JavaSparkContext(conf);
	}
	
	
	//Lire un fichier de données 
	public void CreatGraph(String path) {
		List <String>source = new ArrayList<String>();

		JavaRDD<String> lines = sc.textFile(path);
		int c =0;
		source.addAll(lines.take((int)lines.count()));
		for(int i = 0; i <lines.count(); i+=9) {//lines.count()
			
		    String[] line = source.get(c).split(",");
		    time.add(line[0]);
		    delai.add(line[1]);
		    sources.add(line[2]);
		    portSource.add(line[3]);
		    destination.add(line[4]);
		    portDest.add(line[5]);
		    protocol.add(line[6]);
		    nbPaquet.add(line[7]);
		    octet.add(line[8]);
		    connexion.add(line[2]+","+line[4]+","+line[7]);
		    c++;
	  }
		System.out.println("source : "+ source.get(1)+" Lines size" +lines.count());
}
	
	/********************Q1**********************************/
	//graph1 question 1
	public int MaxPaqGraph( ) {
		int max = 0; 
		for(int i = 0 ; i < connexion.size( ); i++) {
			String[] line = connexion.get(i).split(",");
			if(Integer.parseInt( line[2]) >max ) {
				this.max = ( Integer.parseInt( line[2]));}
		}
		return max ; 
	}
	
	
	
	//Min nombre paquet Graph 1
	public int MinPaqGraph( ) {
		int max = 0; 
		for(int i = 0 ; i < connexion.size( ); i++) {
			String[] line = connexion.get(i).split(",");
			if (max == 0 )
				this.max = ( Integer.parseInt( line[2]));
		    else if(Integer.parseInt( line[2]) < max  && max != 0 ) {
				this.max = ( Integer.parseInt( line[2]));}
		}
		return max ; 
	}
	
	
	//Question 1  Calcul somme min max dev moy 
	public int NbConnection(String s , String d) {
		
		int nbConnexion =0;
		this.setSum(0) ; 
		this.setMin(-1 ) ; 
		this.setMax(0); 
		this.setMoy(0); 
		String source ; 
		String dest ;
		
		
		for(int i = 0 ; i < connexion.size( ); i++) {
			String[] line = connexion.get(i).split(",");	
			if(line[0].equals(s) && line[1].equals(d)) {
				if(Integer.parseInt( line[2])>this.getMax())
					this.setMax( Integer.parseInt( line[2]));
				
				if(this.getMin() == -1 )
					this.setMin(Integer.parseInt( line[2]));
				else if(Integer.parseInt( line[2])<this.getMin() )
					this.setMin(Integer.parseInt( line[2]));
				//calcul sum
				this.setSum(this.getSum() + Integer.parseInt( line[2]));
				// nb Connexion
				nbConnexion++;
			}
		}
		
		//Calcul moyenne
		if(sum>0)
			this.setMoy( this.getSum ()/ nbConnexion); 
		return nbConnexion;
		
	}
	/******************  Creat Graph 1 ****************************/
	//graph 1 question 2
	public void CreatConnexionGraph() {
        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        
     	List<Tuple2<Object, String>> vert = new ArrayList<>();
     	List<Edge<String>> lEdge = new ArrayList<>();
     	JavaRDD<Tuple2<Object, String>> vertRDD ;
     	JavaRDD<Edge<String>> edgeRDD;
        for(int i = 0 ; i <connexion.size() ; i++) {
        	String[] line = connexion.get(i).split(",");
        	
        	vert.add(new Tuple2<>("", line[0] ));
        	vert.add(new Tuple2<>("", line[1] ));    
        	String def = ""+this.getSum() +""+this.getMin()+""+this.getMax()+""+this.getMoy();
        
        	lEdge.add(new Edge<String>(Integer.parseInt(line[0]), Integer.parseInt(line[1]), def));
   
       
        }
     
         vertRDD = sc.parallelize(vert);
         edgeRDD = sc.parallelize(lEdge);
		
        this. graph = Graph.apply( vertRDD.rdd(),edgeRDD.rdd(), "", StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),stringTag, stringTag);    
       // graph.edges().saveAsTextFile("C:\\Users\\ileft\\\\OneDrive\\Desktop\\Master2\\bigdata\\Project big data\\echantillon-flows\\Lien.txt");        
        graph.vertices().toJavaRDD().collect().forEach(System.out::println);
      
	}
	/******************  Creat Graph 2 ****************************/
	//graph 2 question 1
	public void CreatConnexionGraph2() {
        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        
     	List<Tuple2<Object, String>> vert = new ArrayList<>();
     	List<Edge<String>> lEdge = new ArrayList<>();
     	JavaRDD<Tuple2<Object, String>> vertRDD ;
     	JavaRDD<Edge<String>> edgeRDD;
     	
        for(int i = 0 ; i <connexion.size() ; i++) {
        	String[] line = connexion.get(i).split(",");
        	
        	vert.add(new Tuple2<>("", line[0] ));
        	vert.add(new Tuple2<>("", line[1] ));    
        	String def = ""+this.portSource.get(i) +""+this.portDest.get(i)+""+this.nbPaquet.get(i)+""+this.octet.get(i);
        	lEdge.add(new Edge<String>(Integer.parseInt(line[0]), Integer.parseInt(line[1]), def));
   
       
        }
     
         vertRDD = sc.parallelize(vert);
         edgeRDD = sc.parallelize(lEdge);
		
        graph2= Graph.apply( vertRDD.rdd(),edgeRDD.rdd(), "", StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),stringTag, stringTag);    
        graph2.edges().saveAsTextFile("C:\\Users\\ileft\\\\OneDrive\\Desktop\\Master2\\bigdata\\Project big data\\echantillon-flows\\Lien.txt");        
        graph2.vertices().toJavaRDD().collect().forEach(System.out::println);
      
	}
	
	
	/**
	 * @return ***************Graph 1 Q3**********************/
	//Calcul degree out sommet
	public int  outtDegree(String sommet) {
		List<String>couple = new ArrayList<String>();
		int nbin =0 ; 
		for(int i =0 ; i<connexion.size() ; i++)// je parcours la liste de connections
		{
			String[] line = connexion.get(i).split(",");		
			/****************Q4***************/
			 // Calcul Somme de question 4 
			int n = NbConnection(line[0] ,line[1]);  // j'appelle la fonction qui calcul la somme la moyenne le min et le max 
		
			/*****************Continue Q3 ******************/				
			if(line[0].equals(sommet)) {
				if(!couple.contains(line[0]+","+line[1])){ // calcul nombre couple source destination distinct 
					couple.add(line[0]+","+line[1]);
					nbin++;
					
					
				}
			
		}
		}
		System.out.println("Sum"+this.getSum()+""+ this.getMoy()+""+ this.getMin()+ ""+ this.getMax());	// affiche la somme final 
		return nbin; 
	}

	
	public int  intDegree(String sommet) {
		List<String>couple = new ArrayList<String>();
		int nbin =0 ; 
		for(int i =0 ; i<connexion.size() ; i++)// je parcours la liste de connections
		{
			String[] line = connexion.get(i).split(",");
			if(line[1].equals(sommet)) {
				if(!couple.contains(line[1]+","+line[0])){ // calcul nombre couple source destination distinct 
					couple.add(line[1]+","+line[0]);
					nbin++;
				}
			}
		}
		return nbin; 
	}
	
	// nombre de relation distincts
	public long InOutDeg() {
		return this.graph.edges().distinct().count(); 
	}
	
	
	/*****************************Graph 2 Question 2 ****************************************/
	public long InOutDeg2( ) {	

	return this.graph2.edges().distinct().count() ; 
	
}
	
	/*********************************************************************/
	
	/**************************************
	 * 
	 */
	//graph 1 question 4 
	public void Sum() {
		for(int i =0 ; i<connexion.size()-1 ; i++)
		{
			String[] line = connexion.get(i).split(",");
			for(int j =1 ; j<connexion.size() ; j++) {
				String[] line2 = connexion.get(i).split(",");
				 if(line[0].equals(line2[0]))
			     NbConnection(line[0] , line2[1]); 
				 
		}	
		}
	}
    public static void main( String[] args )
    {
    	  
    	
	    	App a = new App();
	    	Scanner scan = new Scanner(System.in);
	    	System.out.println("Veuillez saisir le path de fichier Data :");
	    	String path = scan.nextLine();
	    	System.out.println("votre path est bien ?  : " + path);
	    	a.CreatGraph(path);
	    	int n = a.NbConnection("C20101","C5720");
	    	System.out.println("here "+n+ " sum "+ a.getSum()+"min"+a.getMin()+" max "+a.getMax() +" moy"+a.getMoy());
	        System.out.println("Min "+ a.MinPaqGraph() + " max" + a.MaxPaqGraph());
	        a.CreatConnexionGraph(); //Creer graph 1 
	        a.CreatConnexionGraph2();//Creer Graph 2 
    }
}

