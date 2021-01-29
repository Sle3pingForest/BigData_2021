# Project BigData
#### LY Hue Nam
#### TRABELSI Illef

GIT : (https://github.com/Sle3pingForest/BigData_2021)


# Intro:
Le projet est séparer en deux parties: le Spark et la Graph. La première partie est realisée par Mr LY, et la deuxième est réalisée par Mme Trabelsi.

##Creation de project:
    - Sous eclipse, crée un scala project
    - Remplace le src initial par le src ci-jointe
    - dans le bluid path du project Add External JARs : ajouter tous les *.jar dans le dossier spark 3.01/jars
    - ajouter scala Library container
    Puis run le main.scala

## Partie Spark

Dans cette partie, on a choisi de coder en scala. Nous avons utilisée Eclipse comme IDE avec une plugin scala et des libraies de spark.

La partie spark elle meme est séparer en plusieur sous parties. Lorsque vous lancez le programme, vous aurez plusieur choix. 

Pour la partie 1:
    vous avez 3 choix pour 3 ensembles de question 
    -- Choix 1  = question 1, 2 et 3
    -- Choix 2  = question 5
    -- Choix 3  = question 6 et 7
On n'a pas les questions 4 et 8 parmi les choix, car c'est au niveau de timer, elle est include dans les questions deja.
ATTENTION, ici, on aura une doute sur le timer. la question est si on stop l'affichage au bout d'un certaine de temps, ou on stop spark qui load le file data apres une certaine de temps.
On a choisi la premiere supposition = > On stop l'affichage apres une certaine de temps. Car , on trouve cela plus logique de load entierement la fichier pour plus d'informations stockées.

De plus, pour calculer la somme, qui est un entier, donc qui peut aller tres tres grand. sous 32 bits, on aura un probleme car la somme sera supperieur à 2 147 483 647.

Le seul inconvenient dans notre l'algo c'est vous serait obliger de relancer a nouveau apres l'affichage du résultat de votre choix.

#### Méthode et Codage
Pour la lisibilité, on respectera le numeration des partie du sujet.
Partie 2.1 : ( Correspondre au choix 1)
    - pour la lecture de fichier , on utlise Source.fromFile, puis on split(',') chaque line pour stock dans les arrays differentes pour chaque colonne.
    - pour la question 5: on utilise un groupByKey
    - pour la question 6,7: on utilise: groupeByKey, flatmap, reduceByKey
    - pour coder le timer, on recupere le temps du systeme, puis le boucle sera stopper si le time su systeme = le time ( init) + 5 ( pour 5 min)

Partie 2.2: ( Correspondre au choix 2)
    - meme technique que la partie precedent, mais avec une certaine modification d'ordre .
Partie 2.3: ( Correspondre au choix 3)
    - meme technique avec le timer de la partie 2.1

#### Frame Partie 
Fichier: App.java

Cette partie a été développé en Java Spark sur eclipse SCALA IDE comme un projet Maven with spark dependecy.
 
Je lis le fichier qui contient les données en JavaRDD ;
 
pour Graph 1 je crée un graph de connexion avec l'ensemble de définition comme la somme , le nombre de paquets.

pour Graph 2 je crée un nouveau Graph avec une nouvelle fonction et les edges Prennent en paramètres sommet source sommet destination et les définitions des arcs. 

J'ai utilisé les mêmes fonctions pour calculer le nombre des edges distinct et les inDgree et outDgree de deux graph puisque ils ont le même principe; 

pour compiler le code il faut changer le path de fichier de données qui est mis par défaut.
 
