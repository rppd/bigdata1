Projet BIGDATA

Le but de ce projet etait d'appliquer ce que nous avions appris lors du cours de BigData. 

Les principaux objectifs étaient de :
  -  se familiariser avec les technologies qui nous avaient été présentées lors de la partie théorique du cours
  -  manipuler de vrais gros volumes de données afin de se rendre comptes des diffcultées spécifiques aux projets en bigdata

La première étape a été de trouver un projet et un dataset pertinent au vues de nos attentes. Nous étions d'abord parti sur l'idée d'utiliser le dataset officiel des CVEs (Common Vulnerabilities & exposures) pour son très grand intérêt cyber.
Nous avions ainsi pensé l'utiliser pour faire des statistiques très macro mais assez révélatrice de l'état de la menace et ainsi comparer avec ce que nous avions appris lors de nos autres cours de l'année:
  -  quels sont les types de vulnérabilités les plus fréquentes? (buffer overflow, file format etc)
  -  quels types d'équipements sont les plus touchés? (routeurs, pare-feux, serveurs)
  -  quelles entreprises sont spécialement touchées, ont le plus de vulnérabilités sur leurs produits?

Malheureusement, le dataset ne faisait que 2 Gigas, répartis sur 280 000 fichiers JSON. On aurait tout de même pu utiliser les technologies vu en cours sur ce dataset mais on aurait sacrifié le côté découverte du bigdata et de ses spécificités/difficultés au profit d'un intérêt purement cyber.
Ce n'était pas l'objectif de ce projet.

Nous avons donc continuer à chercher, mais il y avait peu de datasets disponibles alliant à la fois masse de données, facilement requêtables et d'intérêt.
Nous avons finalement décidé de jouer le jeu jusqu'au bout en prenant un dataset purement d'intérêt bigdata.

Ce dataset intitulé "Google cluster-usage traces v3" compile les données relatives à l'utilisation des clusters de google sur une période données.
Les données retournées sont multiples, allant des caractéristiques des machines utilisés dans le cluster à leur consommation de CPU au cours du temps en passant par le planning de réservation de ces machines.
Le dataset fait plus de 10 Tos ce qui répond largement à notre besoin. 
Nous avons donc entrepris de réaliser cinqs calculs, variants en complexité, afin d'illustrer les différentes axes d'intérêts permis par ces donnés.

Aspect techniques:
Un des gros points positifs de ce dataset est qu'il est mis à disposition par google sur BigQuery et facilement requêtable à travers un connecteur présent dans PySpark. ( pas besoin de le stocker nous même)
De plus, l'offre gratuite de Google Cloud Platform (GCP), nous a permis de directement gérer les technologies utilisées depuis la plateforme en ligne et de faire nos calculs avec PySPark depuis des Jupyters Notebook directement mis à disposition.

Difficultés:
Nous avons utilisées beaucoups de frameworks et d'interfaces graphiques, ce qui a permi au final de garder un code simple et direct pour faire nos calculs. 
En contrepartie, quand quelque chose ne marchait pas, nous avons été forcé de regarder sous le capot de GCP où on retrouvait toutes les technologies et la c'était autrement plus compliqué que si nous l'avions fait à la main

Choix technologiques:
Le choix du dataset impose l'utilisation de Google BigQuery (seul moyen disponible pour y accéder). Heureusement, cela ouvre la porte à tout un écosystème technologique, dont un certain nombre d'outils sont utiles à ce projet.

Notamment, on choisit d'utiliser le connecteur PySpark+BigQuery, pour rejoindre les techniques vues en cours, et se raccrocher à l'écosystème Google Cloud. De plus, Google Cloud propose un environnement tout trouvé pour utiliser PySpark+BigQuery : en suivant la documentation proposée, on déploie un cluster Google Dataproc Compute, avec plusieurs noeuds (ici 1 master et 2 slaves), déjà configurés pour distribuer des tâches avec PySpark.

On utilise ce cluster avec un Notebook JupyterLab, où le connecteur PySpark est déjà fourni (sous la forme d'un JAR), avec même un échantillon de code Python pour l'activer. Le dataset est alors disponible sur BigQuery, en utilisant simplement une chaîne de caractères l'identifiant.