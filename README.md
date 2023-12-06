## Projet de programmation large échelle, M2 Informatique 2023-2024

L’objectif de ce projet est de concevoir un système d’analyse de combinaison de cartes pour le jeu
vidéo Clash Royale. Dans ce jeu chaque joueur possède un ensemble de cartes (96 maximum) et il
doit en choisir 8 pour constituer un « deck » qui lui permettra d’affronter un adversaire qui lui aussi
possède un deck de 8 cartes.

### Partie 1 : Map/Reduce

Pour cette première partie, il nous est demandé de calculer pour chaque deck :
- Son nombre d'utilisations
- Son nombre de victoires
- Le nombre de joueurs différents l'ayant utilisé au moins une fois
- Le niveau du clan le plus élevé dans lequel un joueur a obtenu une victoire en l'utilisant
- La différence moyenne de la force du deck comparé à celui de l'adversaire lorsqu'il gagne

On doit pouvoir obtenir ces infos sur plusieurs niveaux de granularité: une semaine, un mois, ou la totalité des données.
Pour chaque critère et niveau de granularité, on doit pouvoir obtenir le top K.

Etapes pour cette partie du projet:
- Faire le data cleaning (virer les données inutiles) et stocker ces nouvelles données dans un fichier texte sur lsd
- Créer un Writable permettant de stocker ces données pour chaque deck
- Faire le map-reduce et sauvegarder les données sur une BDD HBase
- Créer un site web simple permettant de visualiser les données et donc le connecter à notre HBase
- Faire un second map-reduce pour obtenir le top K sur le critère souhaité