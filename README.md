# TEDx Clusteranalyse Project

Dit project voert een clusteranalyse uit op TED-video's om te bepalen of ze populair zijn of niet, gebaseerd op verschillende kenmerken zoals het aantal views, likes, reacties, en de duur van de video. Het doel van dit project is om video's te clusteren en populaire video's te identificeren met behulp van machine learning technieken. Het project maakt gebruik van een **CI/CD-pipeline** om dagelijks nieuwe YouTube-video's op te halen en te verwerken.

## Inhoud

- [Doel van het Project](#doel-van-het-project)
- [Dataset](#dataset)
- [Gebruikte Technologieën](#gebruikte-technologieën)
- [Installatie](#installatie)
- [Gebruik](#gebruik)
- [Evaluatie](#evaluatie)
- [CI/CD-pipeline](#cicd-pipeline)
- [Licentie](#licentie)

## Doel van het Project

Dit project maakt gebruik van de KMeans clustering techniek van scikit-learn om TED-video's te groeperen op basis van hun metadata. Door deze clustering kunnen we video's classificeren als 'populair' of 'onpopulair'. Het project is gericht op het gebruik van verschillende kenmerken van de video's om deze clustering te verbeteren en te evalueren met behulp van de silhouette score.

Daarnaast wordt een **CI/CD-pipeline** gebruikt om dagelijks nieuwe YouTube-video's op te halen via de YouTube API, deze te verwerken en toe te voegen aan de dataset voor analyse.

## Dataset

De dataset die in dit project wordt gebruikt is de 'Kaggle_TED_video_metadata_balanced.csv'. Deze dataset bevat de volgende kolommen:

- **tags**: Tags die zijn gekoppeld aan de video.
- **views**: Aantal views van de video.
- **likes**: Aantal likes.
- **commentcount**: Aantal reacties.
- **duration**: Duur van de video in seconden.
- **cluster**: De cluster waartoe de video behoort (aangemaakt door KMeans).

Je kunt de dataset vinden op [Kaggle](https://www.kaggle.com/datasets).

## Gebruikte Technologieën

- **Python**: Programmeertaal die voor de analyse en machine learning wordt gebruikt.
- **scikit-learn**: Machine learning bibliotheek voor clustering (KMeans) en datascalaanpassing (StandardScaler).
- **Pandas**: Voor het verwerken en manipuleren van dataframes.
- **Matplotlib**: Voor het visualiseren van clusteringresultaten.
- **CI/CD Pipeline**: Automatisering van het dagelijks ophalen en verwerken van YouTube-video's.

## Installatie

Volg deze stappen om het project op je lokale machine te draaien:

1. Clone de repository:
   ```bash
   git clone https://github.com/jouwgebruikersnaam/tedx-clustering-project.git
