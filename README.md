![SP_95](https://github.com/jphnh/projects/assets/165770792/054a95aa-e0ac-49e4-94b8-f032adf577bd)

![GitHub last commit](https://img.shields.io/github/last-commit/jphnh/projects)

features - installation - usage

usage - installation - config options - how to contribute - acknowledgements - donations

key features - how to use - download 

Ce projet contient un script Python conçu pour être exécuté sur Airflow. Le script extrait des données du site https://data.economie.gouv.fr, transforme les données puis les charge vers Google BigQuery.

## Caractéristiques

- Utilise Airflow pour planifier et automatiser le flux de travail via un pipeline d'Extraction, Transformation et Chargement de données
- Script prêt à être utilisé
- Extensible pour inclure d'autres sources de données ou destinations de stockage
- Adaptable et réutilisable à d'autres projets similaires
- Utilisable sur Mac (fonctionne également avec d'autres systèmes d'exploitation, mais la méthode d'installation change)

## Installation

### 1. Homebrew

Si vous n'avez pas encore Homebrew, installez-le en exécutant la commande suivante dans votre terminal :

```sh
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 2. Python

Installez Python via Homebrew :

```sh
brew install python
```

### 3. Airflow

Installez Airflow : 

```sh
pip install apache-airflow
```

Vous devez créer un environnement virtuel pour Airflow afin de ne pas mélanger les dépendances entre elles. Pour le créer et l'activer : 

```sh
python3 -m venv airflow-env
source airflow-env/bin/activate
```

### 4. Google Cloud SDK

Installez le Google Cloud SDK via Homebrew : 

```sh
brew install --cask google-cloud-sdk
```

Initialisez le SDK : 

```sh
gcloud init
```

### 5. Installation des dépendances Python

Créez un fichier `requirements.txt` et ajoutez-y les dépendances suivantes : 

```css
apache-airflow[gcp]==2.3.0
google-cloud-bigquery
```

Ensuite, installez les dépendances : 

```sh
pip install -r requirements.txt
```

Configuration d'Airflow
