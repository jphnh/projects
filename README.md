![SP_95](https://github.com/jphnh/projects/assets/165770792/054a95aa-e0ac-49e4-94b8-f032adf577bd)

![GitHub last commit](https://img.shields.io/github/last-commit/jphnh/projects)

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

## Configuration de Google BigQuery

### 1. Création d'un compte Google Cloud

Si vous n'avez pas encore de compte Google Cloud, créez-en un en visitant [Google Cloud](https://cloud.google.com/?hl=en).

### 2. Création d'un projet Google Cloud

1. Accédez à la [console Google Cloud](https://console.cloud.google.com/?_ga=2.26276858.-671429165.1703105920&hl=en&pli=1).

2. Créez un nouveau projet

3. Notez l'ID du projet car il sera utilisé plus tard.

### 3. Activation de l'API BigQuery

1. Dans la console Google Cloud, accédez à **API & Services > Bibliothèque**.
2. Recherchez "BigQuery API" et activez-la pour votre projet.

### 4. Création d'un dataset BigQuery

1. Accédez à la console BigQuery.
2. Sélectionnez votre projet.
3. Créez un nouveau dataset.

### 5. Création d'un compte de services et clé API

1. Dans la console Google Cloud, accédez à IAM & Admin > Comptes de service.
2. Créez un nouveau compte de service.
3. Associez les rôles nécessaires (par exemple, BigQuery Data Editor et BigQuery Job User).
4. Créez une clé JSON pour ce compte de service et téléchargez-la. Gardez cette clé en lieu sûr car elle sera utilisée pour la configuration d'Airflow.

## Configuration d'Airflow

### 1. Configurer le fichier `airflow.cfg` :

Modifiez le fichier de configuration d'Airflow pour ajouter les informations nécessaires pour Google Cloud et BigQuery. Cela inclut généralement la configuration des connexions et des variables Airflow.

### 2. Configurer les connexions Airflow : 

Ajoutez une connexion Airflow pour Google Cloud : 

```sh
airflow connections add 'my_gcp_connection' \
--conn-type 'google_cloud_platform' \
--conn-extra '{"extra__google_cloud_platform__key_path":"/path/to/your/service-account-file.json", "extra__google_cloud_platform__scope":"https://www.googleapis.com/auth/cloud-platform"}'
```

### 3. Configurer les variables Airflow : 

```sh
airflow variables set 'gcp_project' 'your-gcp-project-id'
airflow variables set 'bq_dataset' 'your-bigquery-dataset'
```

## Placement du fichier Python pour le DAG

Pour qu'Airflow reconnaisse votre DAG, placez le fichier Python du DAG dans le répertoire dags de votre installation Airflow. Si vous avez suivi les instructions d'installation par défaut, ce répertoire se trouve généralement à l'emplacement suivant :

```sh
~/airflow/dags
```

Copiez votre fichier Python dans ce répertoire : 

```sh
cp /path/to/your_dag.py ~/airflow/dags/
```

## Exécution

Pour exécuter le script, démarrez le scheduler et le webserver Airflow dans votre Terminal :

```sh
airflow scheduler
```

Dans un autre terminal, démarrez le webserver :

```sh
airflow webserver 
```

Accédez à l'interface web d'Airflow (généralement http://localhost:8080), et activez votre DAG pour exécuter le script.

## Contribution 

Les étapes d'installation et de configuration étant génériques, des ajustements peuvent être nécessaires. Vous pouvez proposer des améliorations en créant une branche au répertoire :

1. Créez une branche pour votre fonctionnalité : `$ git checkout https://github.com/jphnh/projects -b nom_pour_la_nouvelle_branche`.
2. Faites les modifications et testez
3. Soumettez une **Pull Request** avec une description des changements proposés

## Licence

Distribué sous la licence MIT. Voir `LICENSE` pour plus d'informations.
