# Guida all'Avvio del Progetto Stock Market Trend Analysis

Questo progetto analizza in tempo reale l’andamento del mercato azionario combinando dati di borsa, indicatori macroeconomici, analisi del sentiment da news e social media.
L'obiettivo è fornire una **previsione della quotazione di ogni azienda a un minuto di distanza**, visualizzabile su una dashboard interattiva, dove è anche possibile monitorare in tempo reale eventuali anomalie nei prezzi di mercato.
Questo repository contiene due configurazioni Docker Compose per avviare il progetto Stock Market Trend Analysis in 2 diverse modalità: streaming continuo con un modello pre-trainato e un'opzione per scaricare dati storici e addestrare il modello da zero.

### Pre requisiti

Per poter avviare e utilizzare il progetto, assicurati di avere installato **Docker** sul tuo sistema. Inoltre, è fondamentale che Docker sia configurato con le seguenti risorse minime: 
- **RAM**: Minimo 8 GB di RAM (allocati a Docker)
- **CPU**: Minimo 12 CPU (allocate a Docker)

### Download del file .env

Scarica il file .env fornito e posizionalo nella directory principale di questo repository. Questo file conterrà le credenziali e le impostazioni di configurazione necessarie.

### Credenziali Alpaca

Per utilizzare i dati storici e streaming reali dello stock market, è necessario configurare le tue credenziali Alpaca. Puoi ottenere una **API_KEY_ALPACA** e una **API_SECRET_ALPACA** registrandoti tramite la Alpaca Trading API: https://alpaca.markets/. In alternativa, puoi inviare una mail a samuele.viola@studenti.unitn.it per ricevere le credenziali aggiornate.

Una volta ottenute, modifica le variabili d'ambiente nel file .env con le tue credenziali (**API_KEY_ALPACA** e **API_SECRET_ALPACA**).

### Dati dello Stock Market

I dati di Alpaca (stock market) in streaming sono reali e vengono forniti da lunedì a venerdì dalle 9:30 alle 16:00 (ora americana, ET) che corrispondono alle **15:30 alle 22:00 (ora italiana estiva, CEST)**. Per il resto del tempo, verranno generati dati sintetici per mantenere il flusso e pertanto non è necessario avere **API_KEY_ALPACA** e **API_SECRET_ALPACA** aggiornati. Gli altri flussi di dati (dati macroeconomici, company fundamentals, sentiment bluesky e sentiment news) sono sempre reali.

## 1. Docker Compose per lo Streaming (docker-compose-stream.yml)

Questa configurazione è pensata per avviare il flusso di dati in tempo reale e utilizzare un modello già addestrato per la predizione. È l'opzione ideale per chi vuole vedere il progetto in azione senza dover gestire il training iniziale.

#### Avvio

Per avviare solo il flusso di dati in streaming e la predizione, esegui il seguente comando nel tuo terminale:

<pre lang="markdown"> docker-compose -f docker-compose-stream.yml up --build -d </pre>

#### Visualizzazione della Dashboard

Dopo aver avviato la configurazione in modalità streaming, puoi accedere alla dashboard tramite il link http://localhost:8501.
Dal menu a tendina in alto puoi selezionare un'azienda e visualizzare sia l’andamento della quotazione che la previsione futura generata dal modello, insieme all’identificazione di eventuali anomalie di mercato in tempo reale. La prima predizione arriverà 5 minuti dopo l'avvio della dashboard e poi proseguirà in maniera continuativa.

## 2. Docker Compose per i Dati Storici e il Training (docker-compose-historical.yml)

Questa configurazione ti permette di scaricare circa 13 milioni di righe (~4GB) di dati storici e di addestrare il modello da zero. Questo è un processo più lungo ma ti dà il pieno controllo sul modello.

#### Avvio

Per scaricare i dati storici e avviare il training del modello, esegui il seguente comando nel tuo terminale:

<pre lang="markdown"> docker-compose -f docker-compose-historical.yml up --build -d </pre>

### Configurazione delle Aziende

Il progetto considera un set specifico di aziende. Puoi modificare queste aziende personalizzando il file companies_info.csv che si trova nella cartella postgres/. Se vuoi modificare le aziende da considerare nel progetto, è **necessario** far ripartire il docker-compose-historical.yml per scaricare nuovamente i dati storici e riaddestrare i modelli sulle nuove aziende. È possibile scegliere tra tutte le aziende quotate alla Borsa di New York (NYSE) e al NASDAQ.

Inoltre, **è fondamentale rimuovere il volume persistente** che contiene i dati precedenti di PostgreSQL, altrimenti le modifiche non verranno applicate.
Esegui il seguente comando prima di riavviare la configurazione:

<pre lang="markdown"> docker volume rm bdt-project_pgdata -d </pre>

**Importante**: Con il piano gratuito dell'API di Alpaca, non è possibile superare le 30 aziende.

Nel file companies_info.csv:
- Modifica la colonna **"ticker_id"** con un numero identificativo univoco.
- Modifica la colonna **"ticker"** con il ticker dell'azienda che desideri aggiungere.
- Modifica la colonna **"company_name"** con il nome completo dell'azienda (questo nome apparirà nella dashboard e verrà utilizzato per le ricerche di sentiment).
- Utilizza la colonna **"related_words"** per aggiungere una seconda parola chiave da ricercare per l'azienda (ad esempio, un soprannome o un termine strettamente correlato, come "Facebook" per "META").
- Imposta **"is_active"** su TRUE o FALSE per includere o escludere un'azienda dal progetto.

Dopo aver modificato e salvato il file companies_info.csv, è necessario aggiornare i dati fondamentali delle aziende. Per fare ciò, esegui il seguente comando:

<pre lang="markdown"> docker exec -it app python company_fundamentals.py -d </pre>

**Limiti dell'API per i Dati Fondamentali**: L'API per i dati fondamentali ha un limite di 250 richieste al giorno. Ogni azienda richiede 3 chiamate API. Pertanto, **non puoi modificare più di 2 volte il set completo di 30 aziende nello stesso giorno**.

### Gestione dei Docker Compose

#### Spegnimento di un Docker Compose

Per spegnere qualsiasi configurazione Docker Compose, usa il comando:

<pre lang="markdown"> docker-compose -f nome_file_docker_compose.yml down </pre>

Ad esempio, per spegnere la configurazione stream:

<pre lang="markdown"> docker-compose -f docker-compose-stream.yml down </pre>

#### Sequenza Importante per l'Avvio dei Flussi

Quando il training del modello con docker-compose-historical.yml è terminato (lo noterai dai log del container o dal fatto che il processo di training si ferma), **devi spegnere questa configurazione** (usando il comando docker-compose -f docker-compose-historical.yml down) prima di avviare docker-compose-stream.yml.






# Project Startup Guide

This project analyzes stock market trends in real time by combining financial data, macroeconomic indicators, and sentiment analysis from news and social media. The goal is to **provide a one-minute-ahead forecast of each company's stock price**, displayed on an interactive dashboard, which also allows real-time monitoring of market price anomalies.
This repository contains two Docker Compose configurations to launch the Stock Market Trend Analysis project in different modes: continuous streaming with a pre-trained model, and an option to download historical data and train the model from scratch.

### Prerequisites

To start and use the project, ensure you have **Docker** installed on your system. Furthermore, it is essential that Docker is configured with the following minimum resources:
- **RAM**: Minimum 8 GB RAM (allocated to Docker)
- **CPU**: Minimum 12 CPUs (allocated to Docker)

### Download of the .env file

Download the provided .env file and place it in the root directory of this repository. This file will contain necessary credentials and configuration settings.

### Alpaca Credentials

To use real stock market streaming and historical data, you need to configure your Alpaca credentials. You can obtain an **API_KEY_ALPACA** and an **API_SECRET_ALPACA** by registering through the Alpaca Trading API: https://alpaca.markets/. Alternatively, you can send an email to samuele.viola@studenti.unitn.it to receive updated credentials.

Once obtained, modify the environment variables in the .env file with your credentials (**API_KEY_ALPACA** and **API_SECRET_ALPACA**).

### Stock Market Data

The Alpaca (stock market) streaming data is real and is provided Monday to Friday from 9:30 AM to 4:00 PM (US Eastern Time, ET), which corresponds to **3:30 PM to 10:00 PM (Italian Summer Time, CEST)**. For the rest of the time, synthetic data will be generated to maintain the flow, and therefore, it is not necessary to have updated **API_KEY_ALPACA** and **API_SECRET_ALPACA**. The other data streams (macroeconomics data, company fundamentals, bluesky’s sentiment and news’ bluesky) are always real.

## 1. Docker Compose for Streaming (docker-compose-stream.yml)

This configuration is designed to start the real-time data stream and use an already trained model for prediction. It's the ideal option for those who want to see the project in action without having to manage the initial training.

#### Startup

To start only the streaming data flow and prediction, run the following command in your terminal:

<pre lang="markdown"> docker-compose -f docker-compose-stream.yml up --build -d </pre>

#### Dashboard Visualization

After launching the streaming configuration, you can access the dashboard at http://localhost:8501.
From the dropdown menu at the top, you can select a company and view both the stock price trend and the future prediction generated by the model, along with the real-time detection of potential market anomalies.

## 2. Docker Compose for Historical Data and Training (docker-compose-historical.yml)

This configuration allows you to download approximately 13 million rows of historical data (~4GB) and train the model from scratch. This is a longer process but gives you full control over the model. The first prediction will be available 5 minutes after the dashboard starts and will then continue in a continuous manner.

#### Startup

To download historical data and start model training, run the following command in your terminal:

<pre lang="markdown"> docker-compose -f docker-compose-historical.yml up --build -d </pre>

### Company Configuration

The project considers a specific set of companies. You can modify these companies by customizing the companies_info.csv file located in the postgres/ folder. If you want to change the companies to be considered in the project, it is **necessary** to restart docker-compose-historical.yml to re-download the historical data and retrain the models on the new companies. You can choose from all the companies listed on the New York Stock Exchange (NYSE) and NASDAQ.

Additionally, **you must remove the persistent PostgreSQL volume** that stores previous company data. Otherwise, the changes will not take effect.
Run the following command before restarting the configuration:

<pre lang="markdown"> docker volume rm bdt-project_pgdata </pre>

**Important**: With Alpaca's free API plan, you cannot exceed 30 companies.

In the companies_info.csv file:
- Modify the **"ticker_id"** column with a unique identification number.
- Modify the **"ticker"** column with the ticker of the company you wish to add.
- Modify the **"company_name"** column with the full company name (this name will appear in the dashboard and will be used for sentiment searches).
- Use the **"related_words"** column to add a second keyword to search for that company (e.g., a company nickname or a closely related term, like "Facebook" for "META").
- Set **"is_active"** to TRUE or FALSE to include or exclude a company from the project.

After modifying and saving the companies_info.csv file, you need to update the fundamental data for the companies. To do this, run the following command:

<pre lang="markdown"> docker exec -it app python company_fundamentals.py </pre>

**API Limits for Fundamental Data**: The API for fundamental data has a limit of 250 requests per day. Each company requires 3 API calls. Therefore, you cannot modify the full set of 30 companies more than 2 times on the same day.

### Managing Docker Compose

#### Shutting Down a Docker Compose

To shut down any Docker Compose configuration, use the command:

<pre lang="markdown"> docker-compose -f docker_compose_filename.yml down </pre>

For example, to shut down the historical configuration:

<pre lang="markdown"> docker-compose -f docker-compose-historical.yml down </pre>

#### Important Sequence for Starting Streams

When the model training with docker-compose-historical.yml is finished (you'll notice this from the container logs or when the training process stops), **you must shut down this configuration** (using the command docker-compose -f docker-compose-historical.yml down) before starting docker-compose-stream.yml.
