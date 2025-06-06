@echo off
setlocal

:: Vai alla directory del tuo progetto Docker Compose
:: Sostituisci "C:\percorso\alla\tua\cartella\progetto" con il percorso reale
:: Questa DEVE essere la cartella che contiene il tuo docker-compose.yml
cd "my_test_ingestion" 

echo %DATE% %TIME% - Avvio retrain del modello...

:: Avvia il database in background se non è già in esecuzione
docker compose up -d postgre

:: Aspetta un po' per assicurarti che il database sia pronto
timeout /t 30 /nobreak

:: Avvia il servizio di training (lstm_trainer)
:: --build = ricostruisce l'immagine (utile per aggiornamenti del codice o requirements)
:: --abort-on-container-exit = ferma gli altri container se questo esce con errore
docker compose run --build --abort-on-container-exit lstm_trainer

:: Controlla il codice di uscita del comando precedente
IF %ERRORLEVEL% NEQ 0 (
    echo %DATE% %TIME% - ERRORE durante il retrain del modello! Codice di uscita: %ERRORLEVEL%
) ELSE (
    echo %DATE% %TIME% - Retrain del modello completato con successo.
)

echo %DATE% %TIME% - Fine script retrain.

endlocal