import os
import sys
import io
import pandas as pd
from minio import Minio

# --- Configurazione MinIO (usa le variabili d'ambiente o i default) ---
MINIO_URL_DEFAULT = "minio:9000" # Importante: usa il nome del servizio Docker Compose
MINIO_ACCESS_KEY_DEFAULT = "admin"
MINIO_SECRET_KEY_DEFAULT = "admin123"
MINIO_SECURE_DEFAULT = "False" # Default per connessione HTTP (non HTTPS)

def read_fundamental_data_test():
    print("🚀 [TEST] Starting fundamental data read test...", file=sys.stderr)
    
    # 1. Recupera e pulisce le variabili d'ambiente
    minio_url_raw = os.getenv("MINIO_URL", MINIO_URL_DEFAULT)
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", MINIO_ACCESS_KEY_DEFAULT)
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", MINIO_SECRET_KEY_DEFAULT)
    minio_secure_str = os.getenv("MINIO_SECURE", MINIO_SECURE_DEFAULT)

    # Rimuove spazi bianchi e caratteri di newline all'inizio/fine della stringa URL
    minio_url_cleaned = minio_url_raw.strip()
    # Converte la stringa "False"/"True" in booleano
    minio_secure_bool = minio_secure_str.lower() == "true"

    print(f"DEBUG: Processed MINIO_URL: '{minio_url_cleaned}'", file=sys.stderr)
    print(f"DEBUG: Processed MINIO_ACCESS_KEY: '{minio_access_key}'", file=sys.stderr)
    print(f"DEBUG: Processed MINIO_SECRET_KEY: (hidden)", file=sys.stderr)
    print(f"DEBUG: Processed MINIO_SECURE: {minio_secure_bool}", file=sys.stderr)

    try:
        # 2. Inizializza il client MinIO
        minio_client = Minio(
            minio_url_cleaned, # Usa la stringa pulita
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=minio_secure_bool
        )
        print("✅ [TEST] MinIO client initialized successfully.", file=sys.stderr)

        # 3. Definisci il bucket e l'oggetto di test
        bucket_name = "company-fundamentals"
        object_name = "AAPL/2024.parquet" # Sostituisci con un file che sai esistere e essere valido

        # --- Codice opzionale per creare un bucket/file di test se non esiste ---
        # Questo è utile per assicurarsi che MinIO abbia qualcosa da leggere
        if not minio_client.bucket_exists(bucket_name):
            print(f"[INFO] Bucket '{bucket_name}' does not exist, creating it.", file=sys.stderr)
            minio_client.make_bucket(bucket_name)

        # Esempio per creare un file di test se AAPL/2024.parquet non esiste
        # Nota: questo sovrascriverà il tuo file esistente se non stai attento!
        # if not minio_client.stat_object(bucket_name, object_name): # MinIO client v7 supporta stat_object per verificare esistenza
        #     print(f"[INFO] Test file '{object_name}' not found, uploading a dummy one.", file=sys.stderr)
        #     df_dummy = pd.DataFrame({
        #         'eps': [10.5], 'cashflow_freeCashFlow': [100.0], 'revenue': [1000.0],
        #         'netIncome': [200.0], 'balance_totalDebt': [50.0], 'balance_totalStockholdersEquity': [500.0],
        #         'timestamp': ['2024-01-01T00:00:00Z'] # Aggiungi un timestamp per robustezza
        #     })
        #     parquet_buffer_dummy = io.BytesIO()
        #     df_dummy.to_parquet(parquet_buffer_dummy, index=False)
        #     parquet_buffer_dummy.seek(0)
        #     minio_client.put_object(
        #         bucket_name,
        #         object_name,
        #         parquet_buffer_dummy,
        #         len(parquet_buffer_dummy.getvalue()),
        #         content_type="application/octet-stream"
        #     )
        #     print(f"✅ [TEST] Uploaded dummy file '{object_name}' for testing.", file=sys.stderr)
        # -----------------------------------------------------------------------

        # 4. Tenta di recuperare l'oggetto da MinIO
        print(f"📥 [TEST] Attempting to get object: {bucket_name}/{object_name}", file=sys.stderr)
        response = None # Inizializza per il blocco finally
        try:
            response = minio_client.get_object(bucket_name, object_name)
            print("✅ [TEST] Object retrieved successfully from MinIO. Attempting to read with pandas...", file=sys.stderr)
            
            # 5. Tentativo di leggere il Parquet direttamente dal response stream
            # Se questo fallisce ancora con 'seek', prova la riga commentata sotto (con BytesIO)
            df = pd.read_parquet(response) 
            
            # --- Alternativa con BytesIO (se il 'seek' persiste) ---
            # parquet_bytes = io.BytesIO(response.read()) 
            # df = pd.read_parquet(parquet_bytes)
            # print("DEBUG: Used BytesIO for reading Parquet.", file=sys.stderr)
            # -------------------------------------------------------

            print(f"🎉 [TEST] Successfully read {len(df)} rows for {object_name}", file=sys.stderr)
            print("--- Head of DataFrame ---", file=sys.stderr)
            print(df.head(), file=sys.stderr)
            print("-------------------------", file=sys.stderr)

        except Exception as e:
            print(f"❌ [ERROR] Failed to read parquet directly from MinIO response: {e}", file=sys.stderr)
        finally:
            if response:
                response.close()
                response.release_conn()

    except Exception as e:
        print(f"❌ [CRITICAL ERROR] Overall test failed during MinIO client init or bucket check: {e}", file=sys.stderr)
        sys.exit(1) # Termina il container con un codice di errore

if __name__ == "__main__":
    read_fundamental_data_test()