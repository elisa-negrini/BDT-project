import requests
import time
import json
from datetime import datetime
from kafka import KafkaProducer

# Il tuo accessJwt (access token)
access_jwt = ""
refresh_jwt = ""
identifier = "michelelovatomenin.bsky.social"
# La tua password (client secret)
password = "BDT-project"

# URL dell'API di Bluesky per cercare i post
url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"

# Kafka config
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'bluesky'

# Kafka producer con retry finché il broker non è disponibile
def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connessione a Kafka riuscita.")
            return producer
        except Exception as e:
            print(f"⏳ Kafka non disponibile, ritento in 5 secondi... ({e})")
            time.sleep(5)

producer = connect_kafka()


# Set per memorizzare gli ID dei post già processati
processed_posts = set()

# Lista delle parole chiave
keywords = [
    "stock", "stocks", "market", "markets", "equity", "equities",
    "trading", "investing", "investment", "portfolio", "bull market", 
    "bear market", "recession", "nasdaq", "dow jones", "s&p 500", 
    "earnings", "earnings report", "buy the dip", "short selling", 
    "federal reserve", "interest rates", "inflation", "wall street",
    "financial markets", "volatility", "dividends", "valuation",
    "price target", "IPO", "stock split", "ETF", "SPY",
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA", "CVX",
    "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO",
    "Apple", "Microsoft", "Nvidia", "Amazon", "Meta", "Berkshire", "Alphabet",
    "Broadcom", "Tesla", "Eli Lilly", "JPMorgan", "Visa", "Exxon", "Netflix",
    "Costco", "UnitedHealth", "Johnson", "Procter", "Mastercard", "Chevron",
    "Merck", "Pepsi", "AbbVie", "Adobe", "Walmart", "BofA", "Home Depot", 
    "Coca-Cola", "Thermo Fisher", "Big Blue"
]

def get_initial_tokens():
    global access_jwt, refresh_jwt
    session_url = "https://bsky.social/xrpc/com.atproto.server.createSession"
    payload = {
        "identifier": identifier,
        "password": password
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(session_url, headers=headers, json=payload)
    
    if response.status_code == 200:
        token_data = response.json()
        access_jwt = token_data['accessJwt']
        refresh_jwt = token_data['refreshJwt']
        print("Access token e refresh token ottenuti con successo.")
    else:
        print("Errore nell'ottenere i token iniziali:", response.status_code, response.text)
        access_jwt = None
        refresh_jwt = None


def get_new_access_token():
    global access_jwt, refresh_jwt
    url = "https://bsky.social/xrpc/com.atproto.server.refreshSession"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {refresh_jwt}"
    }
    response = requests.post(url, headers=headers)
    
    if response.status_code == 200:
        token_data = response.json()
        access_jwt = token_data['accessJwt']
        refresh_jwt = token_data['refreshJwt']
        print("✅ Token rinnovato con successo.")
    else:
        print(f"❌ Errore nel rinnovo del token: {response.status_code}, {response.text}")
        access_jwt = None

def fetch_posts(keyword):
    headers = {
        'Authorization': f'Bearer {access_jwt}'
    }
    params = {
        'q': keyword
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        posts = response.json().get('posts', [])
        return posts
    else:
        print(f"Errore nella richiesta dei post per '{keyword}':", response.status_code)
        return []

def track_posts():
    global access_jwt
    get_initial_tokens()

    if access_jwt is None:
        print("Impossibile proseguire senza un accessJwt valido.")
        return

    last_token_renewal_time = time.time()
    
    while True:
        if access_jwt is None:
            print("Impossibile proseguire senza un accessJwt valido.")
            break

        print("Eseguendo ricerca nuovi post...")

        for keyword in keywords:
            new_posts = fetch_posts(keyword)
            new_posts_filtered = [post for post in new_posts if post['cid'] not in processed_posts]

            if new_posts_filtered:
                print(f"Trovati {len(new_posts_filtered)} nuovi post per '{keyword}'.")

                for post in new_posts_filtered:
                    processed_posts.add(post['cid'])
                    post_id = post.get('cid')
                    post_text = post.get('record', {}).get('text')
                    post_created_at = post.get('record', {}).get('createdAt')

                    data = {
                        "id": post_id,
                        "text": post_text,
                        "created_at": post_created_at,
                        "keyword": keyword
                    }

                    print(json.dumps(data, ensure_ascii=False))
                    producer.send(KAFKA_TOPIC, value=data)

            else:
                print(f"Nessun nuovo post trovato per '{keyword}'.")

        time.sleep(30)

        if time.time() - last_token_renewal_time > 900:
            get_new_access_token()
            last_token_renewal_time = time.time()

track_posts()
