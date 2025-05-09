import requests
import time
import json
from datetime import datetime
from kafka import KafkaProducer

# Il tuo accessJwt (access token)
access_jwt = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NksifQ.eyJzY29wZSI6ImNvbS5hdHByb3RvLmFjY2VzcyIsInN1YiI6ImRpZDpwbGM6NDNuYnhodGQyZHFuM3g2djcyZHpuN2ZxIiwiaWF0IjoxNzQ2ODEzNTEyLCJleHAiOjE3NDY4MjA3MTIsImF1ZCI6ImRpZDp3ZWI6ZW50b2xvbWEudXMtd2VzdC5ob3N0LmJza3kubmV0d29yayJ9.7sLkO2J4G6F1KZLrrwINZAZeG_Uh5zZMYWq6J6zZ_6yEIwBbuW9gOqXDb0amhzt8axLAjdjUcaCGBYPtZMc19Q"
# Il tuo refreshJwt (refresh token)
refresh_jwt = "eyJ0eXAiOiJyZWZyZXNoK2p3dCIsImFsZyI6IkVTMjU2SyJ9.eyJzY29wZSI6ImNvbS5hdHByb3RvLnJlZnJlc2giLCJzdWIiOiJkaWQ6cGxjOjQzbmJ4aHRkMmRxbjN4NnY3MmR6bjdmcSIsImF1ZCI6ImRpZDp3ZWI6YnNreS5zb2NpYWwiLCJqdGkiOiJYN3M0R2VlSmhGcUUxSUJyZC81SjRUZzRRRU9DMUNrTkdwOGFqVUtMbzhNIiwiaWF0IjoxNzQ2ODEzNTEyLCJleHAiOjE3NTQ1ODk1MTJ9.hcgH7f8oYQdYK6g3tc84oHJZe1j48B84jNhkbJflFWKTBubT39iBFWP8zcJIzjUf10RIss2JH_WMFCxn_OxBgg"
# Il tuo identifier (client ID)
identifier = "michelelovatomenin.bsky.social"
# La tua password (client secret)
password = "BDT-project"

# URL dell'API di Bluesky per cercare i post
url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"

# Kafka config
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'bluesky'

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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
    "price target", "IPO", "stock split", "options", "ETF", "SPY",
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "GOOG",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA", "CVX",
    "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO",
    "Apple", "Microsoft", "Nvidia", "Amazon", "Meta", "Berkshire", "Alphabet",
    "Broadcom", "Tesla", "Eli Lilly", "JPMorgan", "Visa", "Exxon", "Netflix",
    "Costco", "UnitedHealth", "Johnson", "Procter", "Mastercard", "Chevron",
    "Merck", "Pepsi", "AbbVie", "Adobe", "Walmart", "BofA", "Home Depot", 
    "Coca-Cola", "Thermo Fisher"
]

def get_new_access_token():
    global access_jwt
    token_url = "https://bsky.social/oauth/token"
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_jwt,
        'client_id': identifier,
        'client_secret': password
    }
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        token_data = response.json()
        access_jwt = token_data['access_token']
        print("Access token rinnovato.")
    else:
        print("Errore nel rinnovo del token:", response.status_code)
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
    if access_jwt is None:
        get_new_access_token()

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
