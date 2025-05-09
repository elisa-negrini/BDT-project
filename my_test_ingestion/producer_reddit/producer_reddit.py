import requests
import time
import json
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# === CONFIG ===
client_id = 'XpeJWmueFBEGkyXjE-dpcA'
client_secret = 'yOyCLHaB0Ur7R0sW75UUmc20MjCPkw'
user_agent = 'Samu_Miki'

subreddit = ["stocks", "investment", "wallstreetbets", "StockMarket", "financialindependence", "geopolitics", "politics", "worldnews", "news", "Economics"]

topic = "reddit"
kafka_server = "kafka:9092"  # per Docker
refresh_interval = 23  # ore
pause_seconds = 30

# === STATE ===
seen_ids = set()
token = None
token_time = None

# === Kafka Producer ===
producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# === FUNZIONI ===

def get_reddit_token():
    url = 'https://www.reddit.com/api/v1/access_token'
    headers = {'User-Agent': user_agent}
    data = {'grant_type': 'client_credentials'}
    auth = (client_id, client_secret)
    response = requests.post(url, headers=headers, data=data, auth=auth)
    if response.status_code == 200:
        print("✅ Token ottenuto.")
        return response.json()['access_token']
    else:
        print(f"❌ Errore token: {response.status_code}")
        return None


def get_new_reddit_posts(subreddit, token):
    url = f'https://oauth.reddit.com/r/{subreddit}/new?limit=100'
    headers = {'Authorization': f'bearer {token}', 'User-Agent': user_agent}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        posts = response.json()['data']['children']
        new_posts = []

        for post in posts:
            post_data = post['data']
            post_id = post_data.get('id')
            if post_id not in seen_ids:
                seen_ids.add(post_id)
                new_posts.append({
                    'id': post_id,
                    'title': post_data.get('title'),
                    'author': post_data.get('author'),
                    'text': post_data.get('selftext', ''),
                    'score': post_data.get('score'),
                    'num_comments': post_data.get('num_comments'),
                    'created_utc': post_data.get('created_utc'),
                    'permalink': post_data.get('permalink'),
                    'url': post_data.get('url'),
                    'subreddit': post_data.get('subreddit')
                })
        return new_posts
    else:
        print(f"[{subreddit}] ❌ Errore richiesta: {response.status_code}")
        return []



# === MAIN LOOP ===

if __name__ == "__main__":
    token = get_reddit_token()
    token_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)

            if now - token_time > timedelta(hours=refresh_interval):
                print("🔄 Rigenerazione token...")
                token = get_reddit_token()
                token_time = datetime.now(timezone.utc)

            print(f"\n🔁 Ciclo: {now.isoformat()} UTC")

            for sub in subreddit:
                posts = get_new_reddit_posts(sub, token)
                for post in posts:
                    producer.send(topic, value=post)
                    print(f"📨 [{post['subreddit']}] {post['title'][:80]}")
                    producer.flush()
                if posts:
                    print(f"📤 Inviati {len(posts)} post da /r/{sub}")

            # 🔒 Forza invio dei messaggi
            producer.flush()

            print("⏳ Pausa 30 secondi...\n")
            time.sleep(pause_seconds)

        except Exception as e:
            print(f"❗ Errore nel ciclo principale: {e}")
            time.sleep(10)  # Attesa breve prima di riprovare









# import requests
# import time
# import json
# from datetime import datetime, timedelta, timezone
# from kafka import KafkaProducer

# # Config Reddit
# client_id = 'XpeJWmueFBEGkyXjE-dpcA'
# client_secret = 'yOyCLHaB0Ur7R0sW75UUmc20MjCPkw'
# user_agent = 'Samu_Miki'

# # Subreddit
# market = ["stocks", "investment", "wallstreetbets", "StockMarket", "financialindependence"]
# geopolitics = ["geopolitics", "politics", "worldnews", "news", "Economics"]

# # Set per tenere traccia dei post già visti
# seen_ids = set()

# # Kafka
# topic = "reddit"
# producer = KafkaProducer(
#     bootstrap_servers='kafka:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# # Funzione per ottenere il token
# def get_reddit_token(client_id, client_secret, user_agent):
#     url = 'https://www.reddit.com/api/v1/access_token'
#     headers = {'User-Agent': user_agent}
#     data = {'grant_type': 'client_credentials'}
#     auth = (client_id, client_secret)

#     response = requests.post(url, headers=headers, data=data, auth=auth)

#     if response.status_code == 200:
#         token = response.json()['access_token']
#         print(f'✅ Token ottenuto: {token}')
#         return token
#     else:
#         print(f'❌ Errore token: {response.status_code}')
#         return None

# # Funzione per ottenere post nuovi
# def get_new_reddit_posts(subreddit, token):
#     url = f'https://oauth.reddit.com/r/{subreddit}/new?limit=100'
#     headers = {
#         'Authorization': f'bearer {token}',
#         'User-Agent': user_agent
#     }

#     response = requests.get(url, headers=headers)

#     if response.status_code == 200:
#         posts = response.json()['data']['children']
#         new_posts = []

#         for post in posts:
#             post_data = post['data']
#             post_id = post_data.get('id')
#             if post_id not in seen_ids:
#                 seen_ids.add(post_id)
#                 new_posts.append({
#                     'id': post_id,
#                     'title': post_data.get('title'),
#                     'author': post_data.get('author'),
#                     'text': post_data.get('selftext', ''),
#                     'score': post_data.get('score'),
#                     'num_comments': post_data.get('num_comments'),
#                     'created_utc': post_data.get('created_utc'),
#                     'permalink': post_data.get('permalink'),
#                     'url': post_data.get('url'),
#                     'subreddit': post_data.get('subreddit')
#                 })
#         return new_posts
#     else:
#         print(f"[{subreddit}] ❌ Errore richiesta: {response.status_code}")
#         return []

# # Ottieni il primo token
# token = get_reddit_token(client_id, client_secret, user_agent)
# token_time = datetime.now(timezone.utc)

# # Loop infinito con refresh token ogni 23h
# while True:
#     now = datetime.now(timezone.utc)

#     # Se il token ha più di 23 ore, rigenera
#     if now - token_time > timedelta(hours=23):
#         print("🔄 Rigenerazione token Reddit...")
#         token = get_reddit_token(client_id, client_secret, user_agent)
#         token_time = datetime.now(timezone.utc)

#     print(f"\n🔁 Ciclo alle {now.isoformat()} UTC")

#     # STREAM MARKET
#     for sub in market:
#         posts = get_new_reddit_posts(sub, token)
#         for post in posts:
#             producer.send(topic, value=post)
#         print(f"📤 Inviati {len(posts)} post MARKET da /r/{sub}")

#     # STREAM GEOPOLITICA
#     for sub in geopolitics:
#         posts = get_new_reddit_posts(sub, token)
#         for post in posts:
#             producer.send(topic, value=post)
#         print(f"📤 Inviati {len(posts)} post GEO da /r/{sub}")

#     print("⏳ Pausa 30 secondi...\n")
#     time.sleep(30)