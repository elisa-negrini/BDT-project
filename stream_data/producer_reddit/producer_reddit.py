import requests
import time
import json
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
import traceback
import os

# === CONFIG ===
client_id = os.getenv("REDDIT_CLIENT_ID")
client_secret = os.getenv("REDDIT_CLIENT_SECRET")
user_agent = os.getenv("REDDIT_USER_AGENT") 

subreddit = [
    "stocks", "investment", "wallstreetbets", "StockMarket",
    "financialindependence", "geopolitics", "politics",
    "worldnews", "news", "Economics"
]

KAFKA_TOPIC = "reddit"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

refresh_interval = 23  # hours
pause_seconds = 30

# === STATE ===
seen_ids = set()
token = None
token_time = None

def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka.")
            return producer
        except Exception as e:
            print(f"Kafka not available, retrying in 5 seconds... ({e})")
            time.sleep(5)

producer = connect_kafka()

# === FUNCTIONS ===

def get_reddit_token():
    url = 'https://www.reddit.com/api/v1/access_token'
    headers = {'User-Agent': user_agent}
    data = {'grant_type': 'client_credentials'}
    auth = (client_id, client_secret)
    response = requests.post(url, headers=headers, data=data, auth=auth)
    if response.status_code == 200:
        print("Token obtained.")
        return response.json()['access_token']
    else:
        print(f"Error obtaining token: {response.status_code}")
        print(response.text)
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
            text = post_data.get('selftext', '')
            if post_id not in seen_ids:
                seen_ids.add(post_id)
                new_posts.append({
                    'id': post_id,
                    'title': post_data.get('title'),
                    'author': post_data.get('author'),
                    'text': text,
                    'score': post_data.get('score'),
                    'num_comments': post_data.get('num_comments'),
                    'created_utc': post_data.get('created_utc'),
                    'permalink': post_data.get('permalink'),
                    'url': post_data.get('url'),
                    'subreddit': post_data.get('subreddit')
                })
        return new_posts
    else:
        print(f"[{subreddit}] Error fetching posts: {response.status_code}")
        return []

# === MAIN LOOP ===

if __name__ == "__main__":
    token = get_reddit_token()
    token_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)

            if now - token_time > timedelta(hours=refresh_interval):
                print("Refreshing token...")
                token = get_reddit_token()
                token_time = datetime.now(timezone.utc)

            print(f"\nCycle: {now.isoformat()} UTC")

            for sub in subreddit:
                print(f"\nChecking subreddit: /r/{sub}")
                posts = get_new_reddit_posts(sub, token)
                print(f"Received {len(posts)} posts from /r/{sub}")

                if posts:
                    try:
                        for post in posts:
                            print(f"[{post['subreddit']}] {post['title'][:80]} | text length: {len(post['text'])}")
                            producer.send(KAFKA_TOPIC, value=post)
                        producer.flush()
                        print(f"Sent {len(posts)} posts from /r/{sub}")
                    except Exception as e:
                        print(f"Error sending posts to Kafka from /r/{sub}: {e}")
                else:
                    print(f"No new posts from /r/{sub}")
                time.sleep(2)

            producer.flush()
            print("Sleeping for 30 seconds...\n")
            time.sleep(pause_seconds)

        except Exception as e:
            print(f"Error in main loop: {e}")
            traceback.print_exc()
            time.sleep(10)