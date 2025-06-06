import requests
import time
import json
from datetime import datetime
from kafka import KafkaProducer
import os
import psycopg2

# Your accessJwt (access token)
access_jwt = ""
refresh_jwt = ""
identifier = os.getenv("BLUESKY_IDENTIFIER")
password = os.getenv("BLUESKY_PASSWORD")

# Bluesky API URL to search posts
url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"

# Kafka config
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = 'bluesky'

# Kafka producer with retry logic
def connect_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka producer connection established.")
            return producer
        except Exception as e:
            print(f"Kafka not available, retrying in 5 seconds... ({e})")
            time.sleep(5)

producer = connect_kafka()

# Set to track already processed post IDs
processed_posts = set()

# PostgreSQL config
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Load keywords from PostgreSQL
def load_keywords_from_db():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ticker, company_name, related_words
            FROM companies_info WHERE is_active = TRUE
        """)
        rows = cursor.fetchall()
        conn.close()

        db_keywords = set()
        for row in rows:
            for field in row:
                if field and isinstance(field, str):
                    stripped_field = field.strip()
                    if len(stripped_field) > 1:
                        db_keywords.add(stripped_field)

        return list(db_keywords)

    except Exception as e:
        print(f"Error fetching keywords from database: {e}")
        return []

# Static keyword list
keywords = [ 
    "stock", "stocks", "market", "markets", "equity", "equities",
    "trading", "investing", "investment", "portfolio", "bull market", 
    "bear market", "recession", "nasdaq", "dow jones", "s&p 500", 
    "earnings", "earnings report", "buy the dip", "short selling", 
    "federal reserve", "interest rates", "inflation", "wall street",
    "financial markets", "volatility", "dividends", "valuation",
    "price target", "IPO", "stock split", "ETF", "SPY",
]

# Get initial access and refresh tokens
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
        print("Access and refresh tokens obtained successfully.")
    else:
        print("Error obtaining initial tokens:", response.status_code, response.text)
        access_jwt = None
        refresh_jwt = None

# Refresh the access token using the refresh token
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
        print("Token successfully refreshed.")
    else:
        print(f"Error refreshing token: {response.status_code}, {response.text}")
        access_jwt = None

# Fetch posts from Bluesky API
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
        print(f"Error fetching posts for '{keyword}':", response.status_code)
        return []

# Main tracking function
def track_posts():
    global access_jwt

    # Merge static and DB keywords
    db_keywords = load_keywords_from_db()
    print(f"Loaded {len(db_keywords)} keywords from the database.")
    all_keywords = list(set(keywords + db_keywords))

    get_initial_tokens()

    if access_jwt is None:
        print("Cannot proceed without a valid accessJwt.")
        return

    last_token_renewal_time = time.time()
    
    while True:
        if access_jwt is None:
            print("Cannot proceed without a valid accessJwt.")
            break

        print("Searching for new posts...")

        for keyword in all_keywords:
            new_posts = fetch_posts(keyword)
            new_posts_filtered = [post for post in new_posts if post['cid'] not in processed_posts]

            if new_posts_filtered:
                print(f"Found {len(new_posts_filtered)} new posts for '{keyword}'.")

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
                print(f"No new posts found for '{keyword}'.")

        time.sleep(30)

        if time.time() - last_token_renewal_time > 900:
            get_new_access_token()
            last_token_renewal_time = time.time()

track_posts()