import requests
import time
import json
from datetime import datetime
from kafka import KafkaProducer
import os
import psycopg2

# --- Configuration ---

# Bluesky API authentication tokens.
# These will be updated dynamically.
access_jwt = ""
refresh_jwt = ""

# Bluesky user credentials. Set as environment variables.
identifier = os.getenv("BLUESKY_IDENTIFIER")
password = os.getenv("BLUESKY_PASSWORD")

# Bluesky API URL for searching posts.
SEARCH_POSTS_URL = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
CREATE_SESSION_URL = "https://bsky.social/xrpc/com.atproto.server.createSession"
REFRESH_SESSION_URL = "https://bsky.social/xrpc/com.atproto.server.refreshSession"

# Kafka broker and topic.
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = 'bluesky'

# PostgreSQL database connection details.
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# --- Kafka Operations ---

def connect_kafka():
    """Establishes and returns a Kafka producer, with retry logic."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka producer connection established.")
            return producer
        except Exception as e:
            print(f"Kafka unavailable, retrying in 5 seconds... ({e})")
            time.sleep(5)

# Initialize Kafka producer.
producer = connect_kafka()

# Set to track already processed post IDs to avoid duplicates.
processed_posts = set()

# --- Database Operations ---

def load_keywords_from_db():
    """
    Loads distinct keywords (tickers, company names, related words) from the database.
    Includes active companies only.
    """
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT, database=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD
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
                if field and isinstance(field, str) and len(field.strip()) > 1:
                    db_keywords.add(field.strip())
        return list(db_keywords)

    except Exception as e:
        print(f"Error fetching keywords from database: {e}")
        return []

# Static keyword list for general financial terms.
STATIC_KEYWORDS = [
    "stock", "stocks", "market", "markets", "equity", "equities",
    "trading", "investing", "investment", "portfolio", "bull market",
    "bear market", "recession", "nasdaq", "dow jones", "s&p 500",
    "earnings", "earnings report", "buy the dip", "short selling",
    "federal reserve", "interest rates", "inflation", "wall street",
    "financial markets", "volatility", "dividends", "valuation",
    "price target", "IPO", "stock split", "ETF", "SPY",
]

# --- Bluesky Authentication ---

def get_initial_tokens():
    """Authenticates with Bluesky to obtain initial access and refresh tokens."""
    global access_jwt, refresh_jwt
    payload = {
        "identifier": identifier,
        "password": password
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(CREATE_SESSION_URL, headers=headers, json=payload)

    if response.status_code == 200:
        token_data = response.json()
        access_jwt = token_data['accessJwt']
        refresh_jwt = token_data['refreshJwt']
        print("Access and refresh tokens obtained successfully.")
    else:
        print(f"Error obtaining initial tokens: {response.status_code}, {response.text}")
        access_jwt = None
        refresh_jwt = None

def get_new_access_token():
    """Refreshes the access token using the current refresh token."""
    global access_jwt, refresh_jwt
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {refresh_jwt}"
    }
    response = requests.post(REFRESH_SESSION_URL, headers=headers)

    if response.status_code == 200:
        token_data = response.json()
        access_jwt = token_data['accessJwt']
        refresh_jwt = token_data['refreshJwt']
        print("Token successfully refreshed.")
    else:
        print(f"Error refreshing token: {response.status_code}, {response.text}")
        access_jwt = None

# --- Bluesky Data Fetching ---

def fetch_posts(keyword):
    """Fetches posts from the Bluesky API for a given keyword."""
    headers = {
        'Authorization': f'Bearer {access_jwt}'
    }
    params = {
        'q': keyword
    }
    response = requests.get(SEARCH_POSTS_URL, headers=headers, params=params)
    if response.status_code == 200:
        return response.json().get('posts', [])
    else:
        print(f"Error fetching posts for '{keyword}': {response.status_code}")
        return []

# --- Main Tracking Function ---

def track_posts():
    """
    Main loop to track Bluesky posts, merge keywords, and send new posts to Kafka.
    Handles token refreshing every 15 minutes.
    """
    global access_jwt

    # Load and merge keywords.
    db_keywords = load_keywords_from_db()
    print(f"Loaded {len(db_keywords)} keywords from the database.")
    all_keywords = list(set(STATIC_KEYWORDS + db_keywords))

    # Obtain initial authentication tokens.
    get_initial_tokens()

    if access_jwt is None:
        print("Cannot proceed without a valid accessJwt.")
        return

    last_token_renewal_time = time.time()

    while True:
        if access_jwt is None:
            print("Cannot proceed without a valid accessJwt. Re-attempting login.")
            get_initial_tokens() # Attempt to get new tokens if current ones are invalid
            if access_jwt is None:
                time.sleep(60) # Wait before retrying
                continue

        print("Searching for new posts...")

        for keyword in all_keywords:
            new_posts = fetch_posts(keyword)
            # Filter out already processed posts.
            new_posts_filtered = [post for post in new_posts if post['cid'] not in processed_posts]

            if new_posts_filtered:
                print(f"Found {len(new_posts_filtered)} new posts for '{keyword}'.")
                for post in new_posts_filtered:
                    processed_posts.add(post['cid'])
                    data = {
                        "id": post.get('cid'),
                        "text": post.get('record', {}).get('text'),
                        "created_at": post.get('record', {}).get('createdAt'),
                        "keyword": keyword
                    }
                    print(json.dumps(data, ensure_ascii=False))
                    producer.send(KAFKA_TOPIC, value=data)
            else:
                print(f"No new posts found for '{keyword}'.")

        time.sleep(30) # Wait before the next search iteration.

        # Refresh token every 15 minutes (900 seconds).
        if time.time() - last_token_renewal_time > 900:
            get_new_access_token()
            last_token_renewal_time = time.time()

# Start the post tracking process.
track_posts()