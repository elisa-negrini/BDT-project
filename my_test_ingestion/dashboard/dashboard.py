import streamlit as st
st.set_page_config(page_title="📈 Real-Time Kafka Dashboard", layout="wide")

from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# 🔁 Auto-refresh ogni 3 secondi
st_autorefresh(interval=3000, key="refresh")

# Tickers disponibili
TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
    "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
    "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
]
selected_ticker = st.selectbox("📊 Seleziona un ticker:", TICKERS)

# Kafka consumer (una sola volta)
@st.cache_resource
def create_consumers():
    consumer_prices = KafkaConsumer(
        "stock_trades",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-consumer-prices"
    )
    consumer_predictions = KafkaConsumer(
        "prediction",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-consumer-predictions"
    )
    return consumer_prices, consumer_predictions

consumer_prices, consumer_predictions = create_consumers()

# Buffer dati in sessione
if "price_buffer" not in st.session_state:
    st.session_state.price_buffer = {}
if "prediction_buffer" not in st.session_state:
    st.session_state.prediction_buffer = {}

if selected_ticker not in st.session_state.price_buffer:
    st.session_state.price_buffer[selected_ticker] = []
if selected_ticker not in st.session_state.prediction_buffer:
    st.session_state.prediction_buffer[selected_ticker] = []

# === CONSUMO TOPIC PREZZI ===
records = consumer_prices.poll(timeout_ms=100)
for tp, messages in records.items():
    for msg in messages:
        data = msg.value
        if data["ticker"] != selected_ticker:
            continue
        try:
            ts = datetime.fromisoformat(data["timestamp"].replace(" ", "T"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            st.session_state.price_buffer[selected_ticker].append({
                "timestamp": ts,
                "price": float(data["price"])
            })
        except:
            continue

# === CONSUMO TOPIC PREVISIONI ===
records_pred = consumer_predictions.poll(timeout_ms=100)
for tp, messages in records_pred.items():
    for msg in messages:
        data = msg.value
        if data["ticker"] != selected_ticker:
            continue
        try:
            ts = datetime.fromisoformat(data["target_timestamp"].replace(" ", "T"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            st.session_state.prediction_buffer[selected_ticker].append({
                "timestamp": ts,
                "predicted_price": float(data["predicted_price"])
            })
        except:
            continue

# === FILTRA ULTIMI 5 MINUTI ===
now = datetime.now(timezone.utc)
cutoff = now - timedelta(minutes=5)

filtered_prices = [
    d for d in st.session_state.price_buffer[selected_ticker]
    if d["timestamp"] >= cutoff
]
filtered_predictions = [
    d for d in st.session_state.prediction_buffer[selected_ticker]
    if d["timestamp"] >= cutoff
]

# === GRAFICO ===
if not filtered_prices:
    st.warning("In attesa di dati recenti da Kafka...")
else:
    df_prices = pd.DataFrame(filtered_prices)
    df_prices["timestamp"] = pd.to_datetime(df_prices["timestamp"])

    fig = go.Figure()

    # Linea prezzi reali
    fig.add_trace(go.Scatter(
        x=df_prices["timestamp"],
        y=df_prices["price"],
        mode="lines+markers",
        name="Prezzo reale",
        line=dict(color="blue")
    ))

    # Linea previsioni
    if filtered_predictions:
        df_preds = pd.DataFrame(filtered_predictions)
        df_preds["timestamp"] = pd.to_datetime(df_preds["timestamp"])

        fig.add_trace(go.Scatter(
            x=df_preds["timestamp"],
            y=df_preds["predicted_price"],
            mode="markers+lines",
            name="Prezzo previsto",
            line=dict(color="red", dash="dash")
        ))

    fig.update_layout(
        title=f"📈 Prezzo e previsioni: {selected_ticker}",
        xaxis_title="Orario",
        yaxis_title="Prezzo ($)",
        template="plotly_white"
    )

    st.plotly_chart(fig, use_container_width=True)












# import streamlit as st
# st.set_page_config(page_title="📈 Real-Time Kafka Dashboard", layout="wide")

# from kafka import KafkaConsumer
# import json
# import pandas as pd
# import plotly.graph_objects as go
# from datetime import datetime, timedelta, timezone
# from streamlit_autorefresh import st_autorefresh

# # 🔁 Auto-refresh ogni 3 secondi
# st_autorefresh(interval=3000, key="refresh")

# # Tickers disponibili
# tickers = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]
# selected_ticker = st.selectbox("📊 Seleziona un ticker:", TICKERS)

# # Init del consumer (una sola volta per sessione)
# @st.cache_resource
# def create_consumer():
#     return KafkaConsumer(
#         "stock_trades",
#         bootstrap_servers="kafka:9092",
#         value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#         auto_offset_reset="latest",
#         group_id="dashboard-consumer"
#     )

# consumer = create_consumer()

# # Inizializza buffer dati
# if "buffer" not in st.session_state:
#     st.session_state.buffer = {}

# if selected_ticker not in st.session_state.buffer:
#     st.session_state.buffer[selected_ticker] = []

# # Poll da Kafka (non blocca)
# records = consumer.poll(timeout_ms=100)

# # Elabora i messaggi ricevuti
# for tp, messages in records.items():
#     for msg in messages:
#         data = msg.value
#         if data["ticker"] != selected_ticker:
#             continue

#         try:
#             ts_str = data["timestamp"].replace(" ", "T")
#             ts = datetime.fromisoformat(ts_str)
#             if ts.tzinfo is None:
#                 ts = ts.replace(tzinfo=timezone.utc)
#         except:
#             continue  # skip se timestamp malformato

#         st.session_state.buffer[selected_ticker].append({
#             "timestamp": ts,
#             "price": float(data["price"])
#         })

# # Filtro: ultimi 5 minuti
# now = datetime.now(timezone.utc)
# cutoff = now - timedelta(minutes=5)
# data_filtered = [
#     d for d in st.session_state.buffer[selected_ticker]
#     if d["timestamp"] >= cutoff
# ]

# # Mostra messaggio se non ci sono dati
# if not data_filtered:
#     st.warning("In attesa di dati recenti da Kafka...")
# else:
#     df = pd.DataFrame(data_filtered)
#     df["timestamp"] = pd.to_datetime(df["timestamp"])

#     fig = go.Figure()
#     fig.add_trace(go.Scatter(
#         x=df["timestamp"],
#         y=df["price"],
#         mode="lines+markers",
#         name=selected_ticker,
#         line=dict(color="blue")
#     ))

#     fig.update_layout(
#         title=f"📈 Prezzo in tempo reale: {selected_ticker}",
#         xaxis_title="Orario",
#         yaxis_title="Prezzo ($)",
#         template="plotly_white"
#     )

#     st.plotly_chart(fig, use_container_width=True)


