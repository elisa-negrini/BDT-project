# import streamlit as st
# st.set_page_config(page_title="📈 Real-Time Kafka Dashboard", layout="wide")

# from kafka import KafkaConsumer
# import json
# import pandas as pd
# import plotly.graph_objects as go
# from datetime import datetime, timedelta, timezone
# from streamlit_autorefresh import st_autorefresh

# # 🔁 Auto-refresh every 3 seconds
# st_autorefresh(interval=3000, key="refresh")

# # Available tickers (ensure this list matches the tickers your models are trained for)
# TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]
# selected_ticker = st.selectbox("Select a ticker:", TICKERS)

# # Kafka consumer (initialized once)
# @st.cache_resource
# def create_consumers():
#     consumer_prices = KafkaConsumer(
#         "stock_trades", # This topic remains the same for real-time price data
#         bootstrap_servers="kafka:9092",
#         value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#         auto_offset_reset="latest",
#         group_id="dashboard-consumer-prices"
#     )
#     consumer_predictions = KafkaConsumer(
#         "prediction", # <--- CHANGED: Now consuming from 'prediction' topic
#         bootstrap_servers="kafka:9092",
#         value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#         auto_offset_reset="latest",
#         group_id="dashboard-consumer-predictions"
#     )
#     return consumer_prices, consumer_predictions

# consumer_prices, consumer_predictions = create_consumers()

# # Data buffer in session state
# if "price_buffer" not in st.session_state:
#     st.session_state.price_buffer = {}
# if "prediction_buffer" not in st.session_state:
#     st.session_state.prediction_buffer = {}

# # Initialize buffers for the selected ticker if they don't exist
# if selected_ticker not in st.session_state.price_buffer:
#     st.session_state.price_buffer[selected_ticker] = []
# if selected_ticker not in st.session_state.prediction_buffer:
#     st.session_state.prediction_buffer[selected_ticker] = []

# # --- CONSUME PRICE TOPIC ---
# records = consumer_prices.poll(timeout_ms=100)
# for tp, messages in records.items():
#     for msg in messages:
#         data = msg.value
#         # Ensure 'ticker' and 'timestamp' keys exist before accessing
#         if "ticker" not in data or "timestamp" not in data or data["ticker"] != selected_ticker:
#             continue
#         try:
#             # Assuming timestamp from stock_trades is ISO format without timezone, then setting UTC
#             ts = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00")).astimezone(timezone.utc)
#             st.session_state.price_buffer[selected_ticker].append({
#                 "timestamp": ts,
#                 "price": float(data["price"])
#             })
#         except Exception as e:
#             st.error(f"Error processing price message: {e} - Data: {data}")
#             continue

# # --- CONSUME PREDICTIONS TOPIC ---
# records_pred = consumer_predictions.poll(timeout_ms=100)
# for tp, messages in records_pred.items():
#     for msg in messages:
#         data = msg.value
#         # Ensure 'ticker', 'timestamp', and 'prediction' keys exist before accessing
#         # The 'timestamp' key from the prediction service is the target_timestamp for the prediction
#         if "ticker" not in data or "timestamp" not in data or "prediction" not in data or data["ticker"] != selected_ticker:
#             continue
#         try:
#             # Timestamp from the prediction service is already ISO format and should be handled correctly
#             ts = datetime.fromisoformat(data["timestamp"]).astimezone(timezone.utc)
#             st.session_state.prediction_buffer[selected_ticker].append({
#                 "timestamp": ts,
#                 "predicted_price": float(data["prediction"]) # <--- CHANGED: Key is now 'prediction'
#             })
#         except Exception as e:
#             st.error(f"Error processing prediction message: {e} - Data: {data}")
#             continue

# # --- FILTER LAST 5 MINUTES OF DATA ---
# now = datetime.now(timezone.utc)
# cutoff = now - timedelta(minutes=5)

# filtered_prices = [
#     d for d in st.session_state.price_buffer[selected_ticker]
#     if d["timestamp"] >= cutoff
# ]
# filtered_predictions = [
#     d for d in st.session_state.prediction_buffer[selected_ticker]
#     if d["timestamp"] >= cutoff
# ]

# # --- PLOTLY CHART ---
# st.header(f"Real-Time Price & Prediction for {selected_ticker}")
# if not filtered_prices:
#     st.warning("Waiting for recent price data from Kafka...")
# else:
#     df_prices = pd.DataFrame(filtered_prices)
#     df_prices["timestamp"] = pd.to_datetime(df_prices["timestamp"])

#     fig = go.Figure()

#     # Real price line
#     fig.add_trace(go.Scatter(
#         x=df_prices["timestamp"],
#         y=df_prices["price"],
#         mode="lines+markers",
#         name="Real price",
#         line=dict(color="blue")
#     ))

#     # Prediction line
#     if filtered_predictions:
#         df_preds = pd.DataFrame(filtered_predictions)
#         df_preds["timestamp"] = pd.to_datetime(df_preds["timestamp"])

#         fig.add_trace(go.Scatter(
#             x=df_preds["timestamp"],
#             y=df_preds["predicted_price"],
#             mode="markers+lines",
#             name="Predicted price",
#             line=dict(color="red", dash="dash")
#         ))
#     else:
#         st.info("No recent predictions available for this ticker yet. Waiting for data...")


#     fig.update_layout(
#         title=f"Price and forecast: {selected_ticker}",
#         xaxis_title="Time (UTC)", # Explicitly state UTC
#         yaxis_title="Price ($)",
#         template="plotly_white",
#         hovermode="x unified" # Improves hover experience
#     )

#     st.plotly_chart(fig, use_container_width=True)















































# import streamlit as st
# st.set_page_config(page_title="📈 Real-Time Kafka Dashboard", layout="wide")

# from kafka import KafkaConsumer
# import json
# import pandas as pd
# import plotly.graph_objects as go
# from datetime import datetime, timedelta, timezone
# from streamlit_autorefresh import st_autorefresh

# # 🔁 Auto-refresh every 3 seconds
# st_autorefresh(interval=3000, key="refresh")

# # Available tickers (ensure this list matches the tickers your models are trained for)
# TICKERS = [
#     "AAPL", "MSFT", "NVDA", "AMZN", "META", "BRK.B", "GOOGL", "AVGO", "TSLA", "IBM",
#     "LLY", "JPM", "V", "XOM", "NFLX", "COST", "UNH", "JNJ", "PG", "MA",
#     "CVX", "MRK", "PEP", "ABBV", "ADBE", "WMT", "BAC", "HD", "KO", "TMO"
# ]
# selected_ticker = st.selectbox("Select a ticker:", TICKERS)

# # Kafka consumer (initialized once)
# @st.cache_resource
# def create_consumers():
#     consumer_prices = KafkaConsumer(
#         "stock_trades", # This topic remains the same for real-time price data
#         bootstrap_servers="kafka:9092",
#         value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#         auto_offset_reset="latest",
#         group_id="dashboard-consumer-prices"
#     )
#     consumer_predictions = KafkaConsumer(
#         "prediction", # <--- CHANGED: Now consuming from 'prediction' topic
#         bootstrap_servers="kafka:9092",
#         value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#         auto_offset_reset="latest",
#         group_id="dashboard-consumer-predictions"
#     )
#     return consumer_prices, consumer_predictions

# consumer_prices, consumer_predictions = create_consumers()

# # Data buffer in session state
# if "price_buffer" not in st.session_state:
#     st.session_state.price_buffer = {}
# if "prediction_buffer" not in st.session_state:
#     st.session_state.prediction_buffer = {}

# # Initialize buffers for the selected ticker if they don't exist
# if selected_ticker not in st.session_state.price_buffer:
#     st.session_state.price_buffer[selected_ticker] = []
# if selected_ticker not in st.session_state.prediction_buffer:
#     st.session_state.prediction_buffer[selected_ticker] = []

# # --- CONSUME PRICE TOPIC ---
# records = consumer_prices.poll(timeout_ms=100)
# for tp, messages in records.items():
#     for msg in messages:
#         data = msg.value
#         # Ensure 'ticker' and 'timestamp' keys exist before accessing
#         if "ticker" not in data or "timestamp" not in data or data["ticker"] != selected_ticker:
#             continue
#         try:
#             # Assuming timestamp from stock_trades is ISO format without timezone, then setting UTC
#             ts = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00")).astimezone(timezone.utc)
#             st.session_state.price_buffer[selected_ticker].append({
#                 "timestamp": ts,
#                 "price": float(data["price"])
#             })
#         except Exception as e:
#             st.error(f"Error processing price message: {e} - Data: {data}")
#             continue

# # --- CONSUME PREDICTIONS TOPIC ---
# records_pred = consumer_predictions.poll(timeout_ms=100)
# for tp, messages in records_pred.items():
#     for msg in messages:
#         data = msg.value
#         # Ensure 'ticker', 'timestamp', and 'prediction' keys exist before accessing
#         # The 'timestamp' key from the prediction service is the target_timestamp for the prediction
#         if "ticker" not in data or "timestamp" not in data or "prediction" not in data or data["ticker"] != selected_ticker:
#             continue
#         try:
#             # Timestamp from the prediction service is already ISO format and should be handled correctly
#             ts = datetime.fromisoformat(data["timestamp"]).astimezone(timezone.utc)
#             st.session_state.prediction_buffer[selected_ticker].append({
#                 "timestamp": ts,
#                 "predicted_price": float(data["prediction"]) # <--- CHANGED: Key is now 'prediction'
#             })
#         except Exception as e:
#             st.error(f"Error processing prediction message: {e} - Data: {data}")
#             continue

# # --- FILTER LAST 5 MINUTES OF DATA ---
# now = datetime.now(timezone.utc)
# cutoff = now - timedelta(minutes=5)

# filtered_prices = [
#     d for d in st.session_state.price_buffer[selected_ticker]
#     if d["timestamp"] >= cutoff
# ]
# filtered_predictions = [
#     d for d in st.session_state.prediction_buffer[selected_ticker]
#     if d["timestamp"] >= cutoff
# ]

# # --- PLOTLY CHART ---
# st.header(f"Real-Time Price & Prediction for {selected_ticker}")
# if not filtered_prices:
#     st.warning("Waiting for recent price data from Kafka...")
# else:
#     df_prices = pd.DataFrame(filtered_prices)
#     df_prices["timestamp"] = pd.to_datetime(df_prices["timestamp"])

#     fig = go.Figure()

#     # Real price line
#     fig.add_trace(go.Scatter(
#         x=df_prices["timestamp"],
#         y=df_prices["price"],
#         mode="lines+markers",
#         name="Real price",
#         line=dict(color="blue")
#     ))

#     # Prediction line
#     if filtered_predictions:
#         df_preds = pd.DataFrame(filtered_predictions)
#         df_preds["timestamp"] = pd.to_datetime(df_preds["timestamp"])

#         fig.add_trace(go.Scatter(
#             x=df_preds["timestamp"],
#             y=df_preds["predicted_price"],
#             mode="markers+lines",
#             name="Predicted price",
#             line=dict(color="red", dash="dash")
#         ))
#     else:
#         st.info("No recent predictions available for this ticker yet. Waiting for data...")


#     fig.update_layout(
#         title=f"Price and forecast: {selected_ticker}",
#         xaxis_title="Time (UTC)", # Explicitly state UTC
#         yaxis_title="Price ($)",
#         template="plotly_white",
#         hovermode="x unified" # Improves hover experience
#     )

#     st.plotly_chart(fig, use_container_width=True)































import streamlit as st
st.set_page_config(page_title="Real-Time Kafka Dashboard", layout="wide")

from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh
import pytz # Import pytz for timezone conversion

# Auto-refresh every 3 seconds
st_autorefresh(interval=3000, key="refresh")

# Available tickers and their corresponding names
TICKERS_DATA = {
    "AAPL": "Apple", "MSFT": "Microsoft", "NVDA": "Nvidia", "AMZN": "Amazon", "META": "Meta",
    "ORCL": "Oracle", "GOOGL": "Google", "AVGO": "Broadcom", "TSLA": "Tesla", "IBM": "Internation Business Machine",
    "LLY": "Eli Lilly", "JPM": "JP Morgan", "V": "Visa", "XOM": "Exxon", "NFLX": "Netflix",
    "COST": "Costco", "UNH": "UnitedHealth", "JNJ": "Johnson & Jonson", "PG": "Procter & Gamble",
    "MA": "Mastercard", "CVX": "Chevron", "MRK": "Merck", "PEP": "Pepsi", "ABBV": "AbbVie",
    "ADBE": "Adobe", "WMT": "Walmart", "BAC": "Bank of America", "HD": "Home Depot", "KO": "Coca-Cola",
    "TMO": "Thermo Fisher"
}

# Create a mapping from name to ticker for the selectbox
NAME_TO_TICKER = {name: ticker for ticker, name in TICKERS_DATA.items()}
DISPLAY_NAMES = list(NAME_TO_TICKER.keys())

# Select by name, then get the corresponding ticker
selected_name = st.selectbox("Select a company:", DISPLAY_NAMES)
selected_ticker = NAME_TO_TICKER[selected_name]

# Kafka consumer (initialized once)
@st.cache_resource
def create_consumers():
    # Consumer for real-time stock price data
    consumer_prices = KafkaConsumer(
        "stock_trades",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-consumer-prices"
    )
    # Consumer for stock price predictions
    consumer_predictions = KafkaConsumer(
        "prediction",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-consumer-predictions"
    )
    return consumer_prices, consumer_predictions

consumer_prices, consumer_predictions = create_consumers()

# Data buffer in session state to store prices and predictions
if "price_buffer" not in st.session_state:
    st.session_state.price_buffer = {}
if "prediction_buffer" not in st.session_state:
    st.session_state.prediction_buffer = {}
# Buffer to store the exchange source for each ticker
if "exchange_status" not in st.session_state:
    st.session_state.exchange_status = {}
# Buffer to store the opening price for each ticker and its date
if "opening_price_data" not in st.session_state:
    st.session_state.opening_price_data = {} # Stores {'ticker': {'price': value, 'date': datetime.date}}


# Define New York timezone
ny_timezone = pytz.timezone('America/New_York')
utc_timezone = timezone.utc

# Initialize buffers for ALL tickers if they don't exist
for ticker in TICKERS_DATA.keys():
    if ticker not in st.session_state.price_buffer:
        st.session_state.price_buffer[ticker] = []
    if ticker not in st.session_state.prediction_buffer:
        st.session_state.prediction_buffer[ticker] = []
    if ticker not in st.session_state.exchange_status:
        st.session_state.exchange_status[ticker] = "N/A" # Default status
    if ticker not in st.session_state.opening_price_data:
        st.session_state.opening_price_data[ticker] = {'price': None, 'date': None} # Initialize opening price and its date


# --- CONSUME PRICE TOPIC ---
records = consumer_prices.poll(timeout_ms=100)
for tp, messages in records.items():
    for msg in messages:
        data = msg.value
        if "ticker" not in data or "timestamp" not in data or "exchange" not in data:
            continue

        current_message_ticker = data["ticker"]

        try:
            st.session_state.exchange_status[current_message_ticker] = data["exchange"]
            ts_utc = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00")).astimezone(utc_timezone)
            price = float(data["price"])

            # Convert current message timestamp to New York time for daily comparison
            ts_ny = ts_utc.astimezone(ny_timezone)
            current_day_ny = ts_ny.date()

            # --- Opening Price Logic ---
            opening_data = st.session_state.opening_price_data[current_message_ticker]

            # If it's a new day, or no opening price set yet, reset/capture
            if opening_data['date'] is None or opening_data['date'] < current_day_ny:
                # Reset for a new day
                opening_data['price'] = None
                opening_data['date'] = None

                # Check if the current message is exactly 9:30 AM ET
                # Note: This assumes precise timestamps from Kafka. Real-world data might be slightly off.
                if ts_ny.hour == 9 and ts_ny.minute == 30 and ts_ny.second == 0 and ts_ny.microsecond == 0:
                    opening_data['price'] = price
                    opening_data['date'] = current_day_ny
                # If not 9:30 AM yet (or if 9:30 AM has passed and we missed it), take the first available
                # This ensures we always have an opening price for the day once data starts flowing
                elif opening_data['price'] is None: 
                    opening_data['price'] = price
                    opening_data['date'] = current_day_ny 
                                                        
            st.session_state.price_buffer[current_message_ticker].append({
                "timestamp": ts_utc, # Store in UTC
                "price": price
            })
        except Exception as e:
            st.error(f"Error processing price message: {e} - Data: {data}")
            continue

# --- CONSUME PREDICTIONS TOPIC ---
records_pred = consumer_predictions.poll(timeout_ms=100)
for tp, messages in records_pred.items(): # CAMBIA records IN records_pred
    for msg in messages:
        data = msg.value
        if "ticker" not in data or "timestamp" not in data or "prediction" not in data:
            continue

        current_message_ticker = data["ticker"]

        try:
            ts_utc = datetime.fromisoformat(data["timestamp"]).astimezone(utc_timezone)
            
            st.session_state.prediction_buffer[current_message_ticker].append({
                "timestamp": ts_utc,
                "predicted_price": float(data["prediction"])
            })
        except Exception as e:
            st.error(f"Error processing prediction message: {e} - Data: {data}")
            continue

# --- CLEAN UP OLD DATA IN BUFFERS (KEEP LAST 10 MINUTES) ---
now_utc = datetime.now(utc_timezone) 
cleanup_cutoff_utc = now_utc - timedelta(minutes=10) 

for ticker in st.session_state.price_buffer:
    st.session_state.price_buffer[ticker] = [
        d for d in st.session_state.price_buffer[ticker] if d["timestamp"] >= cleanup_cutoff_utc
    ]

for ticker in st.session_state.prediction_buffer:
    st.session_state.prediction_buffer[ticker] = [
        d for d in st.session_state.prediction_buffer[ticker] if d["timestamp"] >= cleanup_cutoff_utc
    ]

# --- FILTER LAST 5 MINUTES OF DATA FOR DISPLAY AND CONVERT TO NEW YORK TIMEZONE ---
display_cutoff_utc = now_utc - timedelta(minutes=5)

filtered_prices = []
for d in st.session_state.price_buffer.get(selected_ticker, []):
    if d["timestamp"] >= display_cutoff_utc:
        d["timestamp_ny"] = d["timestamp"].astimezone(ny_timezone)
        filtered_prices.append(d)

filtered_predictions = []
for d in st.session_state.prediction_buffer.get(selected_ticker, []):
    if d["timestamp"] >= display_cutoff_utc:
        d["timestamp_ny"] = d["timestamp"].astimezone(ny_timezone)
        filtered_predictions.append(d)

# --- DISPLAY HEADER BASED ON EXCHANGE STATUS ---
current_exchange_status = st.session_state.exchange_status.get(selected_ticker, "N/A")

if current_exchange_status == "RANDOM":
    st.header(f"Real-Time Price & Prediction for {selected_name} \n Simulated Data")
else:
    st.header(f"Real-Time Price & Prediction for {selected_name}")

# --- PLOTLY CHART ---
if not filtered_prices:
    st.warning("Waiting for recent price data...")
else:
    df_prices = pd.DataFrame(filtered_prices)
    df_prices["timestamp_ny"] = pd.to_datetime(df_prices["timestamp_ny"])

    fig = go.Figure()

    # Determine the name for the real price line based on exchange status
    real_price_line_name = "Real Price"
    if current_exchange_status == "RANDOM":
        real_price_line_name = "Simulated Price"

    # Real price line
    fig.add_trace(go.Scatter(
        x=df_prices["timestamp_ny"],
        y=df_prices["price"],
        mode="lines+markers",
        name=real_price_line_name,
        line=dict(color="blue"),
        marker=dict(size=5)
    ))

    # Prediction line
    if filtered_predictions: # Questo blocco è stato ripristinato e funziona correttamente
        df_preds = pd.DataFrame(filtered_predictions)
        df_preds["timestamp_ny"] = pd.to_datetime(df_preds["timestamp_ny"])

        fig.add_trace(go.Scatter(
            x=df_preds["timestamp_ny"],
            y=df_preds["predicted_price"], # Utilizza la chiave 'predicted_price'
            mode="markers+lines",
            name="Predicted Price",
            line=dict(color="red", dash="dash"),
            marker=dict(size=5)
        ))
    else:
        st.info("No recent predictions available for this company yet.")

    # --- ADD OPENING PRICE LINE ---
    opening_price_value = st.session_state.opening_price_data.get(selected_ticker, {}).get('price')
    opening_price_date = st.session_state.opening_price_data.get(selected_ticker, {}).get('date')

    if opening_price_value is not None:
        fig.add_hline(
            y=opening_price_value,
            line_dash="dash",
            line_color="grey",
            annotation_text=f"Opening Price ({opening_price_date.strftime('%Y-%m-%d')}): ${opening_price_value:.2f}",
            annotation_position="bottom left",
            annotation_font_color="grey"
        )

    fig.update_layout(
        title=f"Price and Forecast: {selected_name} ({selected_ticker})",
        xaxis_title="Time (New York)",
        yaxis_title="Price ($)",
        template="plotly_white",
        hovermode="x unified"
    )

    st.plotly_chart(fig, use_container_width=True)

   # --- DISPLAY OPENING PRICE TEXT WITH PERCENTAGE DIFFERENCE ---
    if st.session_state.opening_price_data[selected_ticker]['price'] is not None:
        opening_price = st.session_state.opening_price_data[selected_ticker]['price']
        display_date = st.session_state.opening_price_data[selected_ticker]['date']
        
        # Get the current (most recent) price
        current_price = filtered_prices[-1]["price"] if filtered_prices else None

        if current_price is not None and opening_price != 0: # Avoid division by zero
            price_diff = current_price - opening_price # Calcola la differenza assoluta
            percentage_diff = ((current_price - opening_price) / opening_price) * 100
            
            # Determine color based on percentage_diff
            color = "green" if percentage_diff >= 0 else "red"
            sign_per = "+" if percentage_diff >= 0 else ""
            sign_diff = "+" if percentage_diff >= 0 else "-" 

            # Define desired font sizes
            text_size = "14px"      # For "Opening Price (data):"
            number_size = "22px"    # For the numbers (opening price, difference, percentage)


            # Construct the full HTML string with custom sizes and bolding for all numbers
            full_display_text = \
                f"<span style='font-size: {text_size};'>Opening Price ({display_date.strftime('%Y-%m-%d')}): </span>" \
                f"<span style='font-size: {number_size};'><strong>{opening_price:.2f}$</strong></span> " \
                f"<span style='font-size: {number_size}; color:{color};'><strong>{sign_diff}{abs(price_diff):.2f}</strong></span> " \
                f"<span style='font-size: {number_size}; color:{color};'><strong>({sign_per}{percentage_diff:.2f}%)</strong></span>"

            # Use unsafe_allow_html=True to render the HTML
            st.write(full_display_text, unsafe_allow_html=True)
            
        else:
            # For consistency, also apply styles here, including bold for the opening price
            text_size = "14px"
            number_size = "22px"
            st.write(f"<span style='font-size: {text_size};'>Opening Price ({display_date.strftime('%Y-%m-%d')}): </span>" \
                     f"<span style='font-size: {number_size};'><strong>{opening_price:.2f}$</strong></span>", unsafe_allow_html=True)
            st.info("Waiting for current price to calculate percentage difference.")
    else:
        # And here, if you want to style the waiting message
        text_size = "14px"
        st.info(f"<span style='font-size: {text_size};'>Waiting for the opening price for today's trading.</span>", unsafe_allow_html=True)
