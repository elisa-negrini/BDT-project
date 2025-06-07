# import streamlit as st
# st.set_page_config(page_title="Real-Time Kafka Dashboard", layout="wide")

# from kafka import KafkaConsumer
# import json
# import pandas as pd
# import plotly.graph_objects as go
# from datetime import datetime, timedelta, timezone
# from streamlit_autorefresh import st_autorefresh
# import pytz # Import pytz for timezone conversion


# # Auto-refresh every 3 seconds
# st_autorefresh(interval=3000, key="refresh")

# # Available tickers and their corresponding names
# TICKERS_DATA = {
#     "AAPL": "Apple", "MSFT": "Microsoft", "NVDA": "Nvidia", "AMZN": "Amazon", "META": "Meta",
#     "ORCL": "Oracle", "GOOGL": "Google", "AVGO": "Broadcom", "TSLA": "Tesla", "IBM": "Internation Business Machine",
#     "LLY": "Eli Lilly", "JPM": "JP Morgan", "V": "Visa", "XOM": "Exxon", "NFLX": "Netflix",
#     "COST": "Costco", "UNH": "UnitedHealth", "JNJ": "Johnson & Jonson", "PG": "Procter & Gamble",
#     "MA": "Mastercard", "CVX": "Chevron", "MRK": "Merck", "PEP": "Pepsi", "ABBV": "AbbVie",
#     "ADBE": "Adobe", "WMT": "Walmart", "BAC": "Bank of America", "HD": "Home Depot", "KO": "Coca-Cola",
#     "TMO": "Thermo Fisher"
# }

# # Create a mapping from name to ticker for the selectbox
# NAME_TO_TICKER = {name: ticker for ticker, name in TICKERS_DATA.items()}
# DISPLAY_NAMES = list(NAME_TO_TICKER.keys())

# # Select by name, then get the corresponding ticker
# selected_name = st.selectbox("Select a company:", DISPLAY_NAMES)
# selected_ticker = NAME_TO_TICKER[selected_name]

# # Kafka consumer (initialized once)
# @st.cache_resource
# def create_consumers():
#     # Consumer for real-time stock price data
#     consumer_prices = KafkaConsumer(
#         "stock_trades",
#         bootstrap_servers="kafka:9092",
#         value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#         auto_offset_reset="latest",
#         group_id="dashboard-consumer-prices"
#     )
#     # Consumer for stock price predictions
#     consumer_predictions = KafkaConsumer(
#         "prediction",
#         bootstrap_servers="kafka:9092",
#         value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#         auto_offset_reset="latest",
#         group_id="dashboard-consumer-predictions"
#     )
#     return consumer_prices, consumer_predictions

# consumer_prices, consumer_predictions = create_consumers()

# # Data buffer in session state to store prices and predictions
# if "price_buffer" not in st.session_state:
#     st.session_state.price_buffer = {}
# if "prediction_buffer" not in st.session_state:
#     st.session_state.prediction_buffer = {}
# # Buffer to store the exchange source for each ticker
# if "exchange_status" not in st.session_state:
#     st.session_state.exchange_status = {}
# # Buffer to store the opening price for each ticker and its date
# if "opening_price_data" not in st.session_state:
#     st.session_state.opening_price_data = {} # Stores {'ticker': {'price': value, 'date': datetime.date}}


# # Define New York timezone
# ny_timezone = pytz.timezone('America/New_York')
# utc_timezone = timezone.utc

# # Initialize buffers for ALL tickers if they don't exist
# for ticker in TICKERS_DATA.keys():
#     if ticker not in st.session_state.price_buffer:
#         st.session_state.price_buffer[ticker] = []
#     if ticker not in st.session_state.prediction_buffer:
#         st.session_state.prediction_buffer[ticker] = []
#     if ticker not in st.session_state.exchange_status:
#         st.session_state.exchange_status[ticker] = "N/A" # Default status
#     if ticker not in st.session_state.opening_price_data:
#         st.session_state.opening_price_data[ticker] = {'price': None, 'date': None} # Initialize opening price and its date


# # --- CONSUME PRICE TOPIC ---
# records = consumer_prices.poll(timeout_ms=100)
# for tp, messages in records.items():
#     for msg in messages:
#         data = msg.value
#         if "ticker" not in data or "timestamp" not in data or "exchange" not in data:
#             continue

#         current_message_ticker = data["ticker"]

#         try:
#             st.session_state.exchange_status[current_message_ticker] = data["exchange"]
#             ts_utc = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00")).astimezone(utc_timezone)
#             price = float(data["price"])

#             # Convert current message timestamp to New York time for daily comparison
#             ts_ny = ts_utc.astimezone(ny_timezone)
#             current_day_ny = ts_ny.date()

#             # --- Opening Price Logic ---
#             opening_data = st.session_state.opening_price_data[current_message_ticker]

#             # If it's a new day, or no opening price set yet, reset/capture
#             if opening_data['date'] is None or opening_data['date'] < current_day_ny:
#                 # Reset for a new day
#                 opening_data['price'] = None
#                 opening_data['date'] = None

#                 # Check if the current message is exactly 9:30 AM ET
#                 # Note: This assumes precise timestamps from Kafka. Real-world data might be slightly off.
#                 if ts_ny.hour == 9 and ts_ny.minute == 30 and ts_ny.second == 0 and ts_ny.microsecond == 0:
#                     opening_data['price'] = price
#                     opening_data['date'] = current_day_ny
#                 # If not 9:30 AM yet (or if 9:30 AM has passed and we missed it), take the first available
#                 # This ensures we always have an opening price for the day once data starts flowing
#                 elif opening_data['price'] is None: 
#                     opening_data['price'] = price
#                     opening_data['date'] = current_day_ny 
                                                        
#             st.session_state.price_buffer[current_message_ticker].append({
#                 "timestamp": ts_utc, # Store in UTC
#                 "price": price
#             })
#         except Exception as e:
#             st.error(f"Error processing price message: {e} - Data: {data}")
#             continue

# # --- CONSUME PREDICTIONS TOPIC ---
# records_pred = consumer_predictions.poll(timeout_ms=100)
# for tp, messages in records_pred.items(): # CAMBIA records IN records_pred
#     for msg in messages:
#         data = msg.value
#         if "ticker" not in data or "timestamp" not in data or "prediction" not in data:
#             continue

#         current_message_ticker = data["ticker"]

#         try:
#             ts_utc = datetime.fromisoformat(data["timestamp"]).astimezone(utc_timezone)
            
#             st.session_state.prediction_buffer[current_message_ticker].append({
#                 "timestamp": ts_utc,
#                 "predicted_price": float(data["prediction"])
#             })
#         except Exception as e:
#             st.error(f"Error processing prediction message: {e} - Data: {data}")
#             continue

# # --- CLEAN UP OLD DATA IN BUFFERS (KEEP LAST 10 MINUTES) ---
# now_utc = datetime.now(utc_timezone) 
# cleanup_cutoff_utc = now_utc - timedelta(minutes=10) 

# for ticker in st.session_state.price_buffer:
#     st.session_state.price_buffer[ticker] = [
#         d for d in st.session_state.price_buffer[ticker] if d["timestamp"] >= cleanup_cutoff_utc
#     ]

# for ticker in st.session_state.prediction_buffer:
#     st.session_state.prediction_buffer[ticker] = [
#         d for d in st.session_state.prediction_buffer[ticker] if d["timestamp"] >= cleanup_cutoff_utc
#     ]

# # --- FILTER LAST 5 MINUTES OF DATA FOR DISPLAY AND CONVERT TO NEW YORK TIMEZONE ---
# display_cutoff_utc = now_utc - timedelta(minutes=5)

# filtered_prices = []
# for d in st.session_state.price_buffer.get(selected_ticker, []):
#     if d["timestamp"] >= display_cutoff_utc:
#         d["timestamp_ny"] = d["timestamp"].astimezone(ny_timezone)
#         filtered_prices.append(d)

# filtered_predictions = []
# for d in st.session_state.prediction_buffer.get(selected_ticker, []):
#     if d["timestamp"] >= display_cutoff_utc:
#         d["timestamp_ny"] = d["timestamp"].astimezone(ny_timezone)
#         filtered_predictions.append(d)

# # --- DISPLAY HEADER BASED ON EXCHANGE STATUS ---
# current_exchange_status = st.session_state.exchange_status.get(selected_ticker, "N/A")

# if current_exchange_status == "RANDOM":
#     st.header(f"Real-Time Price & Prediction for {selected_name} \n Simulated Data")
# else:
#     st.header(f"Real-Time Price & Prediction for {selected_name}")

# # --- PLOTLY CHART ---
# if not filtered_prices:
#     st.warning("Waiting for recent price data...")
# else:
#     df_prices = pd.DataFrame(filtered_prices)
#     df_prices["timestamp_ny"] = pd.to_datetime(df_prices["timestamp_ny"])

#     fig = go.Figure()

#     # Determine the name for the real price line based on exchange status
#     real_price_line_name = "Real Price"
#     if current_exchange_status == "RANDOM":
#         real_price_line_name = "Simulated Price"

#     # Real price line
#     fig.add_trace(go.Scatter(
#         x=df_prices["timestamp_ny"],
#         y=df_prices["price"],
#         mode="lines+markers",
#         name=real_price_line_name,
#         line=dict(color="blue"),
#         marker=dict(size=5)
#     ))

#     # Prediction line
#     if filtered_predictions: # Questo blocco è stato ripristinato e funziona correttamente
#         df_preds = pd.DataFrame(filtered_predictions)
#         df_preds["timestamp_ny"] = pd.to_datetime(df_preds["timestamp_ny"])

#         fig.add_trace(go.Scatter(
#             x=df_preds["timestamp_ny"],
#             y=df_preds["predicted_price"], # Utilizza la chiave 'predicted_price'
#             mode="markers+lines",
#             name="Predicted Price",
#             line=dict(color="red", dash="dash"),
#             marker=dict(size=5)
#         ))
#     else:
#         st.info("No recent predictions available for this company yet.")

#     # --- ADD OPENING PRICE LINE ---
#     opening_price_value = st.session_state.opening_price_data.get(selected_ticker, {}).get('price')
#     opening_price_date = st.session_state.opening_price_data.get(selected_ticker, {}).get('date')

#     if opening_price_value is not None:
#         fig.add_hline(
#             y=opening_price_value,
#             line_dash="dash",
#             line_color="grey",
#             annotation_text=f"Opening Price ({opening_price_date.strftime('%Y-%m-%d')}): ${opening_price_value:.2f}",
#             annotation_position="bottom left",
#             annotation_font_color="grey"
#         )

#     fig.update_layout(
#         title=f"Price and Forecast: {selected_name} ({selected_ticker})",
#         xaxis_title="Time (New York)",
#         yaxis_title="Price ($)",
#         template="plotly_white",
#         hovermode="x unified"
#     )

#     st.plotly_chart(fig, use_container_width=True)

#    # --- DISPLAY OPENING PRICE TEXT WITH PERCENTAGE DIFFERENCE ---
#     if st.session_state.opening_price_data[selected_ticker]['price'] is not None:
#         opening_price = st.session_state.opening_price_data[selected_ticker]['price']
#         display_date = st.session_state.opening_price_data[selected_ticker]['date']
        
#         # Get the current (most recent) price
#         current_price = filtered_prices[-1]["price"] if filtered_prices else None

#         if current_price is not None and opening_price != 0: # Avoid division by zero
#             price_diff = current_price - opening_price # Calcola la differenza assoluta
#             percentage_diff = ((current_price - opening_price) / opening_price) * 100
            
#             # Determine color based on percentage_diff
#             color = "green" if percentage_diff >= 0 else "red"
#             sign_per = "+" if percentage_diff >= 0 else ""
#             sign_diff = "+" if percentage_diff >= 0 else "-" 

#             # Define desired font sizes
#             text_size = "14px"      # For "Opening Price (data):"
#             number_size = "22px"    # For the numbers (opening price, difference, percentage)


#             # Construct the full HTML string with custom sizes and bolding for all numbers
#             full_display_text = \
#                 f"<span style='font-size: {text_size};'>Opening Price ({display_date.strftime('%Y-%m-%d')}): </span>" \
#                 f"<span style='font-size: {number_size};'><strong>{opening_price:.2f}$</strong></span> " \
#                 f"<span style='font-size: {number_size}; color:{color};'><strong>{sign_diff}{abs(price_diff):.2f}</strong></span> " \
#                 f"<span style='font-size: {number_size}; color:{color};'><strong>({sign_per}{percentage_diff:.2f}%)</strong></span>"

#             # Use unsafe_allow_html=True to render the HTML
#             st.write(full_display_text, unsafe_allow_html=True)
            
#         else:
#             # For consistency, also apply styles here, including bold for the opening price
#             text_size = "14px"
#             number_size = "22px"
#             st.write(f"<span style='font-size: {text_size};'>Opening Price ({display_date.strftime('%Y-%m-%d')}): </span>" \
#                      f"<span style='font-size: {number_size};'><strong>{opening_price:.2f}$</strong></span>", unsafe_allow_html=True)
#             st.info("Waiting for current price to calculate percentage difference.")
#     else:
#         # And here, if you want to style the waiting message
#         text_size = "14px"
#         st.info(f"<span style='font-size: {text_size};'>Waiting for the opening price for today's trading.</span>", unsafe_allow_html=True)

























import streamlit as st
st.set_page_config(page_title="Real-Time Kafka Dashboard", layout="wide")

from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone, time
from streamlit_autorefresh import st_autorefresh
import pytz # Import pytz for timezone conversion

# Auto-refresh every 3 seconds to keep data flowing
st_autorefresh(interval=3000, key="refresh")

# --- Configuration ---
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

# Define New York timezone and market hours
ny_timezone = pytz.timezone('America/New_York')
utc_timezone = timezone.utc
MARKET_OPEN_TIME = time(9, 30, 0) # 9:30 AM ET
MARKET_SECOND_REFERENCE_TIME = time(16, 1, 0) # 4:00 PM ET - The second daily reference point

# --- Streamlit UI Selection ---
selected_name = st.selectbox("Select a company:", DISPLAY_NAMES)
selected_ticker = NAME_TO_TICKER[selected_name]

# --- Kafka Consumers Initialization ---
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

# --- Session State Management for Data Buffers ---
# Buffer for raw price data (last 10 minutes)
if "price_buffer" not in st.session_state:
    st.session_state.price_buffer = {}
# Buffer for raw prediction data (last 10 minutes)
if "prediction_buffer" not in st.session_state:
    st.session_state.prediction_buffer = {}
# Buffer to store the exchange source for each ticker (e.g., "RANDOM" or real)
if "exchange_status" not in st.session_state:
    st.session_state.exchange_status = {}
# Buffer to store the daily reference price and its flags for each ticker
if "daily_reference_data" not in st.session_state:
    st.session_state.daily_reference_data = {} 

# Initialize buffers for ALL tickers if they don't exist
for ticker in TICKERS_DATA.keys():
    if ticker not in st.session_state.price_buffer:
        st.session_state.price_buffer[ticker] = []
    if ticker not in st.session_state.prediction_buffer:
        st.session_state.prediction_buffer[ticker] = []
    if ticker not in st.session_state.exchange_status:
        st.session_state.exchange_status[ticker] = "N/A" # Default status
    if ticker not in st.session_state.daily_reference_data:
        st.session_state.daily_reference_data[ticker] = {
            'daily_reference_price': None, 
            'reference_date': None, 
            'is_market_open_price_set': False,
            'is_post_market_price_set': False
        }

# --- Kafka Data Consumption and Processing ---

# Consume Price Topic
records = consumer_prices.poll(timeout_ms=100)
for tp, messages in records.items():
    for msg in messages:
        data = msg.value
        # Ensure essential keys are present in the message
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

            # --- Daily Reference Price Logic ---
            reference_data = st.session_state.daily_reference_data[current_message_ticker]

            # Condition to reset ALL daily reference prices if it's a NEW DAY.
            # This is key: it ensures reset upon receiving a message for a new calendar day.
            if reference_data['reference_date'] is None or reference_data['reference_date'] < current_day_ny:
                reference_data['daily_reference_price'] = None
                reference_data['reference_date'] = current_day_ny # Set to current day for new cycle
                reference_data['is_market_open_price_set'] = False 
                reference_data['is_post_market_price_set'] = False

            # 1. Capture the very first price of the day (pre-market or on initial dashboard load).
            # This handles the "primissima volta prende il primo dato".
            if reference_data['daily_reference_price'] is None: 
                reference_data['daily_reference_price'] = price
                # reference_data['reference_date'] is already set to current_day_ny by the reset logic above

            # 2. Update to the official 9:30 AM ET reference price.
            # This overwrites any pre-market price if market open time is reached and not yet set.
            if ts_ny.time() >= MARKET_OPEN_TIME and \
               not reference_data['is_market_open_price_set']:
                reference_data['daily_reference_price'] = price
                reference_data['is_market_open_price_set'] = True # Mark as official 9:30 AM reference

            # 3. Update to the 4:00 PM ET (second reference) price.
            # This overwrites the 9:30 AM price if the second reference time is reached and not yet set.
            # Once set, it sticks for the rest of the day.
            if ts_ny.time() >= MARKET_SECOND_REFERENCE_TIME and \
               not reference_data['is_post_market_price_set']:
                reference_data['daily_reference_price'] = price
                reference_data['is_post_market_price_set'] = True # Mark as official 4:00 PM reference
            
            # Add current price data to buffer
            st.session_state.price_buffer[current_message_ticker].append({
                "timestamp": ts_utc, # Store in UTC
                "price": price
            })
        except Exception as e:
            st.error(f"Error processing price message: {e} - Data: {data}")
            continue

# Consume Predictions Topic
records_pred = consumer_predictions.poll(timeout_ms=100)
for tp, messages in records_pred.items(): # FIX: Usare records_pred invece di records
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

# --- Data Cleanup (Keep Last 10 Minutes) ---
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

# --- Filter Data for Display (Last 5 Minutes) and Convert to New York Timezone ---
# FIX: Applicare la logica del PRIMO script per le predizioni
display_cutoff_utc = now_utc - timedelta(minutes=5)

filtered_prices = []
for d in st.session_state.price_buffer.get(selected_ticker, []):
    if d["timestamp"] >= display_cutoff_utc:
        d["timestamp_ny"] = d["timestamp"].astimezone(ny_timezone)
        filtered_prices.append(d)

# FIX: Logica dal PRIMO script - modifica direttamente il dizionario originale
filtered_predictions = []
for d in st.session_state.prediction_buffer.get(selected_ticker, []):
    if d["timestamp"] >= display_cutoff_utc:
        d["timestamp_ny"] = d["timestamp"].astimezone(ny_timezone)
        filtered_predictions.append(d)

# --- Streamlit Dashboard Layout ---

# Display Header based on Exchange Status
current_exchange_status = st.session_state.exchange_status.get(selected_ticker, "N/A")

if current_exchange_status == "RANDOM":
    st.header(f"Real-Time Price & Prediction for {selected_name} \n Simulated Data")
else:
    st.header(f"Real-Time Price & Prediction for {selected_name}")

# --- Plotly Chart for Prices and Predictions ---
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
        marker=dict(size=4)
    ))

    # # Prediction line - FIX: Usare la logica del PRIMO script
    # if filtered_predictions:
    #     df_preds = pd.DataFrame(filtered_predictions)
    #     df_preds["timestamp_ny"] = pd.to_datetime(df_preds["timestamp_ny"])

    #     fig.add_trace(go.Scatter(
    #         x=df_preds["timestamp_ny"],
    #         y=df_preds["predicted_price"], # FIX: Questa chiave ora esiste
    #         mode="markers+lines",
    #         name="Predicted Price",
    #         line=dict(color="red", dash="dash"),
    #         marker=dict(size=5)
    #     ))
    # else:
    #     st.info("No recent predictions available for this company yet.")
    
    if filtered_predictions:
        df_preds = pd.DataFrame(filtered_predictions)
        df_preds["timestamp_ny"] = pd.to_datetime(df_preds["timestamp_ny"])

        fig.add_trace(go.Scatter(
            x=df_preds["timestamp_ny"],
            y=df_preds["predicted_price"],
            mode="markers+lines",
            name="Predicted Price",
            line=dict(color="red", dash="dash"),
            marker=dict(size=5)
        ))
    else:
        # Aggiungi trace vuoto per mantenere la legenda
        fig.add_trace(go.Scatter(
            x=[],
            y=[],
            mode="markers+lines",
            name="Predicted Price",
            line=dict(color="red", dash="dash"),
            marker=dict(size=5),
            showlegend=True
        ))
        st.info("No recent predictions available for this company yet.")

    # --- Add Daily Reference Price Line to Chart ---
    daily_reference_price_value = st.session_state.daily_reference_data.get(selected_ticker, {}).get('daily_reference_price')
    reference_date = st.session_state.daily_reference_data.get(selected_ticker, {}).get('reference_date')
    is_post_market_price_set = st.session_state.daily_reference_data.get(selected_ticker, {}).get('is_post_market_price_set', False)
    is_market_open_price_set = st.session_state.daily_reference_data.get(selected_ticker, {}).get('is_market_open_price_set', False)

    if daily_reference_price_value is not None:
        annotation_text = ""
        line_color = "grey" # Default color for initial data (before 9:30 AM)
        line_dash = "dash"

        if is_post_market_price_set:
            annotation_text = f"Opening Price Simulated Data, ({reference_date.strftime('%Y-%m-%d')}): ${daily_reference_price_value:.2f}"
            line_color = "grey" # Specific color for the 4:00 PM reference
            line_dash = "dash"
        elif is_market_open_price_set:
            annotation_text = f"Opening Price (09:30 AM, {reference_date.strftime('%Y-%m-%d')}): ${daily_reference_price_value:.2f}"
            line_color = "grey" # Specific color for the 9:30 AM reference
            line_dash = "dash"
        else: 
            # This branch is for the very first data point captured on a new day, before 9:30 AM
            annotation_text = f"Reference Price ({reference_date.strftime('%Y-%m-%d')}): ${daily_reference_price_value:.2f}"
            line_color = "grey" 
            line_dash = "dash"

        fig.add_hline(
            y=daily_reference_price_value,
            line_dash=line_dash,
            line_color=line_color,
            annotation_text=annotation_text,
            annotation_position="bottom left",
            annotation_font_color=line_color
        )
    
    fig.update_layout(
        title=f"Price and Forecast: {selected_name} ({selected_ticker})",
        xaxis_title="Time (New York)",
        yaxis_title="Price ($)",
        template="plotly_white",
        hovermode="x unified"
    )

    st.plotly_chart(fig, use_container_width=True)


# --- Display Daily Reference Price and Performance ---
# This section shows the fixed daily reference price and the performance relative to it.

if st.session_state.daily_reference_data[selected_ticker]['daily_reference_price'] is not None:
    reference_price = st.session_state.daily_reference_data[selected_ticker]['daily_reference_price']
    display_date = st.session_state.daily_reference_data[selected_ticker]['reference_date']
    is_post_market_price_set = st.session_state.daily_reference_data[selected_ticker]['is_post_market_price_set']
    is_market_open_price_set = st.session_state.daily_reference_data[selected_ticker]['is_market_open_price_set']

    # Get the current (most recent) price to calculate daily performance
    current_price = filtered_prices[-1]["price"] if filtered_prices else None

    # Determine the label for the reference price based on which one is active
    reference_label = "Reference Price" 
    if is_post_market_price_set:
        reference_label = "Opening Price Simulated Data"
    elif is_market_open_price_set:
        reference_label = "Opening Price (09:30 AM)"
    
    if current_price is not None and reference_price != 0: # Avoid division by zero
        price_diff = current_price - reference_price
        percentage_diff = ((current_price - reference_price) / reference_price) * 100
        
        # Determine color for performance display
        color = "green" if percentage_diff >= 0 else "red"
        sign_per = "+" if percentage_diff >= 0 else ""
        sign_diff = "+" if percentage_diff >= 0 else "" 
        if price_diff < 0: sign_diff = "-" # Ensure negative sign for price_diff if applicable

        # Define desired font sizes for display
        text_size = "14px"
        number_size = "22px"

        # Construct the full HTML string with custom sizes and bolding
        full_display_text = \
            f"<span style='font-size: {text_size};'>{reference_label} ({display_date.strftime('%Y-%m-%d')}): </span>" \
            f"<span style='font-size: {number_size};'><strong>{reference_price:.2f}$</strong></span> " \
            f"<span style='font-size: {number_size}; color:{color};'><strong>{sign_diff}{abs(price_diff):.2f}</strong></span> " \
            f"<span style='font-size: {number_size}; color:{color};'><strong>({sign_per}{percentage_diff:.2f}%)</strong></span>"

        # Use unsafe_allow_html=True to render the HTML
        st.write(full_display_text, unsafe_allow_html=True)
        
    else:
        # Display only the reference price if current price is not available for calculation
        text_size = "14px"
        number_size = "22px"
        st.write(f"<span style='font-size: {text_size};'>{reference_label} ({display_date.strftime('%Y-%m-%d')}): </span>" \
                 f"<span style='font-size: {number_size};'><strong>{reference_price:.2f}$</strong></span>", unsafe_allow_html=True)
        st.info("Waiting for current price to calculate percentage difference.")
else:
    # Message displayed if no daily reference price has been set yet for the current day
    text_size = "14px"
    st.info("Waiting for the daily reference price for today's trading.")