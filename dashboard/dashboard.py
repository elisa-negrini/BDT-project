import streamlit as st
import json
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone, time
from streamlit_autorefresh import st_autorefresh
import pytz
from kafka import KafkaConsumer
import os
import psycopg2

# ====== Streamlit Page Configuration ======
st.set_page_config(page_title="Real-Time Kafka Dashboard", layout="wide")

# Auto-refresh every 3 seconds to keep data flowing.
st_autorefresh(interval=3000, key="refresh")

# ====== Environment Variables and Configuration ======
# PostgreSQL environment variables (used to load tickers).
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

@st.cache_data(ttl=3600) # Cache the data for 1 hour to avoid frequent DB connections.
def load_tickers_from_db():
    """
    Loads stock tickers and company names from the PostgreSQL database.
    Returns a dictionary {ticker: company_name}.
    Raises Streamlit error if DB connection fails.
    """
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ticker, company_name FROM companies_info WHERE is_active = TRUE;")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        tickers_data = {row[0]: row[1] for row in rows}
        if not tickers_data:
            st.error("No tickers found in the database.")
            st.stop()

        return tickers_data

    except Exception as e:
        st.error(f"Failed to load tickers from the database: {e}")
        st.stop()  # Blocca l’esecuzione della dashboard


# ====== Configuration ======
# Available tickers and their corresponding names
TICKERS_DATA = load_tickers_from_db()

# Create a mapping from name to ticker for the selectbox
NAME_TO_TICKER = {name: ticker for ticker, name in TICKERS_DATA.items()}
DISPLAY_NAMES = list(NAME_TO_TICKER.keys())

# Define New York timezone and market hours
ny_timezone = pytz.timezone('America/New_York')
utc_timezone = timezone.utc
MARKET_OPEN_TIME = time(9, 30, 0) 
MARKET_SECOND_REFERENCE_TIME = time(16, 1, 0)

# ====== Streamlit UI Selection ======
selected_name = st.selectbox("Select a company:", DISPLAY_NAMES)
selected_ticker = NAME_TO_TICKER[selected_name]

# ====== Kafka Consumers Initialization ======
@st.cache_resource
def create_consumers():
    """Initializes and returns Kafka consumers for stock prices, predictions, and anomaly detection."""
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
    # Consumer for anomaly detection data
    consumer_anomalies = KafkaConsumer(
        "anomaly_detection",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="dashboard-consumer-anomalies"
    )
    return consumer_prices, consumer_predictions, consumer_anomalies

consumer_prices, consumer_predictions, consumer_anomalies = create_consumers()

# ====== Session State Management for Data Buffers ======
# Buffer for raw price data (last 10 minutes)
if "price_buffer" not in st.session_state:
    st.session_state.price_buffer = {}
# Buffer for raw prediction data (last 10 minutes)
if "prediction_buffer" not in st.session_state:
    st.session_state.prediction_buffer = {}
# Buffer for anomaly data (last 10 minutes)
if "anomaly_buffer" not in st.session_state:
    st.session_state.anomaly_buffer = {}
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
    if ticker not in st.session_state.anomaly_buffer:
        st.session_state.anomaly_buffer[ticker] = []
    if ticker not in st.session_state.exchange_status:
        st.session_state.exchange_status[ticker] = "N/A"
    if ticker not in st.session_state.daily_reference_data:
        st.session_state.daily_reference_data[ticker] = {
            'daily_reference_price': None,
            'reference_date': None,
            'is_market_open_price_set': False,
            'is_post_market_price_set': False
        }

# ====== Kafka Data Consumption and Processing ======

# Consume Price Topic
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

            # --- Daily Reference Price Logic ---
            reference_data = st.session_state.daily_reference_data[current_message_ticker]

            # Condition to reset ALL daily reference prices if it's a NEW DAY.
            if reference_data['reference_date'] is None or reference_data['reference_date'] < current_day_ny:
                reference_data['daily_reference_price'] = None
                reference_data['reference_date'] = current_day_ny
                reference_data['is_market_open_price_set'] = False
                reference_data['is_post_market_price_set'] = False

            # 1. Capture the very first price of the day (pre-market or on initial dashboard load).
            if reference_data['daily_reference_price'] is None:
                reference_data['daily_reference_price'] = price

            # 2. Update to the official 9:30 AM ET reference price.
            if ts_ny.time() >= MARKET_OPEN_TIME and \
               not reference_data['is_market_open_price_set']:
                reference_data['daily_reference_price'] = price
                reference_data['is_market_open_price_set'] = True # Mark as official 9:30 AM reference

            # 3. Update to the 4:00 PM ET (second reference) price.
            if ts_ny.time() >= MARKET_SECOND_REFERENCE_TIME and \
               not reference_data['is_post_market_price_set']:
                reference_data['daily_reference_price'] = price
                reference_data['is_post_market_price_set'] = True # Mark as official 4:00 PM reference

            # Add current price data to buffer
            st.session_state.price_buffer[current_message_ticker].append({
                "timestamp": ts_utc,
                "price": price
            })
        except Exception as e:
            st.error(f"Error processing price message: {e} - Data: {data}")
            continue

# Consume Predictions Topic
records_pred = consumer_predictions.poll(timeout_ms=100)
for tp, messages in records_pred.items():
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

# Consume Anomaly Detection Topic
records_anomalies = consumer_anomalies.poll(timeout_ms=100)
for tp, messages in records_anomalies.items():
    for msg in messages:
        data = msg.value

        if "ticker" not in data or "timestamp" not in data or "anomaly_type" not in data:
            continue

        current_message_ticker = data["ticker"]

        try:
            ts_utc = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00")).astimezone(utc_timezone)
            min_price = float(data.get("min_price"))
            max_price = float(data.get("max_price"))
            price_difference = float(data.get("price_difference"))

            reported_min_timestamp_utc = datetime.fromisoformat(data.get("min_timestamp").replace("Z", "+00:00")).astimezone(utc_timezone)
            reported_max_timestamp_utc = datetime.fromisoformat(data.get("max_timestamp").replace("Z", "+00:00")).astimezone(utc_timezone)

            sorted_timestamps = sorted([reported_min_timestamp_utc, reported_max_timestamp_utc])
            chron_min_timestamp_utc = sorted_timestamps[0]
            chron_max_timestamp_utc = sorted_timestamps[1]


            st.session_state.anomaly_buffer[current_message_ticker].append({
                "timestamp": ts_utc,
                "anomaly_type": data.get("anomaly_type"),
                "min_price": min_price,
                "max_price": max_price,
                "price_difference": price_difference,
                "min_timestamp": chron_min_timestamp_utc, 
                "max_timestamp": chron_max_timestamp_utc 
            })
        except Exception as e:
            st.error(f"Error processing anomaly message: {e} - Data: {data}")
            continue

# ====== Data Cleanup and Filtering for Display (Harmonized to 5 Minutes) ======
display_and_cleanup_cutoff_utc = datetime.now(utc_timezone) - timedelta(minutes=5)

# Cleanup and Filter for Prices
filtered_prices = []
st.session_state.price_buffer[selected_ticker] = [
    d for d in st.session_state.price_buffer[selected_ticker]
    if d["timestamp"] >= display_and_cleanup_cutoff_utc
]
for d in st.session_state.price_buffer.get(selected_ticker, []):
    d["timestamp_ny"] = d["timestamp"].astimezone(ny_timezone)
    filtered_prices.append(d)

# Cleanup and Filter for Predictions
filtered_predictions = []
st.session_state.prediction_buffer[selected_ticker] = [
    d for d in st.session_state.prediction_buffer[selected_ticker]
    if d["timestamp"] >= display_and_cleanup_cutoff_utc
]
for d in st.session_state.prediction_buffer.get(selected_ticker, []):
    d["timestamp_ny"] = d["timestamp"].astimezone(ny_timezone)
    filtered_predictions.append(d)

# Cleanup and Filter for Anomalies
filtered_anomalies = []
st.session_state.anomaly_buffer[selected_ticker] = [
    d for d in st.session_state.anomaly_buffer[selected_ticker]
    if d["min_timestamp"] >= display_and_cleanup_cutoff_utc or d["max_timestamp"] >= display_and_cleanup_cutoff_utc
]
for d in st.session_state.anomaly_buffer.get(selected_ticker, []):
    d["timestamp_ny"] = d["timestamp"].astimezone(ny_timezone)
    d["min_timestamp_ny"] = d["min_timestamp"].astimezone(ny_timezone)
    d["max_timestamp_ny"] = d["max_timestamp"].astimezone(ny_timezone)
    filtered_anomalies.append(d)

# ====== Streamlit Dashboard Layout ======

# Display Header based on Exchange Status
current_exchange_status = st.session_state.exchange_status.get(selected_ticker, "N/A")

if current_exchange_status == "RANDOM":
    st.header(f"Real-Time Price & Prediction for {selected_name} \n Simulated Data")
else:
    st.header(f"Real-Time Price & Prediction for {selected_name}")

# ====== Plotly Chart for Prices and Predictions ======
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

    fig.add_trace(go.Scatter(
        x=[None], y=[None], mode='lines',
        line=dict(color='rgba(0, 128, 0, 0.2)', width=0),
        fill='tozeroy',
        name='Positive Anomaly Period',
        showlegend=True
    ))
    fig.add_trace(go.Scatter(
        x=[None], y=[None], mode='lines',
        line=dict(color='rgba(255, 0, 0, 0.2)', width=0),
        fill='tozeroy',
        name='Negative Anomaly Period',
        showlegend=True
    ))

    shapes_to_add = []
    for anomaly in filtered_anomalies:

        fill_color = "rgba(0, 128, 0, 0.2)" if anomaly["price_difference"] >= 0 else "rgba(255, 0, 0, 0.2)"

        shapes_to_add.append(
            go.layout.Shape(
                type="rect",
                xref="x", yref="paper",
                x0=anomaly["min_timestamp_ny"],
                x1=anomaly["max_timestamp_ny"],
                y0=0,
                y1=1,
                fillcolor=fill_color,
                layer="below",
                line_width=0,
            )
        )

# ====== Add Daily Reference Price Line to Chart as a Shape ======
    daily_reference_price_value = st.session_state.daily_reference_data.get(selected_ticker, {}).get('daily_reference_price')
    is_post_market_price_set = st.session_state.daily_reference_data.get(selected_ticker, {}).get('is_post_market_price_set', False)
    is_market_open_price_set = st.session_state.daily_reference_data.get(selected_ticker, {}).get('is_market_open_price_set', False)

    if daily_reference_price_value is not None:
        annotation_text = ""
        line_color = "grey"
        line_dash = "dash"

        if is_post_market_price_set:
            annotation_text = "Opening Price Simulated Data"
        elif is_market_open_price_set:
            annotation_text = "Opening Price"
        else:
            annotation_text = "Reference Price"

        # Add the horizontal line as a shape
        shapes_to_add.append(
            go.layout.Shape(
                type="line",
                xref="paper", yref="y",
                x0=0, x1=1,
                y0=daily_reference_price_value, y1=daily_reference_price_value,
                line=dict(
                    color=line_color,
                    dash=line_dash,
                    width=2,
                ),
                layer="above"
            )
        )
        # Add annotation separately as a layout annotation
        fig.add_annotation(
            xref="paper", yref="y",
            x=0,
            y=daily_reference_price_value,
            text=annotation_text,
            showarrow=False,
            xanchor="left",
            yanchor="bottom",
            font=dict(color=line_color, size=12)
        )

    fig.update_layout(
        title=f"Price and Forecast: {selected_name} ({selected_ticker})",
        xaxis_title="Time (New York)",
        yaxis_title="Price ($)",
        template="plotly_white",
        hovermode="x unified",
        shapes=shapes_to_add
    )

    fig.update_layout(
        title=f"Price and Forecast: {selected_name} ({selected_ticker})",
        xaxis_title="Time (New York)",
        yaxis_title="Price ($)",
        template="plotly_white",
        hovermode="x unified",
        shapes=shapes_to_add
    )

    st.plotly_chart(fig, use_container_width=True)

# ====== Display Daily Reference Price and Performance ======
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

    if current_price is not None and reference_price != 0:
        price_diff = current_price - reference_price
        percentage_diff = ((current_price - reference_price) / reference_price) * 100

        # Determine color for performance display
        color = "green" if percentage_diff >= 0 else "red"
        # Determine sign for display (only '+' for positive percentage, '-' for negative)
        sign_per = "+" if percentage_diff >= 0 else ""
        # Determine sign for price difference (only '+' for positive, '-' for negative)
        sign_diff = "+" if price_diff >= 0 else ""
        # The price_diff itself already contains the negative sign if it's negative,
        # so we don't need to add another one. If it's zero or positive, we want a '+' sign.
        if price_diff < 0: sign_diff = ""

        # Define desired font sizes for display
        text_size = "14px"
        number_size = "22px"

        # Construct the full HTML string with custom sizes and bolding
        full_display_text = \
            f"<span style='font-size: {text_size};'>{reference_label} ({display_date.strftime('%Y-%m-%d')}): </span>" \
            f"<span style='font-size: {number_size};'><strong>{reference_price:.2f}$</strong></span> " \
            f"<span style='font-size: {number_size}; color:{color};'><strong>{sign_diff}{price_diff:.2f}</strong></span> " \
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

if filtered_anomalies:
    # Sort anomalies by min_timestamp in descending order to get the most recent one
    filtered_anomalies_sorted = sorted(filtered_anomalies, key=lambda x: x['min_timestamp_ny'], reverse=True)

    # Display only the most recent anomaly
    last_anomaly = filtered_anomalies_sorted[0]

    st.subheader("Last Anomaly Detected")

    # Determine the color for the anomaly summary text and the sign for price_difference
    anomaly_text_color = "green"
    price_diff_value = last_anomaly['price_difference']
    price_diff_display = f"${price_diff_value:.2f}"

    if price_diff_value < 0:
        anomaly_text_color = "red"
        # price_diff_display will automatically have the '-' sign from formatting a negative number
    elif price_diff_value > 0:
        pass

    # Use the chronologically ordered timestamps for display
    start_time_str = last_anomaly['min_timestamp_ny'].strftime('%Y-%m-%d %H:%M:%S')
    end_time_str = last_anomaly['max_timestamp_ny'].strftime('%H:%M:%S')

    time_range_str = f"{start_time_str} - {end_time_str}"
    if price_diff_value > 0:         
        st.markdown(
            f"""
            <div style="border: 1px solid #FFDDC1; border-radius: 5px; padding: 10px; margin-bottom: 5px;">
                <strong>Anomaly Period:</strong> {time_range_str}<br>
                <strong>Price Range:</strong> ${last_anomaly['min_price']:.2f} - ${last_anomaly['max_price']:.2f}<br>
                <strong>Price Difference:</strong> <span style="color:{anomaly_text_color};"><strong>{price_diff_display}</strong></span>
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            f"""
            <div style="border: 1px solid #FFDDC1; border-radius: 5px; padding: 10px; margin-bottom: 5px;">
                <strong>Anomaly Period:</strong> {time_range_str}<br>
                <strong>Price Range:</strong> ${last_anomaly['max_price']:.2f} - ${last_anomaly['min_price']:.2f}<br>
                <strong>Price Difference:</strong> <span style="color:{anomaly_text_color};"><strong>{price_diff_display}</strong></span>
            </div>
            """,
            unsafe_allow_html=True
        )