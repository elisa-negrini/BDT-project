# - i dati stream sono simultati
# - viene preso il modello e viene fatto fine tuning con i dati stream
# - per ora fatto solo per Apple
# - il plot mostra la previsione live
# - prima di usare questo bisognerebbe fare training anche con la sentiment analysis e magari gli altri dati
# che vorremo considerare
# - nel fine tuning bisognerà aggiungere anche lo stream di dati della sentiment analisys qualora dovesser
# arrivare notizie

### IMPORTANTE! non runnare come al solito, ma dal terminale con il comando: streamlit run predict_stream.py


# predict_stream.py — versione Streamlit con delay reale per simulazione streaming

import torch
import torch.nn as nn
import pandas as pd
import joblib
import os
import streamlit as st
from datetime import datetime, timedelta
import numpy as np
import plotly.graph_objects as go
import time

# === CONFIG ===
company = 'AAPL'
model_path = f'models/{company}_model.pth'
x_scaler_path = f'models/{company}_X_scaler.pkl'
y_scaler_path = f'models/{company}_y_scaler.pkl'
prediction_days = 30

# === MODEL CLASS ===
class StockPredictor(nn.Module):
    def __init__(self):
        super(StockPredictor, self).__init__()
        self.fc1 = nn.Linear(6, 64)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(64, 32)
        self.fc3 = nn.Linear(32, 1)

    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.fc3(x)
        return x

# === LOAD MODEL + SCALERS ===
model = StockPredictor()
model.load_state_dict(torch.load(model_path))
model.train()

x_scaler = joblib.load(x_scaler_path)
y_scaler = joblib.load(y_scaler_path)

# === INIZIALIZZA DATI STREAM (simulati) ===
np.random.seed(42)
dates = pd.date_range(end=datetime.today(), periods=30)
df = pd.DataFrame({
    'timestamp': dates,
    'close': np.linspace(200, 210, 30) + np.random.normal(0, 1, 30),
    'volume': np.random.randint(1e6, 2e6, 30),
    'vwap': np.linspace(201, 209, 30) + np.random.normal(0, 0.5, 30)
})

all_timestamps = list(df['timestamp'])
all_closes = list(df['close'])
all_preds = []
prediction_counter = 0

st.set_page_config(page_title="Stock Forecast Live", layout="wide")
st.title(f"📈 Live Forecasting: {company}")

chart = st.empty()
progress = st.progress(0, text="Inizio simulazione...")

# === LOOP DI SIMULAZIONE STREAM ===
for i in range(prediction_days):
    df['close_lag1'] = df['close'].shift(1)
    df['close_lag2'] = df['close'].shift(2)
    df['volume_lag1'] = df['volume'].shift(1)
    df['volume_lag2'] = df['volume'].shift(2)
    df['vwap_lag1'] = df['vwap'].shift(1)
    df['vwap_lag2'] = df['vwap'].shift(2)
    df = df.dropna()

    if len(df) < 2:
        continue

    X = df[['close_lag1', 'close_lag2', 'volume_lag1', 'volume_lag2', 'vwap_lag1', 'vwap_lag2']].copy()
    y = df['close'].copy()

    try:
        X_pred = X.iloc[[-1]]
        y_true = y.iloc[-1]
    except IndexError:
        continue

    X_scaled = x_scaler.transform(X_pred)
    X_tensor = torch.tensor(X_scaled, dtype=torch.float32)

    with torch.no_grad():
        y_pred_scaled = model(X_tensor)
        y_pred = y_scaler.inverse_transform(y_pred_scaled.numpy())[0][0]
    all_preds.append(y_pred)
    prediction_counter += 1

    # === FINE-TUNING ===
    model.train()
    y_true_scaled = y_scaler.transform([[y_true]])
    y_true_tensor = torch.tensor(y_true_scaled, dtype=torch.float32)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    criterion = nn.MSELoss()
    optimizer.zero_grad()
    output = model(X_tensor)
    loss = criterion(output, y_true_tensor)
    loss.backward()
    optimizer.step()

    torch.save(model.state_dict(), model_path)

    # === Simula nuovo dato ===
    new_close = y_pred + np.random.normal(0, 0.8)
    new_volume = df['volume'].iloc[-1] * np.random.uniform(0.95, 1.05)
    new_vwap = df['vwap'].iloc[-1] + np.random.normal(0, 0.3)
    new_time = df['timestamp'].iloc[-1] + timedelta(days=1)

    df = pd.concat([df, pd.DataFrame.from_records([{
        'timestamp': new_time,
        'close': new_close,
        'volume': new_volume,
        'vwap': new_vwap
    }])], ignore_index=True)

    all_timestamps.append(new_time)
    all_closes.append(new_close)

    # === AGGIORNA CHART ===
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=all_timestamps, y=all_closes, mode='lines', name='Simulated Close', line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=all_timestamps[-len(all_preds):], y=all_preds, mode='lines+markers', name='Predicted Close', line=dict(color='red')))
    fig.update_layout(margin=dict(l=20, r=20, t=30, b=20), height=500)
    chart.plotly_chart(fig, use_container_width=True)
    progress.progress((i+1)/prediction_days, text=f"Step {i+1}/{prediction_days} — predizioni: {prediction_counter}")

    # Delay per simulazione streaming reale
    time.sleep(1)

st.success(f"✅ Simulazione completata. Totale predizioni: {prediction_counter}")
