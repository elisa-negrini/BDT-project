import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
import os
import joblib
import datetime

# Performance logging
mse_results = []

# Load data
df = pd.read_parquet('historical_data.parquet').reset_index()
symbols = df['symbol'].unique()

# Define model class
class StockPredictor(nn.Module):
    def __init__(self, input_dim):
        super(StockPredictor, self).__init__()
        self.fc1 = nn.Linear(input_dim, 64)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(64, 32)
        self.fc3 = nn.Linear(32, 1)

    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.fc3(x)
        return x

# Create model folder
os.makedirs('models', exist_ok=True)

# Features to use
feature_cols = ['close_lag1', 'close_lag2', 'volume_lag1', 'volume_lag2', 'vwap_lag1', 'vwap_lag2']
input_dim = len(feature_cols)

for company in symbols:
    print(f"\n🔁 Training model for {company}")
    df_company = df[df['symbol'] == company].copy().sort_values('timestamp')

    # Create lag features
    df_company['close_lag1'] = df_company['close'].shift(1)
    df_company['close_lag2'] = df_company['close'].shift(2)
    df_company['volume_lag1'] = df_company['volume'].shift(1)
    df_company['volume_lag2'] = df_company['volume'].shift(2)
    df_company['vwap_lag1'] = df_company['vwap'].shift(1)
    df_company['vwap_lag2'] = df_company['vwap'].shift(2)
    df_company = df_company.dropna()

    if len(df_company) < 20:
        print(f"❌ Skipping {company}: not enough data.")
        continue

    X = df_company[feature_cols]
    y = df_company['close']

    # Split
    split_index = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]

    # Scale
    X_scaler = StandardScaler()
    y_scaler = StandardScaler()
    X_train_scaled = X_scaler.fit_transform(X_train)
    X_test_scaled = X_scaler.transform(X_test)
    y_train_scaled = y_scaler.fit_transform(y_train.values.reshape(-1, 1))
    y_test_scaled = y_scaler.transform(y_test.values.reshape(-1, 1))

    # Tensors
    X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32)
    y_train_tensor = torch.tensor(y_train_scaled, dtype=torch.float32)
    X_test_tensor = torch.tensor(X_test_scaled, dtype=torch.float32)
    y_test_tensor = torch.tensor(y_test_scaled, dtype=torch.float32)

    # Model
    model = StockPredictor(input_dim=input_dim)
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    # Train
    for epoch in range(100):
        model.train()
        optimizer.zero_grad()
        outputs = model(X_train_tensor)
        loss = criterion(outputs, y_train_tensor)
        loss.backward()
        optimizer.step()

    # Predict and inverse scale
    model.eval()
    with torch.no_grad():
        y_pred_tensor = model(X_test_tensor)
        y_pred_rescaled = y_scaler.inverse_transform(y_pred_tensor.numpy())
        y_test_rescaled = y_scaler.inverse_transform(y_test_tensor.numpy())

    mse = mean_squared_error(y_test_rescaled, y_pred_rescaled)
    print(f"✅ {company} MSE: {mse:.2f}")

    # Save model + scalers
    torch.save(model.state_dict(), f'models/{company}_model.pth')
    joblib.dump(X_scaler, f'models/{company}_X_scaler.pkl')
    joblib.dump(y_scaler, f'models/{company}_y_scaler.pkl')

    mse_results.append({'symbol': company, 'mse': mse})

# Dopo il ciclo FOR, salva su CSV
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
mse_df = pd.DataFrame(mse_results).set_index("symbol")

csv_path = "model_performance.csv"

# Se esiste già, lo aggiorniamo aggiungendo una nuova colonna
if os.path.exists(csv_path):
    prev = pd.read_csv(csv_path, index_col="symbol")
    merged = prev.join(mse_df.rename(columns={"mse": f"mse_{timestamp}"}), how="outer")
    merged.to_csv(csv_path)
else:
    mse_df.rename(columns={"mse": f"mse_{timestamp}"}).to_csv(csv_path)

print(f"\n📊 Saved MSE results to {csv_path}")