# per ora è fatto solo per una company, da modificare per fare
# un modello per ogni company oppure uno generale
# con la variabile company
import pandas as pd
import matplotlib.pyplot as plt
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
import os

# Load data
df = pd.read_parquet('historical_data.parquet')
df = df.reset_index()

# Choose one company
company = 'AAPL'  # Change as needed
df_company = df[df['symbol'] == company].copy()
df_company = df_company.sort_values('timestamp')

# Create lag features
df_company['close_lag1'] = df_company['close'].shift(1)
df_company['close_lag2'] = df_company['close'].shift(2)
df_company = df_company.dropna()

# Input & target
X = df_company[['close_lag1', 'close_lag2']].copy()
y = df_company['close'].copy()

# Split train/test
if len(X) > 10:
    split_index = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]
else:
    X_train, X_test = X.iloc[:-2], X.iloc[-2:]
    y_train, y_test = y.iloc[:-2], y.iloc[-2:]

# Scale X and y
X_scaler = StandardScaler()
X_train_scaled = X_scaler.fit_transform(X_train)
X_test_scaled = X_scaler.transform(X_test)

y_scaler = StandardScaler()
y_train_scaled = y_scaler.fit_transform(y_train.values.reshape(-1, 1))
y_test_scaled = y_scaler.transform(y_test.values.reshape(-1, 1))

# Convert to tensors
X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32)
y_train_tensor = torch.tensor(y_train_scaled, dtype=torch.float32)
X_test_tensor = torch.tensor(X_test_scaled, dtype=torch.float32)
y_test_tensor = torch.tensor(y_test_scaled, dtype=torch.float32)

# Define model
class StockPredictor(nn.Module):
    def __init__(self):
        super(StockPredictor, self).__init__()
        self.fc1 = nn.Linear(2, 64)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(64, 32)
        self.fc3 = nn.Linear(32, 1)

    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.fc3(x)
        return x

model = StockPredictor()
criterion = nn.MSELoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Train
epochs = 100
for epoch in range(epochs):
    model.train()
    optimizer.zero_grad()
    outputs = model(X_train_tensor)
    loss = criterion(outputs, y_train_tensor)
    loss.backward()
    optimizer.step()
    if (epoch+1) % 20 == 0:
        print(f"Epoch {epoch+1}/{epochs}, Loss: {loss.item():.4f}")

# Predict and inverse scale
model.eval()
with torch.no_grad():
    y_pred_tensor = model(X_test_tensor)
    y_pred_rescaled = y_scaler.inverse_transform(y_pred_tensor.numpy())
    y_test_rescaled = y_scaler.inverse_transform(y_test_tensor.numpy())

# MSE
mse = mean_squared_error(y_test_rescaled, y_pred_rescaled)
print(f"Mean Squared Error: {mse:.2f}")

# Plot
timestamps_full = df_company['timestamp']
timestamps_test = df_company.loc[y_test.index]['timestamp']

plt.figure(figsize=(14,7))
plt.plot(timestamps_full, y.values, label='True Close Price', color='blue')
plt.plot(timestamps_test, y_pred_rescaled.flatten(), label='Predicted Close Price (Test Set)', color='red')
plt.legend()
plt.title(f'True vs Predicted Close Prices for {company}')
plt.xlabel('Date')
plt.ylabel('Price')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()

# Save model + scalers
#os.makedirs('models', exist_ok=True)
#torch.save(model.state_dict(), f'models/{company}_model.pth')
#joblib.dump(X_scaler, f'models/{company}_X_scaler.pkl')
#joblib.dump(y_scaler, f'models/{company}_y_scaler.pkl')
#print(f"Model and scalers for {company} saved.")
