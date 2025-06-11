import pandas as pd
import requests
import time
import os

# === CONFIGURATION ===
API_KEY = os.getenv("COMPANY_F_API_KEY")
COMPANY_CSV_PATH = "postgresql/companies_info.csv"
LIMIT = 10
SLEEP_TIME = 1
OUTPUT_FOLDER = "historical_data/producer_h_company"

# === LOAD TICKERS ===
try:
    df_companies = pd.read_csv(COMPANY_CSV_PATH)
    tickers = df_companies["ticker"].tolist()
    print(f"Loaded {len(tickers)} tickers from {COMPANY_CSV_PATH}")
except FileNotFoundError:
    print(f"Error: {COMPANY_CSV_PATH} not found. Please ensure the file exists.")
    exit()
except KeyError:
    # Handle error if the 'ticker' column is not present in the CSV file
    print(f"Error: 'ticker' column not found in {COMPANY_CSV_PATH}.")
    exit()

# === FUNCTION TO FETCH DATA FROM FMP ===
def get_fmp_data(endpoint, symbol):
    # Construct the URL for the API request
    url = f"https://financialmodelingprep.com/api/v3/{endpoint}/{symbol}?limit={LIMIT}&apikey={API_KEY}"
    # Execute the GET request
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    print(f"Warning: Could not fetch data for {symbol} from {endpoint}. Status code: {response.status_code}")
    return []

# === MAIN DATA FETCHING LOOP ===
all_data = []

print("Fetching data for each ticker...")
for i, symbol in enumerate(tickers):
    print(f"[{i+1}/{len(tickers)}] {symbol}")

    # Fetch income statement data
    income_list = get_fmp_data("income-statement", symbol)
    # Fetch balance sheet data
    balance_list = get_fmp_data("balance-sheet-statement", symbol)
    # Fetch cash flow statement data
    cashflow_list = get_fmp_data("cash-flow-statement", symbol)

    for idx in range(LIMIT):
        income = income_list[idx] if idx < len(income_list) else {}
        balance = balance_list[idx] if idx < len(balance_list) else {}
        cashflow = cashflow_list[idx] if idx < len(cashflow_list) else {}

        if income or balance or cashflow:
            merged = {
                "symbol": symbol,
                "reportIndex": idx + 1,
                "reportDate": income.get("date", balance.get("date", cashflow.get("date", None))),
                **income,
                **{f"balance_{k}": v for k, v in balance.items()},
                **{f"cashflow_{k}": v for k, v in cashflow.items()}
            }
            all_data.append(merged)

    time.sleep(SLEEP_TIME)

# === SAVE AND CORRECT RESULTS ===
df_company_fundamentals = pd.DataFrame(all_data)

# Correction: If a ticker has "2025" in its 'calendarYear',
# decrement all of its 'calendarYear' values by 1.
if "calendarYear" in df_company_fundamentals.columns:
    df_company_fundamentals["calendarYear_numeric"] = pd.to_numeric(
        df_company_fundamentals["calendarYear"], errors="coerce"
    )

    # Identify which symbols (tickers) have at least one report with 'calendarYear' equal to 2025
    tickers_to_correct = df_company_fundamentals[
        df_company_fundamentals["calendarYear_numeric"] == 2025
    ]["symbol"].unique()

    # Apply the correction only if tickers with the year 2025 were found
    if len(tickers_to_correct) > 0:
        print(f"Applying -1 year correction to tickers: {', '.join(tickers_to_correct)}")
        for symbol_to_correct in tickers_to_correct:
            mask_ticker = df_company_fundamentals["symbol"] == symbol_to_correct
            df_company_fundamentals.loc[mask_ticker, "calendarYear_numeric"] = (
                df_company_fundamentals.loc[mask_ticker, "calendarYear_numeric"].sub(1)
            )
        
        df_company_fundamentals["calendarYear"] = (
            df_company_fundamentals["calendarYear_numeric"].fillna(0).astype(int).astype(str)
        )
    else:
        print("No tickers found with '2025' in 'calendarYear', no global correction applied.")

    # Remove the temporary numeric column
    df_company_fundamentals = df_company_fundamentals.drop(columns=["calendarYear_numeric"])

# Create the output folder if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
output_file_path = os.path.join(OUTPUT_FOLDER, "df_company_fundamentals.parquet")

# Save the final DataFrame to Parquet format in the specified folder
df_company_fundamentals.to_parquet(output_file_path, index=False)
print(f"Data saved to '{output_file_path}'")