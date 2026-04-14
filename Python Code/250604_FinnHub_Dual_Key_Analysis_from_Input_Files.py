
# === Version 1: Settings-Driven Analysis Script ===
import os
import pandas as pd
import requests
from datetime import datetime
import math
import re

SETTINGS_FILE = 'options_analysis_settings.txt'

# === LOAD SETTINGS ===
def load_settings(filepath):
    settings = {}
    with open(filepath, 'r') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, val = line.split('=', 1)
                settings[key.strip()] = val.strip()
    return settings

settings = load_settings(SETTINGS_FILE)

# Assign config variables from settings file
CSV_INPUT = settings.get('INPUT_CSV', 'options_to_analyze.csv')
CSV_OUTPUT = settings.get('OUTPUT_CSV', 'Sorted_Options_List.csv')
DEFAULT_BETA = float(settings.get('DEFAULT_BETA', 1.5))
MYT = float(settings.get('MYT', 0.02))
LBT = float(settings.get('LBT', 0.12))
MIN_PRICE = float(settings.get('MIN_PRICE', 0.50))
MAX_PRICE = float(settings.get('MAX_PRICE', 25.00))
MIN_EXPIRY_DAYS = int(settings.get('MIN_EXPIRY_DAYS', 30))
MAX_EXPIRY_DAYS = int(settings.get('MAX_EXPIRY_DAYS', 60))
FINNHUB_KEYS = ['d0v03h9r01qmg3ujdm9gd0v03h9r01qmg3ujdma0', 'd0v1v91r01qmg3ujo8h0d0v1v91r01qmg3ujo8hg']
BASE_FINNHUB_URL = 'https://finnhub.io/api/v1'

# === HELPER FUNCTIONS ===
def extract_ticker(company_str):
    match = re.search(r'\((?:XNYS|XNAS|ARCX|XASE|XPHL|PINX|OTC):([A-Z\.]+)\)', str(company_str))
    return match.group(1) if match else None

def load_valid_tickers():
    try:
        df = pd.read_csv(CSV_INPUT, encoding='latin1')

        # Extract and clean price
        df['Price'] = df['Price'].replace(r'[\$,]', '', regex=True).replace('#FIELD!', pd.NA)
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        df = df[(df['Price'] >= MIN_PRICE) & (df['Price'] <= MAX_PRICE)]

        # Ticker: use column if valid; otherwise extract from company string
        df['Ticker'] = df.apply(
            lambda row: row['Ticker'] if pd.notna(row['Ticker']) else extract_ticker(row['Company']),
            axis=1
        )

        # Clean beta values and apply fallback
        df['BETA'] = pd.to_numeric(df['Beta'], errors='coerce').fillna(DEFAULT_BETA)
        df['BETA'] = df['BETA'].apply(lambda x: DEFAULT_BETA if x <= 0 else x)

        filtered = df[['Ticker', 'BETA']].dropna()
        filtered['Ticker'] = filtered['Ticker'].str.upper()
        return dict(zip(filtered['Ticker'], filtered['BETA']))
    except Exception as e:
        print(f"[ERROR] Failed to load or parse input CSV: {e}")
        return {}

def fetch_finnhub(endpoint, params):
    for key in FINNHUB_KEYS:
        params['token'] = key
        try:
            response = requests.get(f"{BASE_FINNHUB_URL}/{endpoint}", params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
        except:
            continue
    return {}

# === CONTRACT TYPE PARSING ===
valid_calls = {"CALL", "CALLS", "C", "c", "call", "calls"}
valid_puts = {"PUT", "PUTS", "P", "p", "put", "puts"}
valid_both = {"BOTH", "ALL", "A", "B", "b", "both", "all"}

contract_type = settings.get('CONTRACT_TYPE', 'BOTH').upper()
if contract_type in valid_calls:
    opt_types_to_check = ["CALL"]
elif contract_type in valid_puts:
    opt_types_to_check = ["PUT"]
else:
    opt_types_to_check = ["CALL", "PUT"]


# Determine contract types from settings
contract_type = settings.get('CONTRACT_TYPE', 'BOTH').upper()
if contract_type in valid_calls:
    opt_types_to_check = ["CALL"]
elif contract_type in valid_puts:
    opt_types_to_check = ["PUT"]
else:
    opt_types_to_check = ["CALL", "PUT"]

# === ANALYSIS ===

ticker_beta_map = load_valid_tickers()
today = datetime.now()
results = []

for ticker, beta in ticker_beta_map.items():
    try:
        beta = float(beta)
    except:
        beta = DEFAULT_BETA

    print(f"🔍 Analyzing {ticker}...")

    quote = fetch_finnhub("quote", {"symbol": ticker})
    current_price = quote.get("c", None)
    quote_ts = quote.get("t", None)
    quote_time = datetime.fromtimestamp(quote_ts).strftime('%Y-%m-%d %H:%M:%S') if quote_ts else 'N/A'

    if not current_price:
        print(f"⚠️ No quote for {ticker}, skipping.")
        continue

    option_data = fetch_finnhub("stock/option-chain", {"symbol": ticker})
    expiration_groups = option_data.get("data", [])

    for group in expiration_groups:
        exp_str = group.get("expirationDate")
        try:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d")
        except:
            continue

        days_to_exp = (exp_date - today).days
        if not (MIN_EXPIRY_DAYS <= days_to_exp <= MAX_EXPIRY_DAYS):
            continue

        for opt_type in opt_types_to_check:
            contracts = group.get("options", {}).get(opt_type, [])
            contracts_sorted = sorted(contracts, key=lambda x: abs(x.get("strike", 0) - current_price))[:5]

            for option in contracts_sorted:
                strike = option.get("strike", 0)
                op = option.get("bid", 0)
                if not op or op <= 0:
                    continue

                call_flag = 1 if opt_type == "CALL" else 0
                II = current_price - op if call_flag else strike - op
                MI = max(strike - II, 0)
                MY = max(MI / II, 0) if II > 0 else 0
                PB = (current_price - strike) / current_price
                LB = (current_price - II) / current_price
                MYT_flag = 1 if MY > MYT else 0
                LBT_flag = 1 if LB > LBT else 0
                include_flag = 1 if MYT_flag and LBT_flag else 0

                metric = MY * PB / math.sqrt(beta) if include_flag and beta > 0 else 0
                annual_yield = MY / (days_to_exp / 365) if days_to_exp > 0 else 0

                results.append({
                    'Ticker': ticker,
                    'Expiration Date': exp_date.strftime('%Y-%m-%d'),
                    'Option Code': f"{ticker}_{'C' if call_flag else 'P'}_{strike}_{exp_str}",
                    'Quote Timestamp': quote_time,
                    'Call?': call_flag,
                    'BETA': beta,
                    'PP': current_price,
                    'SP': strike,
                    'OP': op,
                    'II': II,
                    'MI': MI,
                    'MY': MY,
                    'PB': PB,
                    'LB': LB,
                    'MYT': MYT_flag,
                    'LBT': LBT_flag,
                    'MY*PB/SQRT(beta) w/ Conditions': metric,
                    'Days til Expiration': days_to_exp,
                    'Annualized Yield (%)': round(annual_yield * 100, 2)
                })

# === OUTPUT ===
df = pd.DataFrame(results)
df = df[df['MY*PB/SQRT(beta) w/ Conditions'] > 0]
df = df.sort_values(by='MY*PB/SQRT(beta) w/ Conditions', ascending=False)
df.to_csv(CSV_OUTPUT, index=False)
print(f"✅ Analysis complete. {len(df)} options saved to {CSV_OUTPUT}.")
