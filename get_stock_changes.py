# get_stock_changes.py

import re
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone
from pandas.tseries.offsets import BDay, DateOffset
import warnings
import time
import statistics
import streamlit as st

# Suppress specific FutureWarning from yfinance/pandas
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- Streamlit Page Configuration ---
st.set_page_config(
    page_title="Manufacturing Stock Dashboard",
    layout="wide" # Use wide layout for better table display
)

# --- Cached Data Fetching Functions ---

# Cache the parsing result
@st.cache_data(ttl=3600) # Cache for 1 hour
def parse_categories_and_tickers(filename="manufacturing_stocks.md"):
    """Parses categories and their stock tickers, company names, and industries."""
    print(f"Parsing {filename}...") # Add print statement to see when it runs
    categories = {}
    current_category = None
    ticker_regex = re.compile(r"\(([-A-Z0-9.&]+?)(?:\.[A-Z]{2,})?\)")
    ticker_regex_simple = re.compile(r"\(([-A-Z0-9.&]+)\)")

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line_strip = line.strip()
                if line_strip.startswith('## '):
                    current_category = line_strip[3:].strip()
                    categories[current_category] = []
                elif line_strip.startswith('|') and current_category and not line_strip.startswith('|--'):
                    parts = [p.strip() for p in line_strip.split('|')[1:-1]]
                    if len(parts) >= 2:
                        company_ticker_part = parts[0]
                        industry_part = parts[1]
                        ticker_match = ticker_regex.search(company_ticker_part) or ticker_regex_simple.search(company_ticker_part)
                        if ticker_match:
                            ticker = ticker_match.group(1)
                            company_name = company_ticker_part[:ticker_match.start()].strip()
                            company_name = re.sub(r'\[?(.*?)\]?\(.*\)', r'\1', company_name).strip()
                            if ticker and company_name:
                                if not any(item['ticker'] == ticker for item in categories[current_category]):
                                    categories[current_category].append({
                                        "ticker": ticker,
                                        "company_name": company_name,
                                        "industry": industry_part
                                    })
    except FileNotFoundError:
        st.error(f"Error: File '{filename}' not found.")
        return None
    categories = {cat: items for cat, items in categories.items() if items}
    return categories

@st.cache_data
def adjust_ticker_for_yfinance(ticker):
    """Attempts to adjust ticker formats for yfinance compatibility."""
    # Note: This function is simple, caching might be overkill but harmless
    if ticker == '1211': return '1211.HK'
    if ticker == '1211.HK': return '1211.HK'
    if ticker == '0700.HK': return '0700.HK'
    if ticker == '0175': return '0175.HK'
    if ticker == '0992': return '0992.HK'
    if ticker == 'RNO.PA': return 'RNO.PA'
    if ticker == '7203': return '7203.T'
    if ticker == '7269': return '7269.T'
    if ticker == '005930': return '005930.KS'
    if ticker == 'NESN': return 'NESN.SW'
    if ticker == 'NSRGY': return 'NSRGY'
    if ticker == 'VOW3': return 'VOW3.DE'
    if ticker == 'ITX': return 'ITX.MC'
    if ticker == 'M&M': return 'M&M.NS'
    if ticker == 'RIL': return 'RELIANCE.NS'
    if any(ticker.endswith(s) for s in ['.HK', '.PA', '.T', '.KS', '.SW', '.DE', '.MC', '.NS']):
         return ticker
    if '.' in ticker:
         parts = ticker.split('.')
         if len(parts[-1]) < 2 or len(parts[-1]) > 3:
              # print(f"Warning: Removing potentially invalid suffix from {ticker} -> {parts[0]}") # Avoid printing warnings in Streamlit app
              return parts[0]
         else:
              return ticker
    return ticker

# Cache the price fetching for each ticker for a shorter duration (e.g., 15 mins)
@st.cache_data(ttl=900)
def get_stock_data_for_ticker(adjusted_ticker):
    """Fetches historical data for a single ticker and calculates changes."""
    print(f"Fetching data for {adjusted_ticker}...") # See when network requests happen
    stock = yf.Ticker(adjusted_ticker)

    # Get info - might fail for some tickers, handle gracefully
    try:
        info = stock.info
        if not info or (info.get('marketState') is None and info.get('regularMarketPrice') is None):
            print(f"Skipping {adjusted_ticker} (invalid/missing essential market data)")
            return {"today_price": None, "5d_ago_price": None, "1mo_ago_price": None, "change_5d": None, "change_1mo": None, "error": "Invalid/Missing Info"}
    except Exception as e:
        print(f"Failed to get info for {adjusted_ticker}. Error: {type(e).__name__}: {e}")
        return {"today_price": None, "5d_ago_price": None, "1mo_ago_price": None, "change_5d": None, "change_1mo": None, "error": f"Info Fetch Error: {e}"}

    # Calculate target dates (relative to now)
    # Use timezone-aware current time
    now_aware = datetime.now(timezone.utc)
    today_date = now_aware.date()
    # Use pandas offsets correctly with timezone-aware time
    date_5d_ago = (pd.to_datetime(now_aware) - BDay(5)).date()
    date_1mo_ago = (pd.to_datetime(now_aware) - DateOffset(months=1)).date()

    price_today = get_closest_price_yf(stock, today_date)
    price_5d = get_closest_price_yf(stock, date_5d_ago)
    price_1mo = get_closest_price_yf(stock, date_1mo_ago)

    change_5d_pct = None
    change_1mo_pct = None
    if price_today is not None and price_5d is not None:
        if price_5d != 0: change_5d_pct = ((price_today - price_5d) / price_5d) * 100
        elif price_today == 0: change_5d_pct = 0.0
        else: change_5d_pct = float('inf')
    if price_today is not None and price_1mo is not None:
        if price_1mo != 0: change_1mo_pct = ((price_today - price_1mo) / price_1mo) * 100
        elif price_today == 0: change_1mo_pct = 0.0
        else: change_1mo_pct = float('inf')

    return {
        "today_price": price_today,
        "5d_ago_price": price_5d,
        "1mo_ago_price": price_1mo,
        "change_5d": change_5d_pct,
        "change_1mo": change_1mo_pct,
        "error": None # Indicate success
    }

def get_closest_price_yf(ticker_obj, target_date):
    """Gets the closing price for the closest trading day on or before the target date."""
    # Note: This function is called by the cached get_stock_data_for_ticker, so doesn't need its own cache decorator
    try:
        start_fetch = target_date - timedelta(days=10)
        end_fetch = target_date + timedelta(days=1)
        hist = ticker_obj.history(start=start_fetch, end=end_fetch, auto_adjust=True)
        if hist.empty: return None

        hist.index = pd.to_datetime(hist.index)
        target_dt = pd.to_datetime(target_date)

        if hist.index.tz is not None:
            hist_index_naive = hist.index.tz_convert('UTC').tz_localize(None)
        else:
            hist_index_naive = hist.index

        hist_filtered = hist[hist_index_naive.normalize() <= target_dt]
        if hist_filtered.empty: return None
        return hist_filtered['Close'].iloc[-1]

    except Exception as e:
        # Log error for debugging if needed, but return None to calling function
        print(f"Error in get_closest_price_yf for {ticker_obj.ticker} around {target_date}: {e}")
        return None

# --- Styling Function --- 
def style_negative_red(value):
    """Styles negative numbers red and positive numbers green."""
    if value is None or pd.isna(value):
        return '' # No style for N/A
    color = 'red' if value < 0 else 'green' if value > 0 else 'black' # Black for zero
    return f'color: {color}'

# --- Main App Logic ---

st.title("🏭 Manufacturing Stock Dashboard")

# Load categories and tickers (cached)
categories = parse_categories_and_tickers()

if not categories:
    st.warning("Could not load categories or tickers from markdown file.")
    st.stop()

# Prepare data structure to hold results
results = {cat: {} for cat in categories}
failed_tickers = []
processed_tickers_info = {} # Store info for all processed tickers

# Fetch data for all tickers
with st.spinner("Fetching latest stock data..."):
    unique_tickers_to_fetch = set()
    for category, ticker_info_list in categories.items():
        for ticker_info in ticker_info_list:
            raw_ticker = ticker_info['ticker']
            adjusted_ticker = adjust_ticker_for_yfinance(raw_ticker)
            if adjusted_ticker:
                unique_tickers_to_fetch.add((adjusted_ticker, raw_ticker, ticker_info['company_name'], ticker_info['industry']))

    for adjusted_ticker, raw_ticker, company_name, industry in unique_tickers_to_fetch:
        stock_data = get_stock_data_for_ticker(adjusted_ticker)
        processed_tickers_info[adjusted_ticker] = {
            **stock_data,
            "raw_ticker": raw_ticker,
            "company_name": company_name,
            "industry": industry
        }
        if stock_data["error"]:
             failed_tickers.append(f"{adjusted_ticker} ({stock_data['error']})")

# Organize results by category
for category, ticker_info_list in categories.items():
    for ticker_info in ticker_info_list:
        raw_ticker = ticker_info['ticker']
        adjusted_ticker = adjust_ticker_for_yfinance(raw_ticker)
        if adjusted_ticker in processed_tickers_info:
            results[category][adjusted_ticker] = processed_tickers_info[adjusted_ticker]

# Calculate category averages
category_averages = {}
summary_data_list = [] # Use a list of dicts first
for category, tickers_data in results.items():
    valid_5d_changes = [data.get('change_5d') for data in tickers_data.values() if data.get('change_5d') is not None and data.get('change_5d') != float('inf')]
    valid_1mo_changes = [data.get('change_1mo') for data in tickers_data.values() if data.get('change_1mo') is not None and data.get('change_1mo') != float('inf')]

    avg_5d = statistics.mean(valid_5d_changes) if valid_5d_changes else None
    avg_1mo = statistics.mean(valid_1mo_changes) if valid_1mo_changes else None

    category_averages[category] = {"avg_5d": avg_5d, "avg_1mo": avg_1mo}
    summary_data_list.append({
        "Category": category,
        "Avg 5d Change (%)": avg_5d, # Keep as number for styling
        "Avg 1mo Change (%)": avg_1mo # Keep as number for styling
    })

# --- Display Dashboard ---
st.header("📊 Category Performance Summary")
if summary_data_list:
    summary_df = pd.DataFrame(summary_data_list).set_index("Category")
    # Apply styling and formatting
    st.dataframe(
        summary_df.style
        .applymap(style_negative_red, subset=["Avg 5d Change (%)", "Avg 1mo Change (%)"])
        .format({ # Format numbers after styling
            "Avg 5d Change (%)": "{:.2f}%",
            "Avg 1mo Change (%)": "{:.2f}%"
        }, na_rep="N/A"),
        use_container_width=True
    )
else:
    st.write("No summary data available.")

# Display tables for each category
for category, tickers_data in results.items():
    st.header(f"📁 {category}")
    category_df_data = []
    sorted_tickers_keys = sorted(tickers_data.keys())
    for adjusted_ticker in sorted_tickers_keys:
        data = tickers_data[adjusted_ticker]
        category_df_data.append({
            "Company Name": data.get('company_name', 'N/A'),
            "Industry": data.get('industry', 'N/A'),
            "Ticker": adjusted_ticker,
            # Keep price as number for potential future styling, format later
            "Current Price": data['today_price'],
            # Keep changes as numbers for styling
            "5d Change (%)": data['change_5d'],
            "1mo Change (%)": data['change_1mo']
        })

    if category_df_data:
        category_df = pd.DataFrame(category_df_data).set_index("Company Name")
        # Apply styling and formatting
        st.dataframe(
            category_df.style
            .applymap(style_negative_red, subset=["5d Change (%)", "1mo Change (%)"])
            .format({
                "Current Price": "{:.2f}",
                "5d Change (%)": "{:.2f}%",
                "1mo Change (%)": "{:.2f}%"
            }, na_rep="N/A"),
            use_container_width=True
        )
    else:
        st.write("No data available for this category.")

# Display failed tickers if any
if failed_tickers:
    st.warning("Failed to fetch or process data for some tickers:")
    st.json(failed_tickers) # Display as JSON for clarity

# --- Auto-Refresh Logic ---
# Simple timer-based refresh every N seconds
refresh_interval_seconds = 3600 # Refresh every hour
st.write(f"Page will refresh automatically every {refresh_interval_seconds} seconds.")
st.caption(f"Last data fetch attempt initiated around: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')}")

time.sleep(refresh_interval_seconds)
st.rerun()

# Note: For true "live" (sub-minute) updates, yfinance might not be suitable due to API rate limits
# and the nature of free EOD data. You might need a paid, real-time data provider.
# Streamlit's st.experimental_fragment could also be used for more granular updates if needed. 