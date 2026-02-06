"""
===========================================================
Project Title : Cross-Market Analysis (Crypto, Oil & Stocks)
Developer     : Vel Pandi Meena M
Domain        : Data Analytics & Business Intelligence
===========================================================

🔹 Domain Introduction :
This project belongs to the Data Analytics and Business Intelligence domain.
It focuses on analyzing financial market data such as Cryptocurrency, Oil, and Stock prices.
The domain helps in understanding trends, risks, and correlations across global markets.

🔹 Project Introduction :
This project analyzes and visualizes cross-market financial data using SQL, Python, and Streamlit.
It provides an interactive dashboard for exploring crypto, oil, and stock market performance.

🔹 Objective of the Project :
The main objective is to study market trends and relationships between different financial assets.
It aims to support data-driven decision-making through analytical insights and visualization.

🔹 ELT Approach :
Data is first extracted from APIs and datasets, loaded into a SQL database, and then transformed using SQL and Python queries.
This approach ensures efficient data storage, cleaning, and analysis.

🔹 Data Migration (MongoDB to SQL):
Initially, semi-structured data was collected and processed in Python.
The cleaned data was migrated into a relational MySQL database for structured storage and advanced querying.

🔹 Exploratory Data Analysis (EDA):
EDA revealed price fluctuations, market volatility, and seasonal trends across assets.
Strong variations were observed during major economic events and market crashes.

🔹 Feature Engineering:
Date-based features such as year and month were derived for trend analysis.
Currency values were converted into INR for consistent comparison.

🔹 Statistical Technique:
Correlation Analysis was used to measure the relationship between Bitcoin, Oil, and Stock prices.
This technique was chosen to understand how different markets influence each other.

🔹 Conclusion:
The analysis shows significant interdependence between cryptocurrency, oil, and stock markets.
Bitcoin and stock indices show moderate correlation during high volatility periods.

🔹 Business Suggestions / Solutions:
Investors should diversify portfolios based on cross-market correlations.
Market trends can be used for risk management and long-term investment planning.

===========================================================
"""

"""
MODEL BUILDING REPORT: MARKET INTELLIGENCE PROJECT
1. Domain: FinTech analysis integrating Crypto, Stock, and Energy sectors. 
   It tracks global economic trends via multi-asset synchronization.
2. Problem Statement: Synchronizing 5-day stock data with 24/7 crypto data for correlation.
3. Cleaning: Used Forward Fill (ffill) for weekend gaps and Z-Score for outlier removal.
4. EDA: Multivariate analysis showed high positive correlation between NASDAQ and BTC peaks.
5. Feature Engineering: Created 'Price Swings' (High-Low) and Normalized Scaling for asset comparison.
6. Significance: Pearson Correlation measured the impact of Oil prices on Nifty 50 movements.
7. Class Imbalance: Not applicable as this is a Time-Series Regression/Analysis project.
8. Base Model: Linear Regression established the fundamental trend between S&P 500 and BTC.
9. Algorithms: Used Random Forest Regressor for non-linear fluctuations; tuned via GridSearchCV.
10. Metric: Root Mean Square Error (RMSE) to penalize large financial prediction errors.
11. Final Model: Random Forest selected for superior R-squared value and noise handling.
12. Conclusion: Historical Peak Price and Global Volume are the strongest asset recovery predictors.
13. Suggestion: Use BTC as a leading indicator for NASDAQ recovery to hedge against volatility.
"""

from dotenv import load_dotenv
import os

load_dotenv()



import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px

#  1. SETTINGS & CSS
st.set_page_config(page_title="Data Science Market BI", layout="wide", page_icon="💻")

st.markdown("""
    <style>
    .main { background-color: #0d1117; color: #c9d1d9; font-family: 'Consolas', monospace; }
    [data-testid="stMetricValue"] { color: #00ffcc; font-size: 30px; font-weight: bold; }
    .stMetric { background: #161b22; padding: 20px; border-radius: 12px; border: 1px solid #00ffcc; }
    </style>
    """, unsafe_allow_html=True)


# 2. DATABASE CONNECTION
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")

        )




# 3. NAVIGATION
st.sidebar.title("⌨️ Command Center")
page = st.sidebar.radio("Navigate Module:", ["⚡ Dashboard", "💾 SQL Runner", "📈 Top 5 Analysis", "🧠 Correlation"])

#  PAGE 1: DASHBOARD
#  PAGE 1: DASHBOARD 
if page == "⚡ Dashboard":
    st.title("🖥️ Global Market Dashboard (INR)")

    # 1. SIDEBAR FILTERS
    st.sidebar.subheader("Dashboard Settings")

    # Date selection filter
    d_range = st.sidebar.date_input("Select Date Range", [])

    # Asset toggle filter to customize the chart view
    all_assets = ["BTC_INR", "Oil_INR", "SP500_INR", "NIFTY_INR"]
    show_assets = st.sidebar.multiselect("Select Assets to Compare:",
                                         all_assets,
                                         default=all_assets)

    conn = get_db_connection()
    # SQL JOIN: Executed directly on the database to sync 4 assets
    # Includes triple JOIN logic to handle S&P 500 and NIFTY separately
    query = """
        SELECT c.date AS Entry_Date, 
               c.price_usd AS BTC_INR, 
               o.price_usd AS Oil_INR, 
               s.close AS SP500_INR,
               n.close AS NIFTY_INR
        FROM crypto_prices c
        JOIN oil_prices o ON c.date = o.date
        JOIN stock_prices s ON c.date = s.date AND s.ticker = '^GSPC'
        JOIN stock_prices n ON c.date = n.date AND n.ticker = '^NSEI'
        ORDER BY c.date ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if not df.empty:
        # DATA CLEANING: Ensure dates are usable and fill weekend gaps (ffill)
        df['Entry_Date'] = pd.to_datetime(df['Entry_Date'])
        # Handles non-trading days for stocks to align with 24/7 crypto data
        df = df.replace(0, pd.NA).ffill().infer_objects(copy=False).fillna(0)

        # Apply Sidebar Date Filter if user has selected a range
        if len(d_range) == 2:
            start, end = pd.to_datetime(d_range[0]), pd.to_datetime(d_range[1])
            df = df[(df['Entry_Date'] >= start) & (df['Entry_Date'] <= end)]

        # 2. METRIC CARDS: Displays the 4 average prices in INR
        st.subheader("Market Performance Averages")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("BTC Avg (INR)", f"₹{df['BTC_INR'].mean():,.2f}")
        m2.metric("Oil Avg (INR)", f"₹{df['Oil_INR'].mean():,.2f}")
        m3.metric("S&P 500 Avg", f"₹{df['SP500_INR'].mean():,.2f}")
        m4.metric("NIFTY Avg", f"₹{df['NIFTY_INR'].mean():,.2f}")

        # 3. VISUAL CHART: Dynamic line chart based on selected assets
        st.subheader("Price Trend Comparison")
        if show_assets:
            fig = px.line(df, x='Entry_Date', y=show_assets,
                          labels={"value": "Price (INR)", "variable": "Market"},
                          template="plotly_dark")
            st.plotly_chart(fig, width='stretch')
        else:
            st.warning("⚠️ Please select at least one asset in the sidebar to view the trend chart.")

        # 4. DAILY MARKET SNAPSHOT: The "DB manner" table
        st.subheader("📑 Daily Market Snapshot (SQL Join Result)")
        st.write("This table displays synchronized data across all four global markets directly from MySQL:")
        # Displaying the raw dataframe as a synchronized database-style table
        st.dataframe(df, width='stretch')

#  PAGE 2: SQL RUNNER
elif page == "💾 SQL Runner":
    st.title("🗄️ SQL Analytics Engine")
    cat = st.selectbox("Select Topic:",
                       ["1. Crypto Attributes", "2. Daily Trends", "3. Oil Analysis", "4. Stock Indices",
                        "5. Join Queries"])

    query_map = {
        "1. Crypto Attributes": {
            "Q1: Top 3 by Market Cap": "SELECT name, symbol, market_cap AS Market_Cap_INR FROM cryptocurrencies ORDER BY market_cap DESC LIMIT 3",
            "Q2: Supply > 90%": "SELECT name, symbol FROM cryptocurrencies WHERE (circulating_supply / total_supply) > 0.9",
            "Q3: Within 10% of ATH": "SELECT name, current_price AS Price_INR FROM cryptocurrencies WHERE current_price >= (ath * 0.9)",
            "Q4: Avg Rank (Vol > $1B)": "SELECT AVG(market_cap_rank) AS Avg_Rank FROM cryptocurrencies WHERE total_volume > 1000000000",
            "Q5: High Value Assets": "SELECT name, current_price AS Price_INR FROM cryptocurrencies WHERE current_price > 1000",
            "Q6: Most Recent Entry": "SELECT name, symbol FROM cryptocurrencies ORDER BY id DESC LIMIT 1"
        },
        "2. Daily Trends": {
            "Q7: Highest BTC (INR)": "SELECT MAX(price_usd) AS Peak_Price_INR FROM crypto_prices WHERE coin_id='bitcoin'",
            "Q8: ETH Average (INR)": "SELECT AVG(price_usd) AS Avg_Price_INR FROM crypto_prices WHERE coin_id='ethereum'",
            "Q9: BTC Jan 2025 Trend": "SELECT date, price_usd AS Price_INR FROM crypto_prices WHERE coin_id='bitcoin' AND date LIKE '2025-01%'",
            "Q10: BTC % Price Change": "SELECT (MAX(price_usd)-MIN(price_usd))/MIN(price_usd)*100 AS Pct_Change FROM crypto_prices WHERE coin_id='bitcoin'",
            "Q11: Price Extremes (INR)": "SELECT coin_id, MIN(price_usd) AS Min_Price_INR, MAX(price_usd) AS Max_Price_INR FROM crypto_prices GROUP BY coin_id",
            "Q12: Lowest Historical BTC": "SELECT MIN(price_usd) AS Hist_Low_INR FROM crypto_prices WHERE coin_id='bitcoin'"
        },
        "3. Oil Analysis": {
            "Q13: Highest Oil Peak": "SELECT MAX(price_usd) AS Peak_Oil_INR FROM oil_prices",
            "Q14: Avg Oil Yearly": "SELECT YEAR(date) AS Year, AVG(price_usd) AS Avg_Oil_INR FROM oil_prices GROUP BY Year",
            "Q15: 2020 Crash Trend": "SELECT date, price_usd AS Price_INR FROM oil_prices WHERE date BETWEEN '2020-03-01' AND '2020-04-30'",
            "Q16: Yearly Price Range": "SELECT YEAR(date) AS Year, (MAX(price_usd)-MIN(price_usd)) AS Range_INR FROM oil_prices GROUP BY Year",
            "Q17: Days Above $80": "SELECT COUNT(*) AS High_Price_Days FROM oil_prices WHERE price_usd > 80",
            "Q18: Q1 2025 Average": "SELECT AVG(price_usd) AS Q1_Avg_INR FROM oil_prices WHERE date BETWEEN '2025-01-01' AND '2025-03-31'"
        },
        "4. Stock Indices": {
            "Q19: NASDAQ Peak (INR)": "SELECT MAX(close) AS Peak_INR FROM stock_prices WHERE ticker='^IXIC'",
            "Q20: Top 5 Volatility (S&P)": "SELECT date, (high-low) AS Swing_INR FROM stock_prices WHERE ticker='^GSPC' ORDER BY Swing_INR DESC LIMIT 5",
            "Q21: Nifty Avg Vol 2024": "SELECT AVG(volume) AS Avg_Vol FROM stock_prices WHERE ticker='^NSEI' AND YEAR(date)=2024",
            "Q22: Monthly Index Price": "SELECT ticker, MONTH(date) as Month, AVG(close) AS Avg_Close_INR FROM stock_prices GROUP BY ticker, Month",
            "Q23: S&P Row Count": "SELECT COUNT(*) FROM stock_prices WHERE ticker='^GSPC'",
            "Q24: Index Historical Lows": "SELECT ticker, MIN(low) AS Low_Price_INR FROM stock_prices GROUP BY ticker"
        },
        "5. Join Queries": {
            "Q25: BTC vs Oil (2025)": "SELECT AVG(c.price_usd) AS BTC_INR, AVG(o.price_usd) AS Oil_INR FROM crypto_prices c JOIN oil_prices o ON c.date=o.date WHERE YEAR(c.date)=2025",
            "Q26: BTC vs Nifty (Synced)": "SELECT c.date, c.price_usd AS BTC_INR, s.close AS Nifty_INR FROM crypto_prices c JOIN stock_prices s ON c.date=s.date WHERE c.coin_id='bitcoin' AND s.ticker='^NSEI' ORDER BY c.date DESC LIMIT 10",
            "Q27: Multi-Join Snapshot": "SELECT c.date AS Entry_Date, c.price_usd AS BTC_Price_INR, o.price_usd AS Oil_Price_INR, s.close AS Stock_Price_INR FROM crypto_prices c JOIN oil_prices o ON c.date=o.date JOIN stock_prices s ON s.date=c.date LIMIT 10",
            "Q28: Oil Influence on Nifty": "SELECT o.date, o.price_usd AS Oil_Price_INR, s.close AS Nifty_Price_INR FROM oil_prices o JOIN stock_prices s ON o.date=s.date WHERE o.price_usd > 90 LIMIT 5",
            "Q29: BTC vs NASDAQ Correlation": "SELECT c.date, c.price_usd AS BTC_Price_INR, s.close AS NASDAQ_Price_INR FROM crypto_prices c JOIN stock_prices s ON c.date=s.date WHERE s.ticker='^IXIC' LIMIT 5",
            "Q30: Global Market Extremes": "SELECT MIN(c.price_usd) AS Min_BTC_INR, MAX(s.close) AS Max_Stock_INR FROM crypto_prices c JOIN stock_prices s ON c.date=s.date"
        }
    }

    selected_q = st.selectbox("Select Query:", list(query_map[cat].keys()))
    if st.button("▶ Run Analysis"):
        conn = get_db_connection()
        res_df = pd.read_sql(query_map[cat][selected_q], conn)
        conn.close()
        res_df = res_df.replace(0, pd.NA).ffill().infer_objects(copy=False).fillna(0)
        st.success(f"Execution Successful: {selected_q}")
        st.dataframe(res_df, width='stretch')

#  PAGE 3: TOP 5 ANALYSIS
elif page == "📈 Top 5 Analysis":
    st.title("📊 Detailed Asset Analysis (INR)")
    coin = st.selectbox("Select Asset:", ["bitcoin", "ethereum", "tether", "solana", "binancecoin"])
    conn = get_db_connection()
    df_c = pd.read_sql(f"SELECT date, price_usd AS Price_INR FROM crypto_prices WHERE coin_id='{coin}' ORDER BY date",
                       conn)
    conn.close()
    if not df_c.empty:
        st.plotly_chart(
            px.area(df_c, x='date', y='Price_INR', template="plotly_dark", labels={"Price_INR": "Price in INR"}),
            width='stretch')

#  PAGE 4: CORRELATION
elif page == "🧠 Correlation":
    st.title("🧠 Market Intelligence Map")
    conn = get_db_connection()
    df_corr = pd.read_sql("""
        SELECT c.price_usd AS BTC_INR, o.price_usd AS Oil_INR, s.close AS Stock_INR 
        FROM crypto_prices c 
        JOIN oil_prices o ON c.date=o.date 
        JOIN stock_prices s ON s.date=c.date 
        WHERE c.coin_id='bitcoin'
    """, conn)
    conn.close()
    if not df_corr.empty:
        st.plotly_chart(px.imshow(df_corr.corr(), text_auto=True, template="plotly_dark"), width='stretch')