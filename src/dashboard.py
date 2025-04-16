import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os

# --- Load Streaming CSVs ---
stream_dir = "/app/data/stream_csv/"

valid_csv_files = [
    os.path.join(stream_dir, f)
    for f in os.listdir(stream_dir)
    if f.endswith(".csv") and not f.startswith("_") and os.path.getsize(os.path.join(stream_dir, f)) > 0
]

stream_dataframes = []
for file in valid_csv_files:
    try:
        df = pd.read_csv(file)
        stream_dataframes.append(df)
    except Exception as e:
        print(f" Skipping {file}: {e}")

stream_df = pd.concat(stream_dataframes, ignore_index=True) if stream_dataframes else pd.DataFrame()

# --- Load Latest Batch Output CSV ---
batch_dir = "/app/data/batch_output/"
batch_df = pd.DataFrame()
try:
    batch_files = [f for f in os.listdir(batch_dir) if f.endswith(".csv") and not f.startswith("_")]
    batch_files.sort()
    if batch_files:
        latest_batch_file = os.path.join(batch_dir, batch_files[-1])
        batch_df = pd.read_csv(latest_batch_file)
    else:
        st.warning(" No batch output CSV found.")
except Exception as e:
    st.error(f" Error loading batch CSV: {e}")
    batch_df = pd.DataFrame()

# --- Load latest batch execution time from log ---
try:
    with open("/app/data/batch_log.txt", "r") as f:
        times = [float(line.strip()) for line in f.readlines() if line.strip()]
        batch_time = times[-1] if times else 0.0
except:
    batch_time = 0.0

# --- Streaming execution time ---
if not stream_df.empty and "window_start" in stream_df.columns and "window_end" in stream_df.columns:
    stream_df["window_start"] = pd.to_datetime(stream_df["window_start"])
    stream_df["window_end"] = pd.to_datetime(stream_df["window_end"])
    stream_df["window_duration_sec"] = (stream_df["window_end"] - stream_df["window_start"]).dt.total_seconds()
    stream_time = stream_df["window_duration_sec"].mean()
else:
    stream_time = 0.0

# ---  Sidebar ---
st.sidebar.header(" Info")
st.sidebar.write(f" Streaming files found: {len(valid_csv_files)}")
st.sidebar.write(f" Streaming DataFrame rows: {len(stream_df)}")
st.sidebar.write(f" Batch files found: {len(batch_files)}")
st.sidebar.write(f" Batch DataFrame rows: {len(batch_df)}")

# --- Dashboard Title ---
st.title("  Real-Time vs Batch Processing Dashboard")

# --- Main Content ---
if stream_df.empty or batch_df.empty:
    st.warning("Waiting for valid streaming or batch data...")
else:
    # Streaming Data Preview
    st.subheader("Latest Streaming Data")
    st.dataframe(stream_df.tail(10))

    # Batch Data Preview
    st.subheader("Latest Batch Aggregation")
    st.dataframe(batch_df)

    # Execution Time Comparison
    st.subheader("Execution Time Comparison")
    st.write(f"Streaming Time (mean window duration):** {stream_time:.2f} seconds")
    st.write(f"Batch Time (from log):** {batch_time:.2f} seconds")
    fig_time, ax_time = plt.subplots()
    ax_time.bar(["Streaming"], [stream_time], color="skyblue")
    ax_time.bar(["Batch"], [batch_time], color="orange")
    ax_time.set_ylabel("Time (seconds)")
    st.pyplot(fig_time)

    # Price Trends
    st.subheader("Average Price Over Time (Streaming)")
    fig_price, ax_price = plt.subplots()
    ax_price.plot(stream_df["window_start"], stream_df["avg_price"], label="Streaming Avg Price", color='blue')
    ax_price.axhline(batch_df["avg_price"].values[0], color='red', linestyle='--', label='Batch Avg Price')
    ax_price.set_xlabel("Time")
    ax_price.set_ylabel("Avg Price")
    ax_price.legend()
    st.pyplot(fig_price)

    # Trade Count Comparison (Numeric)
    st.subheader(" Trade Count Comparison (Numeric)")
    stream_trades = stream_df["trade_count"].iloc[-1]
    batch_trades = batch_df["trade_count"].iloc[0]
    st.write(f"Streaming Trade Count (latest window):** {stream_trades}")
    st.write(f"Batch Trade Count (total):** {batch_trades}")

    # Volume Comparison (Numeric)
    st.subheader("Volume Comparison (Numeric)")
    stream_volume = stream_df["total_volume"].iloc[-1]
    batch_volume = batch_df["total_volume"].iloc[0]
    st.write(f"Streaming Volume (latest window):** {stream_volume:.2f}")
    st.write(f"Batch Volume (total):** {batch_volume:.2f}")

    # Market Sentiment
    st.subheader("Market Sentiment Distribution (Streaming)")
    sentiment_df = stream_df["market_sentiment"].value_counts().reset_index()
    sentiment_df.columns = ["Sentiment", "Count"]
    st.dataframe(sentiment_df)

    fig_sentiment, ax_sentiment = plt.subplots()
    stream_df["market_sentiment"].value_counts().plot.pie(
        autopct='%1.1f%%', ax=ax_sentiment, startangle=90, shadow=True,
        colors=["#8fd3f4", "#f4a261", "#2a9d8f"]
    )
    ax_sentiment.set_ylabel("")
    st.pyplot(fig_sentiment)