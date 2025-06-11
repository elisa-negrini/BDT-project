# Machine Learning model

This folder contains the two configurations to train and retrain the model on both historical and new incoming stream aggregated data. 

## Folders Overview

`retrain`:

- **Source**: Historical and stream aggregated data stock

- **Output**: Different model for each ticker

- **Retraining logic**: After the retraining has been completed the new models are mounted on the prediction layer volumes 

`train`:

- **Source**: Historical aggregated data stock stored in a postegres table

- **Output **: Different model for each ticker

- **Volume**: Around 13 millions historical data stocks

- **Train logic**: Once aggregated data are sent to the postgres dedicated table from which they will be taken as training set


## Model Overview

**Inputs features**:
- price_mean_1min,
- price_mean_5min,
- rice_std_5min,
- price_mean_30min,
- price_std_30min,
- size_tot_1min,
- size_tot_5min,
- size_tot_30min,
- sentiment_bluesky_mean_2hours,
- sentiment_bluesky_mean_1day,
- sentiment_news_mean_1day,
- sentiment_news_mean_3days,
- sentiment_general_bluesky_mean_2hours,
- sentiment_general_bluesky_mean_1day,
- minutes_since_open,
- day_of_week,
- day_of_month,
- week_of_year,
- month_of_year,
- market_open_spike_flag,
- market_close_spike_flag,
- eps,
- free_cash_flow,
- profit_margin,
- debt_to_equity,
- gdp_real,
- cpi,
- ffr,
- t10y,
- t2y,
- spread_10y_2y,
- unemployment

**Target Variable**
- y1 (prediction for one minute ahead)

**Choice of the model**

We use an LSTM (Long Short-Term Memory) model with a sequence length of 5 and a buffering strategy aligned to our data’s 10-second granularity. For each minute, we collect six buffers corresponding to the 00, 10, 20, 30, 40, and 50-second marks. Each buffer is paired with the subsequent minute’s value as the prediction target. This approach allows the model to preserve the one-minute granularity of the historical training set while leveraging all sub-minute aggregations. 

## Configuration

 Environment variables from .env used by the models:

- PostgreSQL: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

