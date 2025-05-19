| **Feature Name**           | **Description**                                   | **Window**   | **Type**            |
| -------------------------- | ------------------------------------------------- | ------------ | ------------------- |
`price_mean_1min`          | % price change vs previous minute                 | 1 min        | Stock-derived       |
| `price_mean_5min`          | % price change over last 5 minutes                | 5 min        | Stock-derived       |
| `log_return_5min`          | Log return over 5 minutes                         | 5 min        | Stock-derived       |
| `price_std_15min`          | Price standard deviation (volatility)             | 15 min       | Stock-derived       |
| `price_max_30min`          | Max price in last 30 minutes                      | 30 min       | Stock-derived       |

| `Size_1min`              | Sum of trade sizes in the last minute             | 1 min        | Size              |
| `Size_diff_1min`         | % change in Size compared to previous min       | 1 min        | Size              |
| `Size_mean_15min`        | Rolling average Size                            | 15 min       | Size              |
| `Size_std_15min`         | Rolling Size volatility                         | 15 min       | Size              |

| `rsi_14`                   | Relative Strength Index (momentum)                | 14 min       | Technical indicator |
| `ema_10`                   | Exponential Moving Average                        | 10 min       | Technical indicator |
| `macd_line`                | MACD line (EMA12 - EMA26)                         | \~26 min     | Technical indicator |
| `bollinger_width`          | Width of Bollinger Bands                          | 20 min       | Technical indicator |
| `obv`                      | On-Balance Volume (cumulative)                    | All data     | Technical indicator |

| `GDP`               | Gross Domestic Product (nominal, current dollars)               | Quarterly     | Level (forward-filled)        |
| `GDPC1`             | Real GDP (adjusted for inflation, chained 2012 dollars)         | Quarterly     | Level (forward-filled)        |
| `QoQ`               | % change in GDP quarter-over-quarter                            | Quarterly     | Change                        |
| `CPI`               | Consumer Price Index – headline inflation                       | Monthly       | Level (forward-filled)        |
| `Fed_Funds_Rate`    | Federal Reserve policy interest rate                            | Monthly       | Level (forward-filled)        |
| `Treasury_10Y`      | 10-year Treasury yield (long-term expectations)                 | Daily         | Level (forward-filled)        |
| `Treasury_2Y`       | 2-year Treasury yield (short-term rate sensitive to Fed policy) | Daily         | Level (forward-filled)        |
| `Yield_Spread`      | Difference between 10Y and 2Y yields (inversion signal)         | Daily         | Derived (forward-filled)      |
| `Unemployment_Rate` | % of unemployed individuals actively seeking work               | Monthly       | Level (forward-filled)        |
| `UMich_Sentiment`   | University of Michigan Consumer Sentiment Index                 | Monthly       | Survey Score (forward-filled) |

Bluesky/reddit
| `sentiment_mean_2hours`  | Avg. sentiment from news/social in last 2 hours                | 2 hours     | Sentiment     |
| `sentiment_mean_1day`    | Avg. sentiment over the last day                               | 1 day       | Sentiment     |
| `sentiment_delta_2hour_1day` | Δ between 2hours and 1day sentiment means                  | 30 min – 2h | Sentiment     |
| `posts_volume_1h`        | Number of posts mentioning the ticker in the last hour         | 1 hour      | Volume signal |

Newspapers
| `sentiment_mean_1d`      | Avg. sentiment from news/social in last 1d                     | 1d          | Sentiment     |
| `sentiment_mean_3d`      | Avg. sentiment over the last 3d                                | 3d          | Sentiment     |
| `sentiment_delta_1d_3d`  | Δ between 1d and 3d sentiment means                            | 1d – 3d     | Sentiment     |
| `news_mentions_3days`    | Number of news articles referencing the ticker in last 3days   | 3days       | Volume signal |

| `minutes_since_open`       | Minutes since market open (9:30 AM)                        | Intraday     | Temporal               |
| `sin_minute`, `cos_minute` | Sin/Cos transf. of `min.._since_o` (capture cyclicity)     | Intraday     | Temporal               |
| `day_of_week`              | Day of the week (0=Monday, ..., 4=Friday)                  | Weekly       | Temporal               |
| `day_of_month`             | Day of the month (1–31)                                    | Monthly      | Temporal               |
| `week_of_year`             | Week number in the year (0–52)                             | Yearly       | Temporal               |
| `month_of_year`            | Month number (1–12)                                        | Yearly       | Temporal               |
| `is_month_end`             | 1 if the current day is the last trading day of the month  | Monthly      | Binary flag            |
| `is_quarter_end`           | 1 if         //                    of a financial quarter  | Quarterly    | Binary flag            |
| `holiday_proximity`        | Distance(in days) to the next/previous U.S. market holiday | Daily        | Numerical              |
| `market_open_spike_flag`   | 1 if the current min is within first 5 min of market open  | Intraday     | Binary flag            |
| `market_close_spike_flag`  | 1 if the current min is within last 5 min of market close  | Intraday     | Binary flag            |

| **Feature Name**     | **Formula**                               |**Description** 
| `profit_margin`      | `netIncome / revenue`                     | Measures net profitability: how much net income is generated per 
   ---                                                               dollar of revenue. Indicates the company’s ability to convert sales 
   ---                                                               into actual profit. |
| `operating_margin`   | `operatingIncome / revenue`               | Captures core operating efficiency before taxes and interest. 
   ---                                                               Highlights how well the company turns revenue into operating 
   ---                                                               profit.                       |
| `debt_to_equity`     | `totalDebt / totalStockholdersEquity`     | Indicates leverage. Higher values imply reliance on debt financing, 
   ---                                                               which can increase risk in unstable    ---                                                               markets.                                         |
| `eps_growth_yoy`     | `(EPS_t - EPS_t-1) / EPS_t-1`             | Year-over-year growth of earnings per share. Helps the model gauge
   ---                                                               whether earnings are accelerating or    ---                                                               declining.                                       |
| `revenue_growth_yoy` | `(revenue_t - revenue_t-1) / revenue_t-1` | Measures the company’s sales expansion or contraction over time. Useful 
   ---                                                               for capturing business    ---                                                               momentum.                                                 |

IMPORTANTE: per questi ultimi il modello verrà allenato sul dato dell'anno precedente, dato che ad esempio ad oggni abbiamo quel del 2024 ma non del 2025

al modello dovremmo dire che quando la borsa è chiusa non deve considerare price mean 5 min, ma l'altra variabile fatta sugli ultimi 20