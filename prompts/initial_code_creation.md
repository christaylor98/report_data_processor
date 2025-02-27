# Project Overview
I need a **Python-based data pipeline** that processes **JSNL files** into **Parquet** files using **DuckDB**. The system should run every **10 minutes**, merge files **hourly, daily, and monthly**, and handle **automatic recovery and alerts**.

# Technologies & Requirements:
- **Python + DuckDB** for Parquet processing.
- **Systemd** for scheduling with retries.
- **Bash scripts** for auto-recovery.
- **Pushover API** for failure alerts.

---

## **1️⃣ Python Code for Processing JSNL to Parquet**
- Read JSNL files from a **specific directory**.
- Parse them into structured data.
- Store important data in **MariaDB** (trades, equity).
- Store other high-volume data in **Parquet** using DuckDB.
- Batch new Parquet files **every 10 minutes**.
- Merge **hourly, daily, and monthly** Parquet files.


Example of the JSNL file lines (last one includes the equity):
```
{"log_id": "0xc0ccce9d56842ff", "timestamp": 1740562241.5720,  "component": "strand", "type": "real_time_strategy", "value": [{"c":"purple","n":"[0] SMA large","st":1740562241.572,"sv":21220.92480650154,"t":"sr","w":2.0},{"c":"blue","n":"[0] SMA small","st":1740562241.572,"sv":21187.722136563876,"t":"sr","w":2.0},{"c":"green","d":"[0] local high","l":{"close":22179.7,"high":22180.7,"low":22177.5,"n":648,"open":22178.4,"timestamp":1740145560.0,"volume":106},"t":"l","w":2.0},{"c":"red","d":"[0] local low","l":{"close":20952.8,"high":20967.2,"low":20949.2,"n":1513,"open":20958.2,"timestamp":1740501120.0,"volume":1223},"t":"l","w":2.0},{"a":"-3.4844e-2","b":"2.1223e4","c":"#F28500","d":"[0] trend lr","n":"1.2000e2","p":685,"q":"No trend","r2":"2.5350e-1","s":2.0,"t":"lrf"},{"a":"-4.2278e-2","b":"2.1534e4","c":"red","d":"[0] down run","dm":"-6.4068e2","mn":"2.0953e4","mx":"2.2180e4","n":3611,"s":2.0,"t":"lr","um":"1.9075e2"},{"a":"-3.4859e-2","b":"2.1222e4","c":"green","d":"[0] up run","dm":"-1.1087e2","mn":"2.0953e4","mx":"2.1278e4","n":686,"s":2.0,"t":"lr","um":"2.5255e2"},{"cg":"[0] trending info up","col":"","diag":[["adx","25.856"],["vix","3.068"],["atr","7.240"],["trend_strength","4.586"],["is_trending","true"]],"dir":"up","id":"efg","res":true,"rsn":"Trending values","t":"cg","tn":"info","tr":-1.0},{"cg":"[0] trending exit up","col":"","diag":[["fast_sma","21187.722"],["slow_sma","21220.925"]],"dir":"up","id":"efg","res":true,"rsn":"Exit long position - Moving average crossover","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"[0] trending exit up","col":"","diag":[["lr_slope","-0.035"]],"dir":"up","id":"efg","res":true,"rsn":"Exit long position - Trend reversal","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"[0] trending exit up","col":"","diag":[["atr","7.240"],["price","21273.050"],["slow_sma","21220.925"]],"dir":"up","id":"efg","res":false,"rsn":"Exit long position - Stop loss (2 ATR)","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"[0] trending exit up","col":"","diag":[["atr","7.240"],["price","21273.050"],["entry_price","none"]],"dir":"up","id":"efg","res":false,"rsn":"Exit long position - Take profit (3 ATR)","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"[0] trending exit up","col":"","diag":[["m","true"],["d","40996.61"]],"dir":"up","id":"efg","res":false,"rsn":"Trading not allowed - In market hours only","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"[0] trending enter up","col":"","diag":[["sma_small","21187.722"],["sma_large","21220.925"],["candle","21273.050"]],"dir":"up","id":"efg","res":false,"rsn":"Long entry - Trend alignment","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"[0] trending enter up","col":"","diag":[["trend_strength","4.586"],["lr_slope","-0.035"],["lr_r2","0.254"],["min_slope","0.010"],["min_r2","0.519"]],"dir":"up","id":"efg","res":false,"rsn":"Long entry - Trend strength","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"[0] trending enter up","col":"","diag":[["atr","7.240"],["volatility_threshold","2.000"],["vol_calc","425.461"]],"dir":"up","id":"efg","res":true,"rsn":"Long entry - Volatility check","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"[0] trending enter up","col":"","diag":[],"dir":"up","id":"efg","res":true,"rsn":"Trade entry - In Market Time","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"[0] trending exit up","dir":"up","res":true,"t":"cgr"}]}
{"log_id": "0x7ff447a2bb250140", "timestamp": 1740562241.5720,  "component": "strand", "type": "real_time_strategy", "value": [{"c":"green","d":"[0] local high","l":{"close":22179.7,"high":22180.7,"low":22177.5,"n":648,"open":22178.4,"timestamp":1740145560.0,"volume":106},"t":"l","w":2.0},{"c":"red","d":"[0] local low","l":{"close":20952.8,"high":20967.2,"low":20949.2,"n":1513,"open":20958.2,"timestamp":1740501120.0,"volume":1223},"t":"l","w":2.0},{"a":"4.3048e-1","b":"2.1208e4","c":"#7900f2","d":"[0] trend lr","n":"1.2000e2","p":120,"q":"Moderate trend","r2":"6.3170e-1","s":2.0,"t":"lrf"},{"a":"-4.2278e-2","b":"2.1534e4","c":"red","d":"[0] down run","dm":"-6.4068e2","mn":"2.0953e4","mx":"2.2180e4","n":3611,"s":2.0,"t":"lr","um":"1.9075e2"},{"a":"-3.4859e-2","b":"2.1222e4","c":"green","d":"[0] up run","dm":"-1.1087e2","mn":"2.0953e4","mx":"2.1278e4","n":686,"s":2.0,"t":"lr","um":"2.5255e2"},{"c":"blue","n":"[0] sliding window","st":1740562241.572,"sv":21235.40852985537,"t":"sr","w":2.0},{"cg":"trending info up","col":"","diag":[["adx","28.756"],["vix","3.068"],["atr","7.237"]],"dir":"up","id":"abc","res":true,"rsn":"Trending values","t":"cg","tn":"info","tr":-1.0},{"cg":"trending exit up","col":"","diag":[["atr","7.237"],["price","21273.050"],["sliding_window","21235.409"],["stop_loss_deviation_factor","10.000"],["deviation","37.641"],["stop_loss_deviation_threshold","-72.369"]],"dir":"up","id":"abc","res":false,"rsn":"Exit position up - Stop loss (related to ATR)","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"trending exit up","col":"","diag":[["atr","7.237"],["price","21273.050"],["sliding_window","21235.409"],["take_profit_deviation_factor","6.957"],["deviation","37.641"],["take_profit_deviation_threshold","50.347"]],"dir":"up","id":"abc","res":false,"rsn":"Exit position up - Take profit (related to ATR)","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"trending exit up","col":"","diag":[["m","false"],["d","-17599.15"]],"dir":"up","id":"abc","res":true,"rsn":"Trading not allowed - During market hours only","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"trending enter up","col":"","diag":[["adx_threshold","25.000"],["adx","28.756"]],"dir":"up","id":"abc","res":false,"rsn":"Long entry - Low ADX","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"trending enter up","col":"","diag":[["atr","7.237"],["entry_deviation_threshold","25.179"],["deviation","37.641"],["entry_deviation_factor","3.479"]],"dir":"up","id":"abc","res":true,"rsn":"Entry - Deviation from mean","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"trending enter up","col":"","diag":[],"dir":"up","id":"abc","res":false,"rsn":"Trade entry - In Market Time","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"trending exit up","dir":"up","res":true,"t":"cgr"}]}
{"log_id": "0xc0ccce9d56842ff", "timestamp": 1740508991.9110,  "component": "strand", "type": "real_time_strategy", "value": [{"c":"purple","n":"[0] SMA large","st":1740508991.911,"sv":21074.57206012377,"t":"sr","w":2.0},{"c":"blue","n":"[0] SMA small","st":1740508991.911,"sv":21108.860041407974,"t":"sr","w":2.0},{"c":"green","d":"[0] local high","l":{"close":21236.300000000003,"high":21236.300000000003,"low":21230.300000000003,"n":3378,"open":21230.35,"timestamp":1740506869.119,"volume":42},"t":"l","w":2.0},{"c":"red","d":"[0] local low","l":{"close":20942.199999999997,"high":20947.25,"low":20942.199999999997,"n":3040,"open":20947.25,"timestamp":1740501204.049,"volume":12},"t":"l","w":2.0},{"a":"3.6568e-1","b":"1.9926e4","c":"#F28500","d":"[0] trend lr","n":"3.4930e3","p":614,"q":"Moderate trend","r2":"7.6682e-1","s":2.0,"t":"lrf"},{"a":"-3.1028e-1","b":"2.2240e4","c":"red","d":"[0] down run","dm":"-2.1099e1","mn":"2.1122e4","mx":"2.1236e4","n":116,"s":2.0,"t":"lr","um":"6.1081e1"},{"a":"4.4818e-1","b":"1.9653e4","c":"green","d":"[0] up run","dm":"-9.7736e1","mn":"2.0942e4","mx":"2.1236e4","n":454,"s":2.0,"t":"lr","um":"5.1637e1"},{"cg":"[0] trending info up","col":"","diag":[["adx","17.833"],["vix","3.442"],["atr","13.899"],["trend_strength","2.467"],["is_trending","false"]],"dir":"up","id":"efg","res":false,"rsn":"Trending values","t":"cg","tn":"info","tr":-1.0},{"cg":"[0] trending exit up","col":"","diag":[["fast_sma","21108.860"],["slow_sma","21074.572"]],"dir":"up","id":"efg","res":false,"rsn":"Exit long position - Moving average crossover","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"[0] trending exit up","col":"","diag":[["lr_slope","0.366"]],"dir":"up","id":"efg","res":false,"rsn":"Exit long position - Trend reversal","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"[0] trending exit up","col":"","diag":[["atr","13.899"],["price","21199.800"],["slow_sma","21074.572"]],"dir":"up","id":"efg","res":false,"rsn":"Exit long position - Stop loss (2 ATR)","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"[0] trending exit up","col":"","diag":[["atr","13.899"],["price","21199.800"],["entry_price","none"]],"dir":"up","id":"efg","res":false,"rsn":"Exit long position - Take profit (3 ATR)","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"[0] trending exit up","col":"","diag":[["m","true"],["d","18057.51"]],"dir":"up","id":"efg","res":false,"rsn":"Trading not allowed - In market hours only","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"[0] trending enter up","col":"","diag":[["sma_small","21108.860"],["sma_large","21074.572"],["candle","21199.800"]],"dir":"up","id":"efg","res":true,"rsn":"Long entry - Trend alignment","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"[0] trending enter up","col":"","diag":[["trend_strength","2.467"],["lr_slope","0.366"],["lr_r2","0.767"],["min_slope","0.010"],["min_r2","0.677"]],"dir":"up","id":"efg","res":true,"rsn":"Long entry - Trend strength","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"[0] trending enter up","col":"","diag":[["atr","13.899"],["volatility_threshold","2.000"],["vol_calc","423.996"]],"dir":"up","id":"efg","res":true,"rsn":"Long entry - Volatility check","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"[0] trending enter up","col":"","diag":[],"dir":"up","id":"efg","res":true,"rsn":"Trade entry - In Market Time","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"[0] trending enter up","dir":"up","res":true,"t":"cgr"},{"instrument":"NAS100_USD","price":21202.600000000002,"profit":0.0,"t":"open"},{"instrument":"NAS100_USD","price":21199.800000000003,"profit":0.0,"t":"adjust-open"}]}
{"log_id": "0xf0afc79f6bf34e76", "timestamp": 1740597207.8710,  "component": "strand", "type": "real_time_strategy", "value": [{"c":"green","d":"[0] local high","l":{"close":21752.6,"high":21754.1,"low":21750.2,"n":790,"open":21751.3,"timestamp":1740389040.0,"volume":124},"t":"l","w":2.0},{"c":"red","d":"[0] local low","l":{"close":20952.8,"high":20967.2,"low":20949.2,"n":597,"open":20958.2,"timestamp":1740501120.0,"volume":1223},"t":"l","w":2.0},{"a":"-2.2083e-2","b":"2.1233e4","c":"#7900f2","d":"[0] trend lr","n":"1.0000e0","p":120,"q":"No trend","r2":"2.0173e-3","s":2.0,"t":"lrf"},{"a":"1.0182e-2","b":"2.1319e4","c":"red","d":"[0] down run","dm":"-6.2896e2","mn":"2.0953e4","mx":"2.1753e4","n":3350,"s":2.0,"t":"lr","um":"1.0363e2"},{"a":"9.1704e-2","b":"2.1085e4","c":"green","d":"[0] up run","dm":"-2.6044e2","mn":"2.0953e4","mx":"2.1360e4","n":1543,"s":2.0,"t":"lr","um":"6.7685e1"},{"c":"blue","n":"[0] sliding window","st":1740597207.871,"sv":21204.29268693987,"t":"sr","w":2.0},{"cg":"trending info up","col":"","diag":[["adx","36.808"],["vix","7.947"],["atr","17.771"]],"dir":"up","id":"abc","res":true,"rsn":"Trending values","t":"cg","tn":"info","tr":-1.0},{"cg":"trending exit up","col":"","diag":[["atr","17.771"],["price","21052.650"],["sliding_window","21204.293"],["stop_loss_deviation_factor","10.000"],["deviation","-151.643"],["stop_loss_deviation_threshold","-177.706"]],"dir":"up","id":"abc","res":false,"rsn":"Exit position up - Stop loss (related to ATR)","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"trending exit up","col":"","diag":[["atr","17.771"],["price","21052.650"],["sliding_window","21204.293"],["take_profit_deviation_factor","6.957"],["deviation","-151.643"],["take_profit_deviation_threshold","123.628"]],"dir":"up","id":"abc","res":false,"rsn":"Exit position up - Take profit (related to ATR)","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"trending exit up","col":"","diag":[["m","false"],["d","17367.15"]],"dir":"up","id":"abc","res":true,"rsn":"Trading not allowed - During market hours only","t":"cg","tn":"hard_pass","tr":-1.0},{"cg":"trending enter up","col":"","diag":[["adx_threshold","25.000"],["adx","36.808"]],"dir":"up","id":"abc","res":false,"rsn":"Long entry - Low ADX","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"trending enter up","col":"","diag":[["atr","17.771"],["entry_deviation_threshold","61.828"],["deviation","-151.643"],["entry_deviation_factor","3.479"]],"dir":"up","id":"abc","res":false,"rsn":"Entry - Deviation from mean","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"trending enter up","col":"","diag":[],"dir":"up","id":"abc","res":false,"rsn":"Trade entry - In Market Time","t":"cg","tn":"part_pass","tr":-1.0},{"cg":"trending exit up","dir":"up","res":true,"t":"cgr"},{"b":"mean_reversion_paper","equity":"0.0000","t":"e"}]}

```

Maria DB table for equity:
```
CREATE TABLE IF NOT EXISTS `dashboard_equity` (
  `id` varchar(255) NOT NULL,
  `timestamp` double NOT NULL,
  `mode` varchar(255) NOT NULL,
  `equity` double NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

Maria DB table for trades:
```
CREATE TABLE IF NOT EXISTS `dashboard_trades` (
  `timestamp` double NOT NULL,
  `id` varchar(255) NOT NULL,
  `trade_event_json` text NOT NULL,
  `mode` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_timestamp` (`timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```
---

## **2️⃣ systemd Service + Timer**
- **Service**: Runs the Python binary.
- **Timer**: Executes the service **every 10 minutes**.
- **Restart on failure** (with **3 retries** before alerting).
- **Auto-recovery**: Restart dependencies (network, MariaDB).

---

## **3️⃣ Bash Recovery Script**
- Checks **failure count** in logs.
- Restarts **network & MariaDB**.
- Skips alerts **during sleep hours (10PM - 8AM)**.
- Sends **Pushover alert** if still failing.

---

## **4️⃣ Sample Pushover Alert**
- Title: `"JSNL Processor Failure"`
- Message: `"Service failed 3 times and did not recover. Check logs."`

---

**Please generate the full codebase including:**
1. Python code for processing & merging Parquet files.
2. A **systemd service** and **timer** for scheduling.
3. A **Bash script** for auto-recovery.
4. A **Pushover integration** for failure alerts.

Ensure the **Python code is production-ready**, uses **DuckDB efficiently**, and supports **batch processing**. Use best practices for **error handling, logging, and retries**. 🚀

