<!--
 * @Author: your name
 * @Date: 2024-08-20 09:51:06
 * @LastEditTime: 2024-10-25 12:09:23
 * @LastEditors: LAPTOP-22MC5HRI
 * @Description: In User Settings Edit
 * @FilePath: \README.md
-->
### 執行永豐API
```
pip install shioaji[speed]
```

### 如出現錯誤circular import pysolace....
```
pip uninstall pysolace
pip install pysolace --no-cache-dir 
pip uninstall orjson
pip install orjson --no-cache-dir 
```

### Using backtrader to backtest the Taiwan Stock Market, the whole code concept
```
1. using the shioaji API to download the Taiwan Stock Market Data
2. using the multi-process to backtest multiple stock data
3. using the pandas Reader to read whole CSV file in backtrader
4. taggle with the data of the Stock Data, and generate the singal to long or shot the Stock
5. Analyze the whole trade result
6. Generate and Caculate the trade of today result, including P&L, winning rate, Position ..ETC
For more detail, please see the analyze_global_result() function
7. Every Stock trading log and result would be place in the result folder
8. You can set the parameter at the Buttom of the Code
EX:
is_dyanmic => If you want to use the dynamic profit and loss
long_short => True if you want to run long strategy, False other wise


Run the code:
python index_v0 (multiprocess of 4 core)
python index_v1 (multiprocess of 16 core)
