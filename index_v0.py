import backtrader as bt
from datetime import datetime, time, timedelta
import os, math, logging, shutil
from multiprocessing import Pool, Manager
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
import quantstats

# ------------------------ 自定義pandas data ------------------------

class PandasData(bt.feeds.PandasData):
    lines = ('open', 'close', 'volume', 'bid_price', 'bid_volume', 'ask_price', 'ask_volume', 'tick_type')
    params = (
        ('datetime', None),          # 使用 DataFrame 的索引作为 datetime
        ('open', 'open'),            # 使用 open 列
        ('high', -1),                # 不使用 high 列
        ('low', -1),                 # 不使用 low 列
        ('close', 'close'),          # 使用 close 列
        ('volume', 'volume'),        # 使用 volume 列
        ('bid_price', 'bid_price'),  # 使用 bid_price 列
        ('ask_price', 'ask_price'),  # 使用 ask_price 列
        ('bid_volume', 'bid_volume'),# 使用 bid_volume 列
        ('ask_volume', 'ask_volume'),# 使用 ask_volume 列
        ('tick_type', 'tick_type'),  # 使用 tick_type 列
        ('openinterest', -1),        # 不使用 openinterest 列
    )

# -------------------------- 信號與策略 -------------------------

from collections import deque, OrderedDict

class PriceVolumeSignal(bt.Signal):
    lines = ('signal',)
    params = (('period', 30), #監測時間秒數
              ('volume_threshold', 10), #成交量門檻
              ('logger', None) # log紀錄
    )
    plotinfo = dict(subplot=False)
    plotlines = dict(
        signal=dict(
            color='red',
            linewidth=2.0,
        )
    )

    def __init__(self):
        self.price_volume_dict = {} # 價量字典
        self.reset()
        self.previous_date = None  # 用來追蹤日期變更
        self.logger = self.params.logger
        self.logger.info(f'The Monitor Period is: {self.params.period} seconds, volume_threshold: {self.params.volume_threshold}')

    def reset(self):
        self.high_price = 0 # 最高價格
        self.low_price = 0 # 最低價格
        self.high_price_stable = deque(maxlen=self.params.period)  # 存储过去30秒的价格和成交量
        self.low_price_stable = deque(maxlen=self.params.period)   # 用于追踪30秒内最低价的成交量
        self.price_limits = {}  # 漲跌停位
        self.has_updated_today = False  # 追蹤當天是否已更新字典
        
    def next(self):
        # 取得當前的日期和時間
        dt = self.data.datetime.date(0).strftime('%Y-%m-%d')  # 提取日期部分 類型: str
        current_date = self.data.datetime.datetime(0).date() # 類型: datetime.date
        current_time = self.data.datetime.datetime(0).time()
        close = self.data.close[0]
        volume = self.data.volume[0]

        # 檢查是否是新的一天, 重設所有資料, 更新當天日期
        if self.previous_date != current_date:
            self.reset()
            self.previous_date = current_date  # 更新為新的一天的日期
  
        # 如果還沒有更新當天的字典
        if not self.has_updated_today:
            # 如果時間是 13:30:00 或當天的第一筆資料
            if current_time >= pd.Timestamp('09:00:00').time() and not self.has_updated_today:
                self._store_price_limits(self.data.close[-1])
                self.has_updated_today = True  # 標記當天已更新
                
            if current_time == pd.Timestamp('13:30:00').time():  # 根据你的数据时间设定
                self.has_updated_today = False

        # 使用日期更新價量字典
        if dt not in self.price_volume_dict:
            self.price_volume_dict[dt] = {}

        if close not in self.price_volume_dict[dt]:
            self.price_volume_dict[dt][close] = volume
        else:
            self.price_volume_dict[dt][close] += volume
        
        # 对价格进行排序，并创建一个新的有序字典
        sorted_dict = OrderedDict(sorted(self.price_volume_dict[dt].items(), key=lambda x: x[0], reverse=True))

        # 更新当天的價量字典为排序后的字典
        self.price_volume_dict[dt] = sorted_dict

        # 获取当前最高价和最低价
        current_high_price = next(iter(sorted_dict))
        current_low_price = next(reversed(sorted_dict))
        current_high_volume = sorted_dict[current_high_price]
        current_low_volume = sorted_dict[current_low_price]

        # 检查最高价是否刷新(做空)
        if current_high_price != self.high_price:
            self.high_price = current_high_price
            self.high_price_stable.clear()  # 最高价更新时，清空追踪队列
        else:
            self.high_price_stable.append(current_high_volume) # 添加当前的最高价成交量到队列中

        # 检查最低价是否刷新(做多)
        if current_low_price != self.low_price:
            self.low_price = current_low_price
            self.low_price_stable.clear()  # 最低价更新时，清空追踪队列
        else:
            self.low_price_stable.append(current_low_volume) # 添加当前的最低价成交量到队列中
        
        # 如果收盘价小于 limit_short_danger，只能检查 high_price_stable 生成做多信号
        if close <= self.price_limits['limit_short_danger'] or close >= self.price_limits['limit_long_danger']:
            return
    
        # 如果沒超過危險空的危險金額, 检查最高价的成交量是否小于指定成交量并持续指定的秒數
        if len(self.high_price_stable) == self.params.period and all(v < self.params.volume_threshold for v in self.high_price_stable):
            self.lines.signal[0] = -1  # 生成做空信号
            return
        
        # 如果沒超過危險多的危險金額, 检查最低价的成交量是否小于指定成交量并持续指定的秒數
        elif len(self.low_price_stable) == self.params.period and all(v < self.params.volume_threshold for v in self.low_price_stable):
            self.lines.signal[0] = 1   # 生成做多信号
            return
        else:
            self.lines.signal[0] = 0   # 没有信号生成
            return

    def get_price_volume_dict(self):
        return self.price_volume_dict
    
    def _store_price_limits(self, close_price):
        self.price_limits = {
            'limit_up': round((math.floor(close_price * (1 + 0.10) / 0.05) * 0.05), 2) if 10 <= close_price < 50 else 
                        round((math.floor(close_price * (1 + 0.10) / 0.1) * 0.1), 1) if 50 <= close_price < 100 else 
                        round((math.floor(close_price * (1 + 0.10) / 0.5) * 0.5), 1) if close_price >= 100 else 
                        round(close_price * (1 + 0.10), 2),

            'limit_down': round((math.floor(close_price * (1 - 0.10) / 0.05) * 0.05), 2) if 10 <= close_price < 50 else 
                          round((math.floor(close_price * (1 - 0.10) / 0.1) * 0.1), 1) if 50 <= close_price < 100 else 
                          round((math.floor(close_price * (1 - 0.10) / 0.5) * 0.5), 1) if close_price >= 100 else 
                          round(close_price * (1 - 0.10), 2),
            'limit_long_danger': math.floor(close_price * (1 + 0.07)),
            'limit_short_danger': math.ceil(close_price * (1 - 0.07))
        }
        self.logger.info(f"Price Limits Calculated {self.previous_date} {self.price_limits}")

class MyStrategy(bt.Strategy):
    params = (('ticks', 5),  # 價差ticks的級距(決定是否入場的tick數)
              ('profit_ratio', 0.03), # 止盈比例
              ('loss_ratio', 0.01), # 止損比例
              ('dynamic', False), # 是否動態訂單
              ('period', 30), # 訊號監測時間秒數
              ('volume_threshold', 10), # 成交量門檻
              ('last_trade_hour', 13), # 最後交易時間
              ('last_trade_minute', 0), # 最後交易時間
              ('long_short', True), # 空策略或多策略(True: 多策略, False: 空策略)
              ('logger', None) # log紀錄
    )
    
    def __init__(self):
        # 记录手續費, 金額
        self.total_commission = 0.0
        self.trade_count = 0
        self.initial_cash = self.broker.get_cash()
        self.logger = self.params.logger
        
        # 策略使用
        self.signal = PriceVolumeSignal(period=self.params.period, volume_threshold=self.params.volume_threshold, logger=self.params.logger) # 訊號
        self.reset_order()
        
    def reset_order(self):
        self.long_order, self.short_order = None, None # 多空訂單
        self.long_stop_order, self.short_stop_order = None, None # 多空止損訂單
        self.long_profit_order, self.short_profit_order = None, None # 多空止盈訂單
        self.long_profit_price, self.long_loss_price = None, None # 多單止盈止損價格
        self.short_profit_price, self.short_loss_price = None, None # 空單止盈止損價格
        
    def next(self):
        dt = self.data.datetime.date(0).strftime('%Y-%m-%d')
        if dt not in self.signal.price_volume_dict:
            self.logger.info(f'In Strategy: The date is not in price_volume_dict {dt}')
            self.reset_order()
            return
        
        # 超過下午1點, 全數平倉不交易, 所有訂單回歸預設
        current_time = self.data.datetime.time(0)
        if current_time > time(self.params.last_trade_hour, self.params.last_trade_minute):
            if self.position:
                self.close()
                self.reset_order()
                self.logger.info(f"All positions closed at {self.data.datetime.datetime(0)} due to end of trading time.")
            return

        # 價量字典中兩個價格的差額
        close_price = self.data.close[0]
        bid_price = self.data.bid_price[0]
        ask_price = self.data.ask_price[0]

        # 如果有倉位, 且為動態止盈止損
        if self.position and self.params.dynamic:
            if self.position.size > 0:  # 多單
                self.logger.info(f"當前有多單倉位, 且為動態止盈止損")
                if not self.long_stop_order: # 多單有倉位無止損單, 先掛一張止損單
                    self.long_stop_order = self.sell(exectype=bt.Order.Stop, price=self.long_loss_price)

                if self.long_profit_price == self.signal.price_limits['limit_up']: # 多單當前金額已經到漲停位
                   # 取消现有止损单
                    if self.long_stop_order:
                        self.cancel(self.long_stop_order)

                    # 上調多單止损比例 新的止损价格（使用当前价格和新的止损比例）
                    stop_loss_price, profit_stop_price = self._dynamic_order_caculate(self.data.close[0], self.long_profit_price, type='long')
                    self.long_loss_price = stop_loss_price
                    # 重挂新的止损单
                    self.long_stop_order = self.sell(exectype=bt.Order.Stop, price=self.long_loss_price)
                    self.logger.info(f"{self.datas[0].datetime.datetime(0)}, Long Pofit Stop Already Limit, Modify Loss Price: {self.long_loss_price}")
                    return

                new_profit_stop = max(self.long_profit_price, self.data.close[0])
                if new_profit_stop != self.long_profit_price: # 更新止盈價格, 重設訂單
                    self.logger.info(f'In Long Order: current close price {new_profit_stop} bigger than previous profit stop {self.long_profit_price}')
                    self.logger.info(f'Previous profit stop: {self.long_profit_price}, Previous loss stop: {self.long_loss_price}')

                    if self.long_profit_order: # 如有止盈多單則取消
                        self.cancel(self.long_profit_order)
                    self.cancel(self.long_stop_order)  # 取消先前多單止損
                    stop_loss_price, profit_stop_price = self._dynamic_order_caculate(self.data.close[0], new_profit_stop, type='long')
                    self.long_loss_price = stop_loss_price
                    self.long_profit_price = profit_stop_price
                    self.long_stop_order = self.sell(exectype=bt.Order.Stop, price=self.long_loss_price) # 更新止損單

                    if self.long_profit_price == self.signal.price_limits['limit_up']: # 多單當前金額已經到漲停位
                        self.long_profit_order = self.sell(exectype=bt.Order.Limit, price=self.long_profit_price) # 多單止盈單

                    self.logger.info(f"{self.datas[0].datetime.datetime(0)}, Updated Long Profit Stop: {self.long_profit_price}, Updated Long Loss Price: {self.long_loss_price}")

            elif self.position.size < 0:  # 空單
                self.logger.info(f"當前有空單倉位, 且為動態止盈止損")
                if not self.short_stop_order: # 空單有倉位無止損單, 先掛一張止損單
                    self.short_stop_order = self.buy(exectype=bt.Order.Stop, price=self.short_loss_price)

                if self.short_profit_price == self.signal.price_limits['limit_down']: # 空單當前金額已經到跌停位
                    # 取消现有止损单
                    if self.short_profit_order:
                        self.cancel(self.short_profit_order)

                    # 上調空單止损比例 新的止损价格（使用当前价格和新的止损比例）
                    stop_loss_price, profit_stop_price = self._dynamic_order_caculate(self.data.close[0], self.short_profit_price, type='short')
                    self.short_loss_price = stop_loss_price
                    # 重挂新的止损单
                    self.short_profit_order = self.buy(exectype=bt.Order.Stop, price=self.short_loss_price)
                    self.logger.info(f"{self.datas[0].datetime.datetime(0)}, Short Pofit Stop Already Limit, Modify Loss Price: {self.short_loss_price}")
                    return

                new_profit_stop = min(self.short_profit_price, self.data.close[0])
                if new_profit_stop != self.short_profit_price:
                    self.logger.info(f'In Short Order: current close price {new_profit_stop} smaller than previous profit stop {self.short_profit_price}')
                    self.logger.info(f'Previous profit stop: {self.short_profit_price}, Previous loss stop: {self.short_loss_price}')

                    if self.short_profit_order: # 如有止盈空單則取消
                        self.cancel(self.short_profit_order)
                    self.cancel(self.short_stop_order)  # 取消先前空單止損
                    stop_loss_price, profit_stop_price = self._dynamic_order_caculate(self.data.close[0], new_profit_stop, type='short')
                    self.short_loss_price = stop_loss_price
                    self.short_profit_price = profit_stop_price
                    self.short_stop_order = self.buy(exectype=bt.Order.Stop, price=self.short_loss_price) # 更新空單止損單

                    if self.short_profit_price == self.signal.price_limits['limit_down']: # 空單當前金額已經到跌停位
                        self.short_profit_order = self.buy(exectype=bt.Order.Limit, price=self.short_profit_price) # 空單止盈單

                    self.logger.info(f"{self.datas[0].datetime.datetime(0)}, Updated Short Profit Stop: {self.short_profit_price}, Updated Short Loss Price: {self.short_loss_price}")
            return
        
        # 如果有倉位, 且為靜態止盈止損
        elif self.position and not self.params.dynamic:
            if self.position.size > 0:  # 多單
                if not self.long_stop_order: # 多單有倉位無止損單, 掛止損單
                    self.logger.info(f"當前「靜態止損」有多單倉位無止損單, 掛止損單")
                    self.long_stop_order = self.sell(exectype=bt.Order.Stop, price=self.long_loss_price)
                elif not self.long_profit_order: # 多單有倉位無止盈單, 掛止盈單
                    self.logger.info(f"當前「靜態止盈」有多單倉位無止盈單, 掛止盈單")
                    self.long_profit_order = self.sell(exectype=bt.Order.Limit, price=self.long_profit_price)
            elif self.position.size < 0:  # 空單
                if not self.short_stop_order: # 空單有倉位無止損單, 掛止損單
                    self.logger.info(f"當前「靜態止損」有空單倉位無止損單, 掛止損單")
                    self.short_stop_order = self.buy(exectype=bt.Order.Stop, price=self.short_loss_price)
                elif not self.short_profit_order: # 空單有倉位無止盈單, 掛止盈單
                    self.logger.info(f"當前「靜態止損」有空單倉位無止盈單, 掛止盈單")
                    self.short_profit_order = self.buy(exectype=bt.Order.Limit, price=self.short_profit_price)
            return

        # 目前無訂單與倉位, 開始計算訊號產生的最高價格和最低價格
        highest_price = max(self.signal.price_volume_dict[dt].keys())
        lowest_price = min(self.signal.price_volume_dict[dt].keys())

        # 訊號多空判斷
        if self.signal.lines.signal[0] == -1 and self.params.long_short is False:
            if self.short_order: # 如果已經下空單，則不再下單
                return
        
            self.logger.info(f'------------------- SHORT SIGNAL -1 -------------------')
            if highest_price > bid_price and highest_price > ask_price:
                # 调用动态 tick 计算方法
                target_price = self.calculate_dynamic_ticks(highest_price, self.params.ticks, type='short')

                if target_price < bid_price and target_price < ask_price:
                    self.short_order = self.sell(exectype=bt.Order.Limit, price=bid_price)
                    stop_loss_price, profit_stop_price = self._dynamic_order_caculate(highest_price, bid_price, type='short')
                    self.short_loss_price = highest_price
                    self.short_profit_price = profit_stop_price
                    self.logger.info(f"SHORT STRATEGY EXECUTED AT {self.data.datetime.datetime(0)}, Highest Price: {highest_price}, Close Price: {close_price}, Bid (Entry) Price: {bid_price}, Ask Price: {ask_price}, Stop Loss: {stop_loss_price}, Profit Stop: {profit_stop_price}")
                else:
                    self.logger.info(f"SHORT STRATEGY NO TRADE AT {self.data.datetime.datetime(0)}, Highest Price: {highest_price}, Close Price: {close_price}, Bid Price: {bid_price}, Ask Price: {ask_price}, Entry Price {bid_price} and Ask Price {ask_price} Need Above: {target_price}")
        
        elif self.signal.lines.signal[0] == 1 and self.params.long_short is True:
            if self.long_order: # 如果已經下多單，則不再下單
                return
            
            self.logger.info(f'------------------- LONG SINGAL 1 -------------------')
            if lowest_price < bid_price and lowest_price < ask_price:
                # 调用动态 tick 计算方法
                target_price = self.calculate_dynamic_ticks(lowest_price, self.params.ticks, type='long')
                
                if target_price > bid_price and target_price > ask_price:
                    self.long_order = self.buy(exectype=bt.Order.Limit, price=bid_price)
                    stop_loss_price, profit_stop_price = self._dynamic_order_caculate(lowest_price, bid_price, type='long')
                    self.long_loss_price = lowest_price
                    self.long_profit_price = profit_stop_price
                    self.logger.info(f"LONG STRATEGY EXECUTED AT {self.data.datetime.datetime(0)}, Lowest Price: {lowest_price}, Close Price: {close_price}, Bid (Entry) Price: {bid_price}, Ask Price: {ask_price}, Stop Loss: {stop_loss_price}, Profit Stop: {profit_stop_price}")
                else:
                    self.logger.info(f"LONG STRATEGY NO TRADE AT {self.data.datetime.datetime(0)}, Lowest Price: {lowest_price}, Close Price: {close_price}, Bid (Entry) Price: {bid_price}, Ask Price {ask_price}, Entry Price {bid_price} and Ask Price {ask_price} Need Below: {target_price}")

    def calculate_dynamic_ticks(self, price, ticks, type='long'):
        result_price = price
        remaining_ticks = ticks
        
        while remaining_ticks > 0: # 获取当前价格对应的 tick_size
            if result_price > 100:
                tick_size = 0.5
            elif 50 <= result_price <= 100:
                tick_size = 0.1
            else:
                tick_size = 0.05

            # 计算下一个跳动的价格
            if type == 'short':
                result_price -= tick_size
            elif type =='long':
                result_price += tick_size
            remaining_ticks -= 1

        return round(result_price, 2)
   
    def _dynamic_order_caculate(self, price1, price2, type='long'):
        """
        动态计算止损与止盈价格，考虑不同价格区间的 tick_size。
        :param price1: 止損金額
        :param price2: 止盈金額
        :param type: 'long' or 'short' 方向
        :return: 止损价格，止盈价格
        """
        if type == 'long':
            # 动态计算止损价格，向上调整
            stop_loss_price = self.calculate_price_with_dynamic_ticks(price1 * (1 - self.params.loss_ratio), 'up')
            # 动态计算止盈价格，向下调整
            profit_stop_price = self.calculate_price_with_dynamic_ticks(price2 * (1 + self.params.profit_ratio), 'down')
            return stop_loss_price, min(self.signal.price_limits['limit_up'], profit_stop_price)
        else:
            # 动态计算止损价格，向下调整
            stop_loss_price = self.calculate_price_with_dynamic_ticks(price1 * (1 + self.params.loss_ratio), 'down')
            # 动态计算止盈价格，向上调整
            profit_stop_price = self.calculate_price_with_dynamic_ticks(price2 * (1 - self.params.profit_ratio), 'up')
            return stop_loss_price, max(self.signal.price_limits['limit_down'], profit_stop_price)

    def calculate_price_with_dynamic_ticks(self, target_price, direction='up'):
        """
        根据价格区间动态调整价格，考虑 tick_size 的变化。
        :param target_price: 目标价格
        :param direction: 调整方向，'up' 表示向上调整，'down' 表示向下调整
        :return: 动态调整后的价格
        """
        current_price = target_price

        while True:
            # 根据当前价格判断 tick_size
            if current_price > 100:
                tick_size = 0.5
            elif 50 <= current_price <= 100:
                tick_size = 0.1
            else:
                tick_size = 0.05

            # 根据方向进行价格调整
            if direction == 'up':
                new_price = math.ceil(current_price / tick_size) * tick_size
            else:
                new_price = math.floor(current_price / tick_size) * tick_size

            # 如果价格不再变化，退出循环
            if new_price == current_price:
                break

            current_price = new_price

        return current_price
    
    def log(self, txt, dt=None):
        # 獲取當前的日期時間
        dt = dt or self.datas[0].datetime.datetime(0)
        self.logger.info(f'{dt.isoformat()} {txt}')

    def notify_order(self, order):
        if order.status in [order.Submitted]:
            exec_type_str = 'Market' if order.exectype == bt.Order.Market else \
                'Limit' if order.exectype == bt.Order.Limit else \
                'Stop' if order.exectype == bt.Order.Stop else \
                'StopLimit' if order.exectype == bt.Order.StopLimit else \
                'Unknown'
            created_dt = bt.num2date(order.created.dt)
            self.logger.info(f'訂單送出(尚未接受): Ref:{order.ref}, Date:{created_dt}, Price: {order.price}, ExecType: {exec_type_str}, Size: {order.size}')
        
        if order.status in [order.Accepted, order.Partial]:            
            exec_type_str = 'Market' if order.exectype == bt.Order.Market else \
                        'Limit' if order.exectype == bt.Order.Limit else \
                        'Stop' if order.exectype == bt.Order.Stop else \
                        'StopLimit' if order.exectype == bt.Order.StopLimit else \
                        'Unknown'
            created_dt = bt.num2date(order.created.dt)

            self.logger.info(f'訂單接受(尚未成交): Ref:{order.ref}, Date:{created_dt}, Price: {order.price}, ExecType: {exec_type_str}, Size: {order.size}')
            return
        
        elif order.status in [order.Completed]:
            exec_type_str = 'Market' if order.exectype == bt.Order.Market else \
                'Limit' if order.exectype == bt.Order.Limit else \
                'Stop' if order.exectype == bt.Order.Stop else \
                'StopLimit' if order.exectype == bt.Order.StopLimit else \
                'Unknown'
            
            if order.isbuy():
                if exec_type_str == 'Market':
                    self.logger.info(f'平倉買入: Ref:{order.ref}, Date:{bt.num2date(order.executed.dt)}, Price: {order.price}, Size: {order.size}, ExecType: {exec_type_str}')
                    self.logger.info(f'買入: 价格 {round(order.executed.price, 2)}, 成本: {round(order.executed.value, 2)}, 手续费: {round(order.executed.comm, 2)}')
                    self.long_order = None
                    
                elif exec_type_str == 'Limit':
                    if self.params.long_short is False:
                        if self.params.dynamic is True:
                            self.logger.info(f'空單動態止盈: Ref:{order.ref}, Date:{bt.num2date(order.executed.dt)}, Price: {order.price}, Size: {order.size}, ExecType: {exec_type_str}')
                        else:
                            self.logger.info(f'空單靜態止盈: Ref:{order.ref}, Date:{bt.num2date(order.executed.dt)}, Price: {order.price}, Size: {order.size}, ExecType: {exec_type_str}')
                            self.short_profit_order = None
                        self.logger.info(f"已經空單止盈, 取消空單止損單")
                        self.cancel(self.short_stop_order)
                        self.short_stop_order = None
                    else:
                        self.logger.info(f'多單買入: Ref:{order.ref}, Date:{bt.num2date(order.executed.dt)}, Price: {order.price}, Size: {order.size}, ExecType: {exec_type_str}')
                    self.logger.info(f'買入: 价格 {round(order.executed.price, 2)}, 成本: {round(order.executed.value, 2)}, 手续费: {round(order.executed.comm, 2)}')
                    self.long_order = None
                    
                elif exec_type_str == 'Stop' or exec_type_str == 'StopLimit': # 碰到止損出場, 取消多單止盈, 重設多單止損
                    self.logger.info(f'空單止損: Ref:{order.ref}, Date:{bt.num2date(order.executed.dt)}, Price: {order.price}, Size: {order.size}, ExecType: {exec_type_str}')
                    self.logger.info(f'買入: 价格 {round(order.executed.price, 2)}, 成本: {round(order.executed.value, 2)}, 手续费: {round(order.executed.comm, 2)}')
                    self.cancel(self.short_profit_order)
                    self.short_profit_order, self.short_stop_order = None, None

            elif order.issell():
                if exec_type_str == 'Market':
                    self.logger.info(f'平倉賣出: Ref: {order.ref}, Date: {bt.num2date(order.executed.dt)}, Price: {order.price}, Size: {order.size}, ExecType: {exec_type_str}')
                    self.logger.info(f'賣出: 价格 {round(order.executed.price, 2)}, 成本: {round(order.executed.value, 2)}, 手续费: {round(order.executed.comm, 2)}')
                    self.short_order = None
                    
                elif exec_type_str == 'Limit':
                    if self.params.long_short is True:
                        if self.params.dynamic is True:
                            self.logger.info(f'多單動態止盈: Ref: {order.ref}, Date: {bt.num2date(order.executed.dt)}, Price: {order.price}, Size: {order.size}, ExecType: {exec_type_str}')
                        else:
                            self.logger.info(f'多單靜態止盈: Ref: {order.ref}, Date: {bt.num2date(order.executed.dt)}, Price: {order.price}, Size: {order.size}, ExecType: {exec_type_str}')
                            self.long_profit_order = None
                        self.logger.info(f"已經多單止盈, 取消多單止損單")
                        self.cancel(self.long_stop_order)
                        self.long_stop_order = None
                    else:
                        self.logger.info(f'空單賣出: Ref: {order.ref}, Date: {bt.num2date(order.executed.dt)}, Price: {order.price}, Size: {order.size}, ExecType: {exec_type_str}')

                    self.logger.info(f'賣出: 价格 {round(order.executed.price, 2)}, 成本: {round(order.executed.value, 2)}, 手续费: {round(order.executed.comm, 2)}')
                    self.short_order = None

                elif exec_type_str == 'Stop' or exec_type_str == 'StopLimit': # 碰到止損出場, 取消空單止盈, 重設空單止損
                    self.logger.info(f'多單止損: Ref:{order.ref}, Date:{bt.num2date(order.executed.dt)}, Price: {order.price}, Size: {order.size}, ExecType: {exec_type_str}')
                    self.logger.info(f'賣出: 价格 {round(order.executed.price, 2)}, 成本: {round(order.executed.value, 2)}, 手续费: {round(order.executed.comm, 2)}')
                    self.cancel(self.long_profit_order)
                    self.long_profit_order, self.long_stop_order = None, None

            # 记录当前日期、持仓金额、剩余现金和初始金额
            dt = self.data.datetime.date(0).isoformat()
            position_value = self.broker.get_value() - self.broker.get_cash()
            cash_remaining = self.broker.get_cash()
            self.logger.info(f'日期: {dt}, 当前持仓金额: {position_value:.2f}, 剩余现金: {cash_remaining:.2f}, 初始金额: {self.initial_cash:.2f}')
            self.logger.info('------------------- 本次訂單成功結束 -------------------')
            self.trade_count += 1
            self.total_commission += round(order.executed.comm, 2)
            return

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.logger.info('------------------- 訂單重設/取消 -------------------')
            self.logger.info(f'Order Canceled/Margin/Rejected, Ref: {order.ref}, Status: {order.getstatusname()}')
            self.long_order, self.short_order = None, None
            return

        # 無訂單狀態
        return
        
    def notify_trade(self, trade):
        if trade.isclosed:  # 当交易完成时记录盈亏
            self.logger.info('------------------- 本次交易盈虧統計 -------------------')
            
            entry_time = bt.num2date(trade.dtopen)
            exit_time = bt.num2date(trade.dtclose)
            pnlcomm = round(trade.pnlcomm, 2) # 带有佣金的盈亏
            trade_ref = trade.ref  # 交易的ref编号

            self.logger.info(f'完整買賣交易結束. 買賣交易代號: {trade_ref}, '
                     f'入場時間: {entry_time}, '
                     f'出場時間: {exit_time}, '
                     f'損益(含手續費): {pnlcomm}'
                    )

    def stop(self):
        self.logger.info('------------------ 全部交易結束資料統計 ------------------')
        self.logger.info(f'Total Data To BackTest: {len(self.data)}')
        # price_volume_dict = self.signal.get_price_volume_dict()
        # print('the final price_volume_dict:', price_volume_dict)

# -------------------------- 自定義分析器 -------------------------

class CustomAnalyzer(bt.Analyzer):
    def __init__(self):
        self.total_profit = 0
        self.total_loss = 0
        self.max_profit = 0
        self.max_loss = 0
        self.holding_times = []
        self.daily_results = []

    def notify_trade(self, trade):
        if trade.isclosed:
            profit = round(trade.pnlcomm, 2)
 
            # 确保 trade_duration 是 datetime.timedelta 对象
            trade_duration = timedelta(seconds=(trade.barclose - trade.baropen))
            self.holding_times.append(trade_duration)

            if profit > 0:
                self.total_profit += profit
                self.max_profit = max(self.max_profit, profit)
            else:
                self.total_loss += abs(profit)
                self.max_loss = max(self.max_loss, abs(profit))

    def _finalize_day(self):
        profit_loss_ratio = self.total_profit / self.total_loss if self.total_loss > 0 else float('inf')

        # 修正的 sum 方法，使用 datetime.timedelta(0) 作为起始值
        avg_duration = sum(self.holding_times, timedelta(0)) / len(self.holding_times) if self.holding_times else timedelta(0)
        max_duration = max(self.holding_times, default=timedelta(0))
        min_duration = min(self.holding_times, default=timedelta(0))

        daily_result = {
            'date': self.data.datetime.date(0).strftime('%Y-%m-%d'),
            'profit_loss_ratio': profit_loss_ratio,
            'max_profit': self.max_profit,
            'max_loss': self.max_loss,
            'avg_duration': avg_duration,
            'max_duration': max_duration,
            'min_duration': min_duration
        }
        self.daily_results.append(daily_result)

        # 重置统计数据
        self.total_profit = 0
        self.total_loss = 0
        self.max_profit = 0
        self.max_loss = 0
        self.holding_times = []

    def stop(self):
        self._finalize_day()

    def get_analysis(self):
        return {'daily': self.daily_results}

# ---------------------------- 主程式執行區域 -----------------------------

# 創建logger的方法
def setup_logger(stock_code):
    logger = logging.getLogger(str(stock_code))
    logger.setLevel(logging.INFO)

    # 創建handler，將log寫入到以股票代碼命名的檔案中，並設置編碼為'utf-8'
    log_file = f'{stock_code}_trading_log.log'
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    # 設定log格式
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # 將handler加到logger
    logger.addHandler(file_handler)
    
    return logger

#計算全局結果
def analyze_global_results(result_list, **params):
    # 使用字典按 result_dir 分组结果
    result_dir = {}
    for result in result_list:
        dir_path = result['result_dir']
        if dir_path not in result_dir:
            result_dir[dir_path] = []
        result_dir[dir_path].append(result)
    
    # 遍历每个 result_dir 的结果进行分析
    for dir_path, results in result_dir.items():
        only_one_result_dir = all(len(d) == 1 and 'result_dir' in d for d in result_list)
        
        if only_one_result_dir:
            print('无交易结果，无法分析')
            continue
        
        total_stocks = len(results)
        
        win_count = sum(1 for result in results if result['pnl'] > 0)
        total_pnl = sum(result['pnl'] for result in results)
        
        win_rate = win_count / total_stocks * 100 if total_stocks > 0 else 0
        avg_pnl = total_pnl / total_stocks if total_stocks > 0 else 0

        # 全局持仓时间和损益统计
        total_avg_duration = sum(result['avg_duration'] for result in results) / total_stocks
        total_max_duration = max(result['max_duration'] for result in results)
        total_min_duration = min(result['min_duration'] for result in results)
        
        total_max_profit = max(result['max_profit'] for result in results)
        total_max_loss = max(result['max_loss'] for result in results)

        # 写入到文件
        with open(os.path.join(dir_path, f"{'Long' if params.get('long_short', True) else 'Short'}_{'dynamic' if params.get('dynamic', False) else 'static'}.txt"), 'w', encoding='utf-8') as f:
            # 写入股票池结果的标题
            f.write('------------------- 股票池结果 -------------------\n')  

            for result in results:
                f.write(f"股票代号: {result['stock_code']}, 盈亏: {result['pnl']}\n")
            
            f.write('------------------- 全局统计 -------------------\n')  
            f.write(f"总回测股票数量: {total_stocks}\n")  
            f.write(f"赚钱股票数量: {win_count}\n")  
            f.write(f"胜率: {win_rate:.2f}%\n")  
            f.write(f"平均盈亏: {avg_pnl:.2f}\n")  
            f.write(f"总平均持仓时间: {total_avg_duration}\n")  
            f.write(f"总最大持仓时间: {total_max_duration}\n")  
            f.write(f"总最小持仓时间: {total_min_duration}\n")  
            f.write(f"总最大盈利: {total_max_profit}\n")  
            f.write(f"总最大亏损: {total_max_loss}\n")
            f.write(f"策略类型: {'做多' if params.get('long_short', True) else '做空'}\n")
            f.write(f"止盈止损类型: {'动态' if params.get('dynamic', False) else '静态'}\n")
            f.write(f"止盈: {(params.get('profit_ratio', None)) * 100}%, 止损: {(params.get('loss_ratio', None)) * 100}%\n")
            f.write(f"进场Ticks价差数: {params.get('ticks', None)}\n")
            f.write(f"成交量门槛: {params.get('volume_threshold', None)}\n")
            f.write(f"成交量观察秒数: {params.get('period', None)}\n")
            f.write(f"平仓时间: {params.get('last_trade_hour', None)}:{params.get('last_trade_minute', None)} \n")
        
def start_backtest(df, stock_code, stock_logger, **params):
    stock_logger.info(f'The current stock code is: {stock_code}')
    # 设置数据源并运行策略
    data = PandasData(dataname=df, timeframe=bt.TimeFrame.Seconds, compression=1)

    cerebro = bt.Cerebro()
    cerebro.adddata(data, name=str(stock_code))
    cerebro.addstrategy(MyStrategy, logger=stock_logger, # !!!不可調參數!!!
                        ticks=params.get('ticks', 5), # 價差ticks的級距(決定是否入場的tick數)
                        profit_ratio=params.get('profit_ratio', 0.03), # 止盈比例
                        loss_ratio=params.get('loss_ratio', 0.01), # 止損比例
                        dynamic=params.get('dynamic', False), # 是否動態訂單
                        period=params.get('period', 30), # 訊號監測時間秒數
                        volume_threshold=params.get('volume_threshold', 10), # 成交量門檻
                        last_trade_minute=params.get('last_trade_minute', 0), # 最後交易「分鐘」(13:xx)
                        last_trade_hour=params.get('last_trade_hour', 13), # 最後交易「小時」(xx:00)
                        long_short=params.get('long_short', True), # 空策略或多策略(True: 多策略, False: 空策略)
                    )

    # 设置初始资金和手续费
    initial_cash = params.get('initial_cash', 1000000)
    cerebro.broker.set_cash(initial_cash)
    cerebro.broker.setcommission(commission=0.02)
    # cerebro.broker.set_slippage_perc(perc=0.001, slip_limit=True)

    # 添加sizer
    cerebro.addsizer(bt.sizers.SizerFix, stake=1000)

    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio', timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns', timeframe=bt.TimeFrame.Days)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    cerebro.addanalyzer(CustomAnalyzer, _name='custom_analyzer')

    # 运行回测
    results = cerebro.run()

    analyzers = {
        'sharpe_ratio': 'sharpe',
        'drawdown': 'drawdown',
        'trade_analyzer': 'trade_analyzer',
        'returns': 'returns',
        'pyfolio': 'pyfolio',
        'custom_analyzer': 'custom_analyzer'
    }

    results_dict = {}

    for name, analyzer in analyzers.items():
        try:
            if analyzer == 'pyfolio':
                results_dict[name] = results[0].analyzers.getbyname(analyzer).get_analysis()
            else:
                results_dict[name] = getattr(results[0].analyzers, analyzer).get_analysis()
        except AttributeError:
            results_dict[name] = f"No {name.replace('_', ' ').capitalize()} Data"


    def print_stat(name, value, format_str="{:.2f}", default="N/A"):
        if value is None or value == 'No Data':
            stock_logger.info(f"{name}: {default}")
        else:
            stock_logger.info(f"{name}: {format_str.format(value)}")

    def format_duration(td):
        """将 timedelta 转换为秒数"""
        return int(td.total_seconds())

    # 有交易才顯示
    if results[0].trade_count > 0:
        data2append = {'stock_code': stock_code}
        stock_logger.info('------------------- 回測結束績效檢驗 -------------------')
        final_value = cerebro.broker.getvalue()
        profit = final_value - initial_cash
        
        stock_logger.info(f"交易股票代號: {stock_code}")
        stock_logger.info(f"初始金額: {initial_cash:.2f}")
        stock_logger.info(f"最終餘額: {final_value:.2f}")
        stock_logger.info(f"損益: {profit:.2f}")
        stock_logger.info(f"总手续费: {results[0].total_commission:.2f}")
        stock_logger.info(f"总交易次数: {results[0].trade_count}")
        
        data2append['commission'] = round(results[0].total_commission, 2)
        data2append['pnl'] = profit
        
        stock_logger.info('------------------- 分析器績效分析 -------------------')
        # 打印所有结果, 遍历字典并根据不同的分析器名进行处理
        for analyzer_name, result in results_dict.items():
            if result:  # 确保结果存在，不为 None 或空值
                # 针对不同的分析器名称，提取特定的值并打印
                if analyzer_name == 'sharpe_ratio': # 打印夏普率
                    print_stat("夏普率", result.get('sharperatio') if isinstance(result, dict) else None, "{:.2f}")

                elif analyzer_name == 'drawdown': # 打印最大回撤
                    print_stat("最大回撤", result.max.drawdown if result != 'No Drawdown Data' else None, "{:.2f}%")

                elif analyzer_name == 'trade_analyzer': # 打印总盈利次数和总亏损次数
                    print_stat("总盈利次数", result.won.total if result != 'No Trade Analyzer Data' else None, "{:d}")
                    print_stat("总亏损次数", result.lost.total if result != 'No Trade Analyzer Data' else None, "{:d}")

                elif analyzer_name == 'returns': # 打印年化收益率
                    print_stat("年化收益率", result.get('rnorm100') if isinstance(result, dict) else None, "{:.2f}%")

                elif analyzer_name == 'pyfolio': # 打印 PyFolio 相关数据 (如有需要)
                    if result != 'No PyFolio Data':
                        returns = result.get('returns', None)
                        if returns is not None: # 将 OrderedDict 转换为 pandas Series
                            if isinstance(returns, dict):
                                returns = pd.Series(returns)

                        if isinstance(returns, pd.Series): # 确保返回的是 pandas Series 生成 HTML 报告
                            # 检查时间跨度是否大于 1 天
                            if returns.index[-1] - returns.index[0] > pd.Timedelta(days=2):
                                try: # 生成 PyFolio/QuantStats 报告
                                    quantstats.reports.html(
                                        returns, 
                                        output=f'{stock_code}.html', 
                                        title=f'{stock_code} Sentiment', 
                                        warn_singular=False
                                    )
                                    stock_logger.info(f'Generated report for {stock_code}')
                                except ZeroDivisionError as e:
                                    stock_logger.info(f'Error generating report for {stock_code}: {e}')
                            else:
                                stock_logger.info(f'Skipping report generation for {stock_code}: Insufficient data (less than 1 year)')
                        else:
                            stock_logger.info("PyFolio Returns: Not a pandas Series")
                    else:
                        stock_logger.info("PyFolio Data: No Returns Data")

                elif analyzer_name == 'custom_analyzer': # 打印自定义分析器的报告
                    if result != 'No Custom Analyzer Data':
                        stock_logger.info('------------------- 自定義分析器报告 -------------------')

                        for report in result.get('daily', []):
                            date = report['date']
                            profit_loss_ratio = report['profit_loss_ratio']
                            max_profit = report['max_profit']
                            max_loss = report['max_loss']
                            avg_duration = format_duration(report['avg_duration'])
                            max_duration = format_duration(report['max_duration'])
                            min_duration = format_duration(report['min_duration'])
                            
                            stock_logger.info(f"日期: {date}, 代號: {stock_code}")
                            stock_logger.info(f"损益比: {profit_loss_ratio:.2f}")
                            stock_logger.info(f"单笔最大盈利: {max_profit:.2f}")
                            stock_logger.info(f"单笔最大亏损: {max_loss:.2f}")
                            stock_logger.info(f"平均持仓时间: {avg_duration} 秒")
                            stock_logger.info(f"最长持仓时间: {max_duration} 秒")
                            stock_logger.info(f"最短持仓时间: {min_duration} 秒")
                            
                            data2append['date'] = date
                            data2append['profit_loss_ratio'] = profit_loss_ratio
                            data2append['max_profit'] = max_profit
                            data2append['max_loss'] = max_loss
                            data2append['avg_duration'] = avg_duration
                            data2append['max_duration'] = max_duration
                            data2append['min_duration'] = min_duration

            else:
                stock_logger.info(f"{analyzer_name}: No Data")

        if params.get('isPlot', False) is True:
            cerebro.plot()
        
        return data2append
    
    else:
        stock_logger.info(f"没有交易记录, 交易{results[0].trade_count}次")
        
    return {} # 因為沒交易所以定義為空

# ---------------------------- 加載回測資料 ----------------------------

period = "3days" # 要檢測哪類型的(3day漲停清單, 5day漲停清單)
year = "2023"
date_to_test = "2023-01-03"

# 建立回測參數, 可以自行調整
params = {
    'isPlot': False, # 是否繪製回測結果
    'long_short': True, # Strategy策略選擇 => (True: 做多, False: 做空)
    'dynamic': False, # 動態(靜態)止盈止損 => (True: 動態, False: 靜態)
    'ticks': 5, # 價差ticks的級距(決定是否入場的tick數)
    'profit_ratio': 0.03, # 止盈比例
    'loss_ratio': 0.01, # 止損比例
    'volume_threshold': 10, # 成交量門檻
    'period': 30, # 監測時間秒數
    'last_trade_minute': 0, # 最後交易「分鐘」(13:xx)
    'last_trade_hour': 13, # 最後交易「小時」(xx:00)
    'initial_cash': 100000, # 初始現金
}

#加載動態路徑
list_dir = f"./list/股票清單_{period}/{year}"
tick_dir = f"./list/Ticks_{period}/{year}"

# Step 1: 讀取stocklist中的交易股票代號
list_file = os.path.join(list_dir, f"stocklist_{date_to_test.replace('-', '_')}_{period}.csv")
stock_list = pd.read_csv(list_file)

def process_stock(stock_code, shared_result):
    tick_file = os.path.join(tick_dir, date_to_test, f"{stock_code}_{date_to_test}_ticks.csv").replace('\\', '/')
    openprice_file = os.path.join(tick_dir, date_to_test, f"{stock_code}_{date_to_test}_openprice.csv").replace('\\', '/')

    if os.path.exists(tick_file) and os.path.exists(openprice_file):
        # 讀取tick和openprice資料
        tick_df = pd.read_csv(tick_file)
        openprice_df = pd.read_csv(openprice_file)
        
        # 使用pandas.concat將openprice的資料加在tick資料的最前面
        if len(openprice_df) == 1:
            combined_df = pd.concat([openprice_df, tick_df], ignore_index=True)
            combined_df['ts'] = pd.to_datetime(combined_df['ts'])
            combined_df.rename(columns={'ts': 'datetime'}, inplace=True)
            combined_df['open'] = combined_df['close'].shift(1)
            combined_df['open'].fillna(combined_df['close'].iloc[0], inplace=True)
            combined_df.set_index('datetime', inplace=True)

            stock_logger = setup_logger(stock_code)
            bt_result = start_backtest(df=combined_df, stock_code=stock_code, stock_logger=stock_logger, shared_result=shared_result, **params)
            
            # 關閉 logger 的所有 handler
            for handler in stock_logger.handlers:
                handler.close()
                stock_logger.removeHandler(handler)
            
            # 創建目標資料夾
            result_dir = os.path.join('result', f"{date_to_test}")
            if not os.path.exists(result_dir):
                os.makedirs(result_dir)

            code_dir = os.path.join(result_dir, f"{date_to_test}_{stock_code}_{'Long' if params['long_short'] else 'Short'}_{'dynamic' if params['dynamic'] else 'static'}")
            os.makedirs(code_dir, exist_ok=True)

            # 指定要移動的文件
            log_file = f"{stock_code}_trading_log.log"
            html_report_file = f"{stock_code}.html"

            # 移動文件到新資料夾
            if os.path.exists(log_file):
                shutil.move(log_file, os.path.join(code_dir, log_file))
            else:
                print(f"{log_file} 不存在")

            if os.path.exists(html_report_file):
                shutil.move(html_report_file, os.path.join(code_dir, html_report_file))
            else:
                print(f"{html_report_file} 不存在")
        else:
            print(f"Openprice檔案 {openprice_file} 中的資料不止一筆")
    else:
        print(f"資料不存在: {tick_file} 或 {openprice_file}")
    
    bt_result['result_dir'] = result_dir
    return shared_result.append(bt_result)  # 回傳result資料夾路徑


if __name__ == "__main__":
    # Step 1: 讀取stocklist中的交易股票代號
    list_file = os.path.join(list_dir, f"stocklist_{date_to_test.replace('-', '_')}_{period}.csv")
    stock_list = pd.read_csv(list_file)

    # 取出代號
    stock_codes = stock_list['Stock Code'].tolist()

    # 使用多進程來處理回測
    with Manager() as manager:
        shared_result = manager.list()  # 创建一个共享列表

        # 使用多进程来处理回测
        with Pool(processes=4) as pool:
            pool.starmap(process_stock, [(code, shared_result) for code in stock_codes])

        # 全部回測結束後統計
        analyze_global_results(list(shared_result), **params)  # 将结果传递给分析函数
