import backtrader as bt
from datetime import datetime, time, timedelta
import os, math, logging, shutil
from multiprocessing import Pool, Manager
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
import quantstats
import sys
import traceback

# ------------------------ 自定义pandas data ------------------------

class PandasData(bt.feeds.PandasData):
    lines = ('open', 'close', 'volume', 'bid_price', 'bid_volume', 'ask_price', 'ask_volume', 'tick_type')
    params = (
        ('datetime', None),
        ('open', 'open'),
        ('high', -1),
        ('low', -1),
        ('close', 'close'),
        ('volume', 'volume'),
        ('bid_price', 'bid_price'),
        ('ask_price', 'ask_price'),
        ('bid_volume', 'bid_volume'),
        ('ask_volume', 'ask_volume'),
        ('tick_type', 'tick_type'),
        ('openinterest', -1),
    )

# -------------------------- 信号与策略 -------------------------

from collections import deque, OrderedDict

class PriceVolumeSignal(bt.Signal):
    lines = ('signal',)
    params = (('period', 30),
              ('volume_threshold', 10),
              ('logger', None)
    )
    plotinfo = dict(subplot=False)
    plotlines = dict(
        signal=dict(
            color='red',
            linewidth=2.0,
        )
    )

    def __init__(self):
        self.price_volume_dict = {}
        self.snapshot_dict = {}
        self.reset()
        self.previous_date = None
        self.logger = self.params.logger
        self.logger.info(f'The Monitor Period is: {self.params.period} seconds, volume_threshold: {self.params.volume_threshold}')

    def reset(self):
        self.high_price = 0
        self.low_price = 0
        self.high_price_stable = deque(maxlen=self.params.period)
        self.low_price_stable = deque(maxlen=self.params.period)
        self.price_limits = {}
        self.has_updated_today = False
        self.prev_time = None
        
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
            self.price_volume_dict[dt] = {}
            self.snapshot_dict[dt] = {}
  
        # 如果還沒有更新當天的字典
        if not self.has_updated_today:
            # 如果時間是 13:30:00 或當天的第一筆資料
            if current_time >= pd.Timestamp('09:00:00').time() and not self.has_updated_today:
                self._store_price_limits(self.data.close[-1])
                self.has_updated_today = True  # 標記當天已更新
                
            if current_time == pd.Timestamp('13:30:00').time():  # 根据你的数据时间设定
                self.has_updated_today = False

        # 更新每秒的成交量累加和收盘价
        if self.prev_time is None:  # 如果是第一笔数据
            self.prev_time = current_time

        # 使用日期更新價量字典
        if dt not in self.price_volume_dict:
            self.price_volume_dict[dt] = {}
            self.logger.info(f'Created new entry in price_volume_dict for date {dt}')

        if close not in self.price_volume_dict[dt]:
            self.price_volume_dict[dt][close] = volume
        else:
            self.price_volume_dict[dt][close] += volume
        
        # 对价格进行排序，并创建一个新的有序字典
        sorted_dict = OrderedDict(sorted(self.price_volume_dict[dt].items(), key=lambda x: x[0], reverse=True))

        # 更新当天的價量字典为排序后的字典
        self.price_volume_dict[dt] = sorted_dict
        
        # 保存这个时间点的快照
        if current_time not in self.snapshot_dict[dt]:
            self.snapshot_dict[dt][current_time] = {}
            
        self.snapshot_dict[dt][current_time] = sorted_dict.copy()
        self.logger.info(f'當前秒數{current_time}, 尚未更新秒數{self.prev_time}, 當前最高價{self.snapshot_dict[dt]}')
            
        # 如果时间不同于上一秒，处理上一秒的累计数据
        if current_time != self.prev_time:
            # 更新上一秒的时间
            self.prev_time = current_time

            order_dict = self.price_volume_dict[dt]

            # 获取当前最高价和最低价
            current_high_price = next(iter(order_dict))
            current_low_price = next(reversed(order_dict))
            
            
            self.logger.info(f'當前秒數{current_time}, 成交量累加時間(扣除1秒){self.prev_time}, 當前最高價{current_high_price}')
            
            
            current_high_volume = order_dict[current_high_price]
            current_low_volume = order_dict[current_low_price]
            
            
            self.logger.info(f'當前秒數{current_time}, 成交量累加時間(扣除1秒){self.prev_time}, 當前最高價的量{current_high_volume}')   
            

            # 检查最高价是否刷新(做空)
            if current_high_price != self.high_price:
                self.high_price = current_high_price
                self.high_price_stable.clear()  # 最高价更新时，清空追踪队列
                self.logger.info(f'當前秒數{current_time}, 做空價格刷新{current_high_price}')   
            else:
                self.high_price_stable.append(current_high_volume) # 添加当前的最高价成交量到队列中

            # 检查最低价是否刷新(做多)
            if current_low_price != self.low_price:
                self.low_price = current_low_price
                self.low_price_stable.clear()  # 最低价更新时，清空追踪队列
                self.logger.info(f'當前秒數{current_time}, 做多價格刷新{current_low_price}')   
            else:
                self.low_price_stable.append(current_low_volume) # 添加当前的最低价成交量到队列中


            # 如果收盘价小于 limit_short_danger，只能检查 high_price_stable 生成做多信号
            if close <= self.price_limits['limit_short_danger'] or close >= self.price_limits['limit_long_danger']:
                self.logger.info(f'當前秒數{current_time}, 當前價格危險 {close}, 不進行訊號判斷')
                self.high_price_stable.clear()
                self.low_price_stable.clear()
                return
            


            self.logger.info(f'當前秒數{current_time}, 成交量累加時間(扣除1秒){self.prev_time}, 每秒的成交量{self.high_price_stable}, 監測秒數{len(self.high_price_stable)}')
            self.logger.info(f'當前秒數{current_time}, 成交量累加時間(扣除1秒){self.prev_time}, 每秒的價量{order_dict}')

            
            
            # 如果沒超過危險空的危險金額, 检查最高价的成交量是否小于指定成交量并持续指定的秒數
            if len(self.high_price_stable) == self.params.period and all(v < self.params.volume_threshold for v in self.high_price_stable):
                self.lines.signal[0] = -1  # 生成做空信号
                self.logger.info(f'當前秒數{current_time}, 符合條件連續N秒成交量小於10, 生成空訊號\n')
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
                        round((math.floor(close_price * (1 + 0.10) / 0.1) * 0.1), 2) if 50 <= close_price < 100 else 
                        round((math.floor(close_price * (1 + 0.10) / 0.5) * 0.5), 1) if close_price >= 100 else 
                        round(close_price * (1 + 0.10), 2),
            
            'limit_down': round((math.ceil(close_price * (1 - 0.10) / 0.05) * 0.05), 2) if 10 <= close_price < 50 else 
                          round((math.ceil(close_price * (1 - 0.10) / 0.1) * 0.1), 2) if 50 <= close_price < 100 else 
                          round((math.ceil(close_price * (1 - 0.10) / 0.5) * 0.5), 1) if close_price >= 100 else 
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
              ('pct_entry', True), # 百分比進場
              ('logger', None) # log紀錄
    )
    
    def __init__(self):
        # 记录手續費, 金額
        self.total_commission = 0.0
        self.trade_count = 0
        self.initial_cash = self.broker.get_cash()
        self.logger = self.params.logger
        self.highest_price = None
        self.lowest_price = None
        
        # 策略使用
        self.signal = PriceVolumeSignal(period=self.params.period, volume_threshold=self.params.volume_threshold, logger=self.params.logger) # 訊號
        self.reset_order()
        
    def reset_order(self):
        self.long_order, self.short_order = None, None # 多空訂單
        self.long_stop_order, self.short_stop_order = None, None # 多空止損訂單
        self.long_profit_order, self.short_profit_order = None, None # 多空止盈訂單
        self.long_profit_price, self.long_loss_price = None, None # 多單止盈止損價格
        self.short_profit_price, self.short_loss_price = None, None # 空單止盈止損價格
        self.order_placed = False
        
    def next(self):
        dt = self.data.datetime.date(0).strftime('%Y-%m-%d')
        if dt not in self.signal.price_volume_dict:
            self.logger.info(f'In Strategy: The date {dt} is not in price_volume_dict')
            self.logger.info(f'Dates in price_volume_dict: {list(self.signal.price_volume_dict.keys())}')
            self.reset_order()
            return
        
        # 超過下午1點, 全數平倉不交易, 所有訂單回歸預設
        current_time = self.data.datetime.time(0)
        if current_time > time(self.params.last_trade_hour, self.params.last_trade_minute):
            if self.position:
                self.close()
                if self.long_stop_order:
                    self.cancel(self.long_stop_order)
                elif self.long_profit_price:
                    self.cancel(self.long_profit_price)
                elif self.short_stop_order:
                    self.cancel(self.short_stop_order)
                elif self.short_profit_order:
                    self.cancel(self.short_profit_order)
                    
                self.reset_order()
                self.logger.info(f"All positions closed at {self.data.datetime.datetime(0)} due to end of trading time.\n")
            return

        # 價量字典中兩個價格的差額
        close_price = self.data.close[0]
        bid_price = self.data.bid_price[0]
        ask_price = self.data.ask_price[0]

        # 如果有倉位, 且為動態止盈止損
        if self.position and self.params.dynamic:
            if self.position.size > 0:  # 多單
                self.logger.info(f"\n當前有多單倉位, 且為動態止盈止損")
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
                    self.logger.info(f'\nIn Long Order: current close price {new_profit_stop} bigger than previous profit stop {self.long_profit_price}')
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
                self.logger.info(f"\n當前有空單倉位, 且為動態止盈止損")
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
                    self.logger.info(f'\nIn Short Order: current close price {new_profit_stop} smaller than previous profit stop {self.short_profit_price}')
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

        # 只有没有仓位并且还没有开过仓，才會有進行訊號判斷與進場
        if self.order_placed is True:
            return

        # 目前無訂單與倉位, 開始計算訊號產生的最高價格和最低價格
        if self.signal.lines.signal[0] == -1 and self.params.long_short is False: # 訊號多空判斷
            if self.short_order: # 如果已經下空單，則不再下單
                return
                    
            # 获取特定时间点的价量快照此处的 highest_price 为当前时间的最高价，而非最终最高价
            self.logger.info(f' cal me at singal {current_time}')
            if dt in self.signal.snapshot_dict:
                snapshot = self.signal.snapshot_dict[dt][current_time]
                cur_order_dict = next(iter(snapshot))
                self.logger.info(f'abc {cur_order_dict}')
                highest_price = snapshot[cur_order_dict]
                self.logger.info(f'def {highest_price}')
                self.highest_price = highest_price  # Sizer使用最高價最低價
            else:
                self.logger.info(f"No snapshot data for {dt} in Short singal")
                return
            
            self.logger.info(f'\n------------------- SHORT SIGNAL -1 -------------------')
            if highest_price > bid_price and highest_price > ask_price:
                # 调用动态 tick 计算方法
                target_price = self.calculate_dynamic_ticks(highest_price, self.params.ticks, type='short')

                if target_price < bid_price and target_price < ask_price:
                    self.short_order = self.sell(exectype=bt.Order.Limit, price=bid_price)
                    stop_loss_price, profit_stop_price = self._dynamic_order_caculate(highest_price, bid_price, type='short')
                    self.short_loss_price = highest_price
                    self.short_profit_price = profit_stop_price
                    self.logger.info(f"SHORT STRATEGY EXECUTED AT {self.data.datetime.datetime(0)}, Highest Price: {highest_price}, Close Price: {close_price}, Bid (Entry) Price: {bid_price}, Ask Price: {ask_price}, Stop Loss: {stop_loss_price}, Profit Stop: {profit_stop_price}")
                    self.place_order = True  # 标记今天已经下单
                else:
                    self.logger.info(f"SHORT STRATEGY NO TRADE AT {self.data.datetime.datetime(0)}, Highest Price: {highest_price}, Close Price: {close_price}, Bid Price: {bid_price}, Ask Price: {ask_price}, Entry Price {bid_price} and Ask Price {ask_price} Need Above: {target_price}")
        
        elif self.signal.lines.signal[0] == 1 and self.params.long_short is True:
            if self.long_order: # 如果已經下多單，則不再下單
                return
            
            # 获取特定时间点的价量快照此处的 lowest_price 为当前时间的最低價，而非最终最低價
            if dt in self.signal.snapshot_dict:
                snapshot = self.signal.snapshot_dict[dt][current_time]
                cur_order_dict = next(reversed(snapshot))
                lowest_price = snapshot[cur_order_dict]
                self.lowest_price = lowest_price # Sizer使用最高價最低價
            else:
                self.logger.info(f"No snapshot data for {dt} in Long singal")
                return
            
            self.logger.info(f'\n------------------- LONG SINGAL 1 -------------------')
            if lowest_price < bid_price and lowest_price < ask_price:
                # 调用动态 tick 计算方法
                target_price = self.calculate_dynamic_ticks(lowest_price, self.params.ticks, type='long')
                
                if target_price > bid_price and target_price > ask_price:
                    self.long_order = self.buy(exectype=bt.Order.Limit, price=bid_price)
                    stop_loss_price, profit_stop_price = self._dynamic_order_caculate(lowest_price, bid_price, type='long')
                    self.long_loss_price = lowest_price
                    self.long_profit_price = profit_stop_price
                    self.logger.info(f"LONG STRATEGY EXECUTED AT {self.data.datetime.datetime(0)}, Lowest Price: {lowest_price}, Close Price: {close_price}, Bid (Entry) Price: {bid_price}, Ask Price: {ask_price}, Stop Loss: {stop_loss_price}, Profit Stop: {profit_stop_price}")
                    self.place_order = True  # 标记今天已经下单
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
            self.logger.info('------------------- 本次訂單成功結束 -------------------\n')

            self.total_commission += round(order.executed.comm, 2)
            return

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.logger.info('\n------------------- 訂單重設/取消 -------------------\n')
            self.logger.info(f'Order Canceled/Margin/Rejected, Ref: {order.ref}, Status: {order.getstatusname()}')
            self.long_order, self.short_order = None, None
            return

        # 無訂單狀態
        return
        
    def notify_trade(self, trade):
        if trade.isclosed:  # 当交易完成时记录盈亏
            self.logger.info('\n------------------- 本次交易盈虧統計 -------------------')
            
            entry_time = bt.num2date(trade.dtopen)
            exit_time = bt.num2date(trade.dtclose)
            pnlcomm = round(trade.pnlcomm, 2) # 带有佣金的盈亏
            trade_ref = trade.ref  # 交易的ref编号
            self.trade_count += 1
            
            self.logger.info(f'完整買賣交易結束. 買賣交易代號: {trade_ref}, '
                     f'入場時間: {entry_time}, '
                     f'出場時間: {exit_time}, '
                     f'損益(含手續費): {pnlcomm}'
                    )

    def stop(self):
        self.logger.info('\n------------------ 全部交易結束資料統計 ------------------')
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

# -------------------------- 自定義Sizer部位大小 -------------------------

class DynamicSizer(bt.Sizer):
    params = (('initial_cash', 100000),) # 初始資金設定

    def _getsizing(self, comminfo, cash, data, isbuy):
        # 取得策略中的最高價或最低價
        strategy = self.strategy  # 取得當前策略
        if isbuy:
            # 做多，使用策略中的最低價格
            price = strategy.lowest_price
        else:
            # 做空，使用策略中的最高價格
            price = strategy.highest_price
        
        # 確保價格有效
        if price > 0:
            # 計算可購買的股數：initial_cash / price，再除以1000取整數，最後乘回1000
            size = int((self.params.initial_cash / price) / 1000) * 1000
        else:
            size = 0  # 若價格無效，則不下單

        return size  # 返回計算的股數

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
    # 按日期和策略类型分组结果
    result_groups = {}
    for result in result_list:
        date = result.get('date')
        strategy_type = 'Long' if params.get('long_short', True) else 'Short'
        dynamic_type = 'dynamic' if params.get('dynamic', False) else 'static'
        key = (date, strategy_type, dynamic_type)
        if key not in result_groups:
            result_groups[key] = []
        result_groups[key].append(result)
    
    # 对每组结果进行分析
    for (date, strategy_type, dynamic_type), results in result_groups.items():
        dir_path = os.path.join('result', date)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        
        total_stocks = len(results)
        if total_stocks == 0:
            print(f'No trading results for {date}, skipping analysis')
            continue
        
        win_count = sum(1 for result in results if 'pnl' in result and result['pnl'] > 0)
        total_pnl = sum(result['pnl'] for result in results if 'pnl' in result)
        
        win_rate = win_count / total_stocks * 100 if total_stocks > 0 else 0
        avg_pnl = total_pnl / total_stocks if total_stocks > 0 else 0

        # 全局持仓时间和损益统计
        total_avg_duration = sum(result.get('avg_duration', 0) for result in results) / total_stocks
        total_max_duration = max(result.get('max_duration', 0) for result in results)
        total_min_duration = min(result.get('min_duration', float('inf')) for result in results)
        total_max_profit = max(result.get('max_profit', 0) for result in results)
        total_max_loss = max(result.get('max_loss', 0) for result in results)
        
        # 写入到文件
        filename = f"{strategy_type}_{dynamic_type}_{date}.txt"
        with open(os.path.join(dir_path, filename), 'w', encoding='utf-8') as f:
            f.write('------------------- 股票池结果 -------------------\n')  

            total_pnl = 0
            for result in results:
                if 'stock_code' in result and 'pnl' in result:
                    pnl = round(result['pnl'], 2)
                    f.write(f"股票代號: {result['stock_code']}, 盈虧: {pnl}\n")
                    total_pnl += pnl

            f.write(f"\n今天的總盈虧為: {round(total_pnl, 2)}\n")
            
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
            f.write(f"止盈: {(params.get('profit_ratio', 0)) * 100}%, 止损: {(params.get('loss_ratio', 0)) * 100}%\n")
            f.write(f"进场Ticks价差数: {params.get('ticks', 0)}\n")
            f.write(f"成交量门槛: {params.get('volume_threshold', 0)}\n")
            f.write(f"成交量观察秒数: {params.get('period', 0)}\n")
            f.write(f"平仓时间: {params.get('last_trade_hour', 0)}:{params.get('last_trade_minute', 0)} \n")
            
    print('本次交易參數:', params)
        
def start_backtest(df, stock_code, stock_logger, **params):
    stock_logger.info(f'The current stock code is: {stock_code}')
    if df.empty:
        stock_logger.info(f'Empty dataframe for stock {stock_code}. Skipping.')
        return {}
    # 设置数据源并运行策略
    data = PandasData(dataname=df, timeframe=bt.TimeFrame.Seconds, compression=1)

    cerebro = bt.Cerebro()
    cerebro.adddata(data, name=str(stock_code))
    cerebro.addstrategy(MyStrategy, logger=stock_logger, 
                        ticks=params.get('ticks', 5), 
                        profit_ratio=params.get('profit_ratio', 0.03), 
                        loss_ratio=params.get('loss_ratio', 0.01), 
                        dynamic=params.get('dynamic', False), 
                        period=params.get('period', 30), 
                        volume_threshold=params.get('volume_threshold', 10), 
                        last_trade_minute=params.get('last_trade_minute', 0), 
                        last_trade_hour=params.get('last_trade_hour', 13), 
                        long_short=params.get('long_short', True)
                    )

    # 設置初始資金和手續費
    initial_cash = params.get('initial_cash', 100000)
    cerebro.broker.set_cash(initial_cash)
    cerebro.broker.setcommission(commission=0.0025)

    # 添加自定義的sizer
    cerebro.addsizer(DynamicSizer, initial_cash=initial_cash)

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

def get_valid_trading_dates(start_date, end_date, tick_dir):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    valid_dates = []
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        date_dir = os.path.join(tick_dir, date_str)
        if os.path.exists(date_dir):
            valid_dates.append(date_str)
        else:
            print(f"Skipping date {date_str}: No data directory found in {date_dir}")
        current += timedelta(days=1)
    return valid_dates

def process_stock(stock_code, date_to_test, shared_result, tick_dir, params):
    print(f"Processing stock {stock_code} for date {date_to_test}")
    
    tick_file = os.path.join(tick_dir, date_to_test, f"{stock_code}_{date_to_test}_ticks.csv")
    openprice_file = os.path.join(tick_dir, date_to_test, f"{stock_code}_{date_to_test}_openprice.csv")

    print(f"Tick file: {tick_file}")
    print(f"Open price file: {openprice_file}")

    if os.path.exists(tick_file) and os.path.exists(openprice_file):
        try:
            tick_df = pd.read_csv(tick_file)
            openprice_df = pd.read_csv(openprice_file)
            
            # print(f"Tick data first date: {tick_df['ts'].min()}")
            # print(f"Tick data last date: {tick_df['ts'].max()}")
            # print(f"Open price data date: {openprice_df['ts'].iloc[0]}")
            
            if tick_df.empty or openprice_df.empty:
                print(f"Skipping stock {stock_code} for date {date_to_test}: Empty data")
                return

            required_columns = ['ts', 'close', 'volume', 'bid_price', 'bid_volume', 'ask_price', 'ask_volume', 'tick_type']
            if not all(col in tick_df.columns for col in required_columns):
                print(f"Skipping stock {stock_code} for date {date_to_test}: Missing required columns")
                return
            
            if len(openprice_df) == 1:
                combined_df = pd.concat([openprice_df, tick_df], ignore_index=True)
                combined_df['ts'] = pd.to_datetime(combined_df['ts'])
                combined_df.rename(columns={'ts': 'datetime'}, inplace=True)
                combined_df['open'] = combined_df['close'].shift(1)
                combined_df.fillna({'open': combined_df['close'].iloc[0]}, inplace=True)
                combined_df.set_index('datetime', inplace=True)

            stock_logger = setup_logger(stock_code)
            bt_result = start_backtest(df=combined_df, stock_code=stock_code, stock_logger=stock_logger, **params)
            
            for handler in stock_logger.handlers:
                handler.close()
                stock_logger.removeHandler(handler)
            
            result_dir = os.path.join('result', f"{date_to_test}")
            if not os.path.exists(result_dir):
                os.makedirs(result_dir)

            code_dir = os.path.join(result_dir, f"{date_to_test}_{stock_code}_{'Long' if params['long_short'] else 'Short'}_{'dynamic' if params['dynamic'] else 'static'}")
            os.makedirs(code_dir, exist_ok=True)

            log_file = f"{stock_code}_trading_log.log"
            html_report_file = f"{stock_code}.html"

            if os.path.exists(log_file):
                shutil.move(log_file, os.path.join(code_dir, log_file))
            else:
                print(f"{log_file} 不存在")

            if os.path.exists(html_report_file):
                shutil.move(html_report_file, os.path.join(code_dir, html_report_file))
            else:
                print(f"{html_report_file} 不存在")

            bt_result['result_dir'] = result_dir
            bt_result['date'] = date_to_test
            shared_result.append(bt_result)
            
        except pd.errors.EmptyDataError:
            print(f"Error processing stock {stock_code} for date {date_to_test}: Empty data file")
        except Exception as e:
            print(f"Unexpected error processing stock {stock_code} for date {date_to_test}: {str(e)}")
            print("Traceback:")
            traceback.print_exc()
    else:
        print(f"Data files not found for stock {stock_code} on date {date_to_test}")

if __name__ == "__main__":
    period = "5days"
    year = "2024"
    
    params = {
        'isPlot': False,
        'long_short': False,
        'dynamic': False,
        'ticks': 8,
        'profit_ratio': 0.03,
        'loss_ratio': 0.01,
        'volume_threshold': 10,
        'period': 30,
        'last_trade_minute': 0,
        'last_trade_hour': 13,
        'initial_cash': 200000,
    }

    # 修正目录路径
    list_dir = os.path.join("testdata", "list", f"股票清單_{period}", year)
    tick_dir = os.path.join("testdata", "list", f"Ticks_{period}", year)

    print(f"List directory: {os.path.abspath(list_dir)}")
    print(f"Tick directory: {os.path.abspath(tick_dir)}")

    start_date = "2024-10-01"
    end_date = "2024-10-01"

    valid_dates = get_valid_trading_dates(start_date, end_date, tick_dir)

    if not valid_dates:
        print(f"No valid trading dates found between {start_date} and {end_date}")
    else:
        print(f"Valid trading dates: {valid_dates}")

    all_results = []
    for date_to_test in valid_dates:
        print(f"Processing date: {date_to_test}")
        
        if datetime.strptime(date_to_test, "%Y-%m-%d") < datetime.strptime(start_date, "%Y-%m-%d") or \
        datetime.strptime(date_to_test, "%Y-%m-%d") > datetime.strptime(end_date, "%Y-%m-%d"):
            print(f"Skipping date {date_to_test}: Out of specified range")
            continue
        
        # 修正文件名格式
        list_file = os.path.join(list_dir, f"stocklist_{date_to_test.replace('-', '_')}_{period}.csv")
        print(f"Looking for stock list file: {os.path.abspath(list_file)}")
        
        if not os.path.exists(list_file):
            print(f"Stock list file not found for date {date_to_test}, skipping.")
            continue

        try:
            stock_list = pd.read_csv(list_file)
            stock_codes = stock_list['Stock Code'].tolist()
            print(f"Found {len(stock_codes)} stocks for date {date_to_test}")

            with Manager() as manager:
                shared_result = manager.list()

                with Pool(processes=16) as pool:
                    pool.starmap(process_stock, [(code, date_to_test, shared_result, tick_dir, params) for code in stock_codes])

                all_results.extend(list(shared_result))
        # 單進程處理每個股票
        # try:
        #     stock_list = pd.read_csv(list_file)
        #     stock_codes = stock_list['Stock Code'].tolist()
        #     print(f"Found {len(stock_codes)} stocks for date {date_to_test}")

        #     shared_result = []

        #     # 單進程處理每個股票
        #     for code in stock_codes:
        #         process_stock(code, date_to_test, shared_result, tick_dir, params)

        #     # 繼續其他的分析處理
        #     all_results.extend(shared_result)

        except Exception as e:
            print(f"Error processing date {date_to_test}: {str(e)}")

    if all_results:
        analyze_global_results(all_results, **params)
    else:
        print("No results to analyze. Check if any valid trading dates were processed.")

    print("Backtest completed for all dates.")
