# -*- coding: utf-8 -*-
# @Time    : 2018/3/13 16:15
# @Author  : xujunyong
import os
import re
import json
import socket
import traceback
from datetime import datetime, date, time
from threading import Event, RLock
from collections import defaultdict
from source.ctp.wrapper import Trading, MarketData, OrderProcedure, HUGE_VAL


class Order:
    def __init__(self, instrument, direction, offset_flag, hedge_flag, volume, price, timeout=None, repeat=None):
        """

        Args:
            instrument:   合约名称 bytes类型 例如: b'IF1805'
            direction:    买卖方向，可选参数 PyCTP.THOST_FTDC_D_Buy: '买', PyCTP.THOST_FTDC_D_Sell: '卖'
            offset_flag:  开平方向，可选参数   PyCTP.THOST_FTDC_OF_Open: '开',
                                               PyCTP.THOST_FTDC_OF_Close: '平',
                                               PyCTP.THOST_FTDC_OF_ForceClose: '强平',
                                               PyCTP.THOST_FTDC_OF_CloseToday: '平今',
                                               PyCTP.THOST_FTDC_OF_CloseYesterday: '平昨',
                                               PyCTP.THOST_FTDC_OF_ForceOff: '强减',
                                               PyCTP.THOST_FTDC_OF_LocalForceClose: '本地强平'
                          一般使用PyCTP.THOST_FTDC_OF_Open/PyCTP.THOST_FTDC_OF_Close/PyCTP.THOST_FTDC_OF_CloseToday/PyCTP.THOST_FTDC_OF_CloseYesterday
                          上期所平仓分平今和平昨，其他交易所不作区分
            hedge_flag:   套保标志，可选参数 PyCTP.THOST_FTDC_HF_Speculation: '投',
                                             PyCTP.THOST_FTDC_HF_Arbitrage: '套',
                                             PyCTP.THOST_FTDC_HF_Hedge: '保'
                          一般开投机仓
            volume:       交易量
            price:        挂单价，未避免出错，目前只支持限价单，可选参数： 数字：按照当前价格挂限价单
                                                                           字符串：'BidPrice1' 最新行情的买一价挂单
                                                                           字符串：'AskPrice1' 最新行情的卖一价挂单
                                                                           字符串：'MidPrice'  最新行情的卖一卖一中间价挂单
            timeout:      数值类型，挂单时间超过指定秒数仍未完全成交时自动撤单，timeout为-1时不等待直接返回，timeout为None是等待直到交易完成或挂单失效
            repeat:       超时后重新挂单的次数，仅在timeout大于0且price为数值时生效
                          例如以AskPrice1的价格买入10手IF1809，超时时间为20s，repeat为2，第一次以最新卖一价挂单，直到超时仅成交4手，此时超时撤单，然后以最新的卖一价挂6手的买单，直到成交或超时，之后不再挂单
        """
        self.instrument = instrument
        self.direction = direction
        self.offset_flag = offset_flag
        self.hedge_flag = hedge_flag
        self.volume = volume
        self.price = price
        self.timeout = timeout
        self.repeat = repeat
        if isinstance(self.price, str):
            assert self.price in ('AskPrice1', 'BidPrice1', 'MidPrice')
        if self.repeat is not None and self.repeat > 1:
            assert self.timeout > 0 and isinstance(self.price, str)

    def __str__(self):
        return 'Order(%s %s %s %s %s %s %s %s)' % (
                self.instrument, self.direction, self.offset_flag, self.hedge_flag, self.volume, self.price,
                self.timeout, self.repeat)


class OrderTrader:

    def __init__(self, cfg, name):
        """
        交易接口
        Args:
            cfg:    配置文件路径，一般以ctp.json命名，每个账户对应如下配置项，以上期模拟账户为例：
                    "simnow1": {                           # 账户名称
                        "TraderFront": [                   # 交易服务器，可以配置多个，程序自动选择可用服务器
                            "tcp://180.168.146.187:10001",
                            "tcp://180.168.146.187:10000",
                            "tcp://218.202.237.33 :10002"
                        ],
                        "TraderCache": "_tmp_simnow_t_",    # 交易api缓存，仅包含字母和下划线，每个账户的缓存名称需保持唯一
                        "MarketFront": [                    # 行情服务器，可以配置多个，程序自动选择可用服务器
                            "tcp://180.168.146.187:10011",
                            "tcp://180.168.146.187:10010",
                            "tcp://218.202.237.33 :10012"
                        ],
                        "MarketCache": "_tmp_simnow_m_",    # 行情api缓存，仅包含字母和下划线，每个账户的缓存名称需保持唯一
                        "Broker": "9999",                   # 期货公司代码
                        "User": "76871313",                 # 账号
                        "Password": "5746531"               # 密码
                        "Client": "SHINNYQ7V2",             # 客户端名称，看穿式监管验证必须填写
                        "AuthCode": "ASDFAFASDFASFA"        # 客户端校验码，看穿式监管验证必须填写
                    },
            name:   配置文件中对应的账号
        """
        self.cfg = cfg
        self.name = name
        self.market_subs = {}
        self.market_data, self.market_event, self.market_lock = dict(), defaultdict(set), RLock()

        with open(cfg, 'r', encoding='utf-8') as f:
            params = json.load(f)[name]
        broker = params['Broker'].encode('gbk')
        user, password = params['User'].encode('gbk'), params['Password'].encode('gbk')

        trader_front = self.get_available_ctp_server(params['TraderFront'])
        if trader_front is not None:
            self.trader = Trading(params['TraderCache'].encode('gbk'))
            self.trader.Connect(trader_front.encode('gbk'))
            if 'Client' in params:
                client = params['Client'].encode('gbk')
                auth_code = params['AuthCode'].encode('gbk')
                self.trader.ReqAuthenticate(broker, user, client, auth_code)
            self.trader.Login(broker, user, password)
            self.trader.ReqSettlementInfoConfirm()
        else:
            print('无可用交易服务器!')
            self.trader = None

        market_front = self.get_available_ctp_server(params['MarketFront'])
        if market_front is not None:
            self.market = MarketData(params['MarketCache'].encode('gbk'))
            self.market.Connect(market_front.encode('gbk'))
            self.market.Login(broker, user, password)
        else:
            print('无可用行情服务器!')
            self.market = None

    def logout(self):
        if self.trader is not None:
            self.trader.Logout()
        # if self.market is not None:
        #     self.market.Logout()

    @staticmethod
    def check_ctp_server(address):
        """
            检查服务器是否可用
        Args:
            address: 服务器地址及端口

        Returns:

        """
        match = re.match(r'^tcp://(.*):(\d+)$', address)
        if match:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3)
            url, port = match.group(1), int(match.group(2))
            try:
                result = sock.connect_ex((url, port))
            except:
                traceback.print_exc()
                return False
            finally:
                sock.close()
            return result == 0
        else:
            return False

    @staticmethod
    def get_available_ctp_server(addresses):
        """
            从配置的服务器列表获取可用服务器
        Args:
            addresses: 服务器列表

        Returns:
            可用服务地址
        """
        if isinstance(addresses, str):
            addresses = [addresses]
        for address in addresses:
            if OrderTrader.check_ctp_server(address):
                return address
        return None

    def update_market_data(self, data):
        with self.market_lock:
            instrument = data['InstrumentID']
            self.market_data[instrument] = data
            for event in self.market_event[instrument]:
                event.set()
            del self.market_event[instrument]

    def update_market_event(self, instrument, event, action=True):
        with self.market_lock:
            if action:
                self.market_event[instrument].add(event)
            else:
                self.market_event[instrument].discard(event)

    def verify_market_data(self, instrument):
        msg = self.market_data.get(instrument, None)
        if msg is not None:
            current_time = datetime.now().time()
            update_time = [int(v) for v in msg['UpdateTime'].decode('gbk').split(':')]
            update_time = time(update_time[0], update_time[1], update_time[2], msg['UpdateMillisec'] * 1000)
            market_time = self.trader.market_time[self.trader.get_instrument_info(instrument)['ExchangeID']]
            lhs = datetime.combine(date.min, current_time) - datetime.combine(date.min, self.trader.login_time)
            rhs = datetime.combine(date.min, update_time) - datetime.combine(date.min, market_time)
            seconds = (lhs - rhs).total_seconds()
            if seconds < -82800:
                print('行情数据跨日: %s %s %s' % (instrument, update_time, current_time))
                seconds += 86400
            elif seconds > 82800:
                print('行情数据跨日: %s %s %s' % (instrument, update_time, current_time))
                seconds -= 86400
            if abs(seconds) <= 20:
                return msg
            else:
                print('合约数据更新时间异常: %s %s %s' % (instrument, update_time, current_time))
                return None
        else:
            print('无合约最新数据: %s' % instrument)
            return None

    def get_current_market_data(self, instrument):
        data = self.verify_market_data(instrument)
        if data is None:
            event = Event()
            event.clear()
            self.update_market_event(instrument, event, True)
            if instrument not in self.market_subs:
                sub_id = self.market.subscribe_md(instrument, self.update_market_data)
                with self.market_lock:
                    self.market_subs[instrument] = sub_id
            if event.wait(20):
                data = self.verify_market_data(instrument)
            else:
                print('获取最新行情数据超时: %s!' % instrument)
                data = None
            self.update_market_event(instrument, event, False)
        return data

    @Trading.operator_async
    def insert_order(self, order):
        """
        下单接口，一次只能下一个指令单，默认在当前线程运行，交易完成前不返回
        调用时可手动添加callback参数新开一个线程运行，从而不阻碍主线程，交易完成后会调用callback函数，当callback为None时，新开线程运行，但结衣结束时不调用回调函数
        Args:
            order: Order类型

        Returns:

        """
        procedures = []
        trading_volume = order.volume
        if isinstance(order.price, str) and order.timeout > 0:
            for _ in range(order.repeat):
                data = self.get_current_market_data(order.instrument)
                if data is not None:
                    if order.price == 'MidPrice':
                        ask1, bid1 = data['AskPrice1'], data['BidPrice1']
                        if ask1 is None or ask1 <= 0 or abs(ask1) >= HUGE_VAL or bid1 is None or bid1 <= 0 or abs(bid1) >= HUGE_VAL:
                            price = None
                        else:
                            price = (ask1 + bid1) / 2
                    else:
                        price = data[order.price]
                else:
                    price = None
                if price is None or price <= 0 or abs(price) >= HUGE_VAL:
                    print('合约价格信息为空: %s %s' % (order.instrument, order.price))
                    return procedures
                p = self.trader.insert_time_limit_order(order.instrument, order.offset_flag, order.direction, trading_volume, price, order.hedge_flag, True, order.timeout)
                if p is None:
                    print('交易失败: %s' % order)
                    return procedures
                procedures.append(p)
                assert p.status in (OrderProcedure.ORDER_CANCEL, OrderProcedure.ORDER_PART_TRADED, OrderProcedure.ORDER_ALL_TRADED)
                trading_volume -= p.traded_volume
                if trading_volume == 0:
                    print('交易完成: %s' % order)
                    break
        else:
            p = self.trader.insert_time_limit_order(order.instrument, order.offset_flag, order.direction, order.volume, order.price, order.hedge_flag, True, order.timeout)
            if p is None:
                print('交易失败: %s' % order)
                return procedures
            procedures.append(p)
            assert p.status in (OrderProcedure.ORDER_CANCEL, OrderProcedure.ORDER_PART_TRADED, OrderProcedure.ORDER_ALL_TRADED)
        return procedures

    def insert_orders(self, orders, callback=None):
        """
            同时下多笔订单，所有订单完成前不返回
        Args:
            orders: Order列表
            callback: 每笔交易完成后均会调用此方法

        Returns:

        """
        operators = []
        for order in orders:
            operators.append(self.insert_order(order, callback=callback))
        return [op.get() for op in operators]


if __name__ == '__main__':
    pass
