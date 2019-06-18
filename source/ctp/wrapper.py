# -*- coding: utf-8 -*-
# @Time    : 2018/2/6 15:22
# @Author  : xujunyong
import re
import time
import datetime
import PyCTP
import pandas as pd
from functools import wraps
from queue import Queue
from threading import Event, Lock
from multiprocessing.pool import ThreadPool
from collections import OrderedDict, defaultdict


HUGE_VAL = 10 ** 300


class Procedure:
    def __init__(self, request_id, field, timeout=None):
        self.request_id, self.field, self.timeout = request_id, field, timeout
        self.code, self.msg, self.result = None, None, None
        self.event = Event()
        self.actions = []

    def lock(self):
        self.event.clear()

    def wait_lock(self):
        return self.event.wait(self.timeout)

    def unlock(self):
        self.event.set()
        self.unlock_action()

    def unlock_action(self):
        for action in self.actions:
            action.unlock()


class OrderProcedure(Procedure):

    ORDER_INIT = 0              # 本地生成订单           初始状态
    ORDER_SUBMITTING = 1        # 提交订单到CTP          中间状态
    ORDER_SUBMITTED = 2         # 已提交到CTP            中间状态
    ORDER_QUEUEING = 3          # 交易所挂单中           中间状态
    ORDER_CANCEL = 4            # 挂单取消               最终状态
    ORDER_PART_TRADED = 5       # 部分成交               最终状态
    ORDER_ALL_TRADED = 6        # 全部成交               最终状态

    def __init__(self, request_id, front, session, order_ref, instrument, field, timeout=None):
        super().__init__(request_id, field, timeout)
        self.front, self.session, self.order_ref, self.instrument = front, session, order_ref, instrument
        self.rtns, self.queue = [], Queue()
        self.status = self.ORDER_INIT
        self.traded_volume = 0

    def update(self, data, msg_type):
        self.rtns.append((data, msg_type))
        if msg_type == 'trade':
            self.traded_volume += data['Volume']
        self.queue.put((data, msg_type))


class ProcedureManager:

    CONNECT_REQUEST_ID = -1

    def __init__(self):
        self._request_id = 0
        self.procedures = OrderedDict()
        self.order_ref_id, self.order_sys_id = OrderedDict(), OrderedDict()
        self.request_id_lock, self.procedure_lock = Lock(), Lock()

    def inc_request_id(self):
        with self.request_id_lock:
            self._request_id += 1
            request_id = self._request_id
        return request_id

    def create_procedure(self, method='normal', front=None, session=None, order_ref=None, instrument=None, target_request_id=None, field=None, timeout=None):
        with self.procedure_lock:
            if method == 'connect':
                request_id = self.CONNECT_REQUEST_ID
            else:
                request_id = self.inc_request_id()
            if method == 'order':
                procedure = OrderProcedure(request_id, front, session, order_ref, instrument, field, timeout)
            else:
                procedure = Procedure(request_id, field, timeout)
            if method == 'action':
                target_procedure = self.get_procedure(request_id=target_request_id)
                target_procedure.actions.append(procedure)
            self.procedures[request_id] = procedure
        return procedure

    def update_order_ref_id(self, request_id, front=None, session=None, order_ref=None, exchange=None, sys_id=None):
        if front is not None:
            self.order_ref_id[(front, session, order_ref)] = request_id
        if exchange is not None:
            self.order_sys_id[(exchange, sys_id)] = request_id

    def get_procedure(self, request_id=None, front=None, session=None, order_ref=None, exchange=None, sys_id=None):
        if request_id is not None:
            return self.procedures.get(request_id)
        elif front is not None:
            return self.procedures.get(self.order_ref_id.get((front, session, order_ref)))
        else:
            return self.procedures.get(self.order_sys_id.get((exchange, sys_id)))


class Trading(PyCTP.CThostFtdcTraderApi):

    market_map = {'INE': '能源中心', 'SHFE': '上期所', 'CFFEX': '中金所', 'DCE': '大商所', 'CZCE': '郑商所'}
    direction_map = {PyCTP.THOST_FTDC_D_Buy: '买', PyCTP.THOST_FTDC_D_Sell: '卖'}
    offset_flag_map = {PyCTP.THOST_FTDC_OF_Open: '开',
                       PyCTP.THOST_FTDC_OF_Close: '平',
                       PyCTP.THOST_FTDC_OF_ForceClose: '强平',
                       PyCTP.THOST_FTDC_OF_CloseToday: '平今',
                       PyCTP.THOST_FTDC_OF_CloseYesterday: '平昨',
                       PyCTP.THOST_FTDC_OF_ForceOff: '强减',
                       PyCTP.THOST_FTDC_OF_LocalForceClose: '本地强平'}
    hedge_flag_map = {PyCTP.THOST_FTDC_HF_Speculation: '投',
                      PyCTP.THOST_FTDC_HF_Arbitrage: '套',
                      PyCTP.THOST_FTDC_HF_Hedge: '保'}
    market_map_revert = dict((v, k) for k, v in market_map.items())
    direction_map_revert = dict((v, k) for k, v in direction_map.items())
    offset_flag_map_revert = dict((v, k) for k, v in offset_flag_map.items())
    hedge_flag_map_revert = dict((v, k) for k, v in hedge_flag_map.items())

    def __new__(cls, flow_cache_path=b'_tmp_t_'):
        return cls.CreateFtdcTraderApi(flow_cache_path)

    def __init__(self, flow_cache_path=b'_tmp_t_'):
        self.TIMEOUT = 30
        self.is_connected = False
        self.flow_cache_path = flow_cache_path
        self.pm = ProcedureManager()
        self.broker, self.user, self.investor, self.session, self.front = None, None, None, None, None
        self.system_name, self.trading_day, self.login_time, self.market_time = None, None, None, {}
        self.max_order_ref, self.order_ref, self.order_action_ref = None, None, 1
        self.order_ref_lock, self.order_action_ref_lock = Lock(), Lock()
        self.instrument_info = None

    @property
    def inc_order_ref(self):
        with self.order_ref_lock:
            order_ref = ('%012d' % self.order_ref).encode('gbk')
            self.order_ref += 1
        return order_ref

    @property
    def inc_order_action_ref(self):
        with self.order_action_ref_lock:
            order_action_ref = self.order_action_ref
            self.order_action_ref += 1
        return order_action_ref

    def operator_flow(self, func, procedure, *args, **kwargs):
        if procedure.timeout is None or procedure.timeout > 0:
            procedure.lock()
        for _ in range(30):
            ret = func(*args, **kwargs)
            if ret is None:
                print('%s: 指令发送成功, 无返回值 %s %s' % (func.__name__, args, kwargs))
                break
            elif ret == 0:
                print('%s: 指令发送成功 %s %s' % (func.__name__, args, kwargs))
                break
            elif ret == -1:
                print('%s: 网络连接失败, 1s后重试 %s %s' % (func.__name__, args, kwargs))
                time.sleep(1)
            elif ret == -2:
                print('%s: 未处理请求超过许可数, 1s后重试 %s %s' % (func.__name__, args, kwargs))
                time.sleep(1)
            elif ret == -3:
                print('%s: 每秒发送请求数超过许可数, 1s后重试 %s %s' % (func.__name__, args, kwargs))
                time.sleep(1)
            else:
                print('%s: 未识别的错误代码: %d, 1s后重试 %s %s' % (func.__name__, ret, args, kwargs))
                time.sleep(1)
        else:
            print('%s: 指令发送失败 %s %s' % (func.__name__, args, kwargs))
            raise Exception('%s: 指令发送失败 %s %s' % (func.__name__, args, kwargs))

        if procedure.timeout is None or procedure.timeout > 0:
            if procedure.wait_lock():
                print('%s: 指令返回: %s %s %s %s ' % (func.__name__, procedure.code, procedure.msg, args, kwargs))
            else:
                print('%s: 指令返回超时 %s %s' % (func.__name__, args, kwargs))
        else:
            print('%s: 指令直接返回 %s %s' % (func.__name__, args, kwargs))
        return procedure

    def operator_flow_callback(self, data, RspInfo, RequestID, IsLast):
        procedure = self.pm.get_procedure(RequestID)
        if RspInfo is not None:
            procedure.code, procedure.msg = RspInfo['ErrorID'], RspInfo['ErrorMsg'].decode('gbk')
        else:
            procedure.code, procedure.msg = 0, None
        if procedure.code == 0:
            if procedure.result is None:
                procedure.result = data if IsLast else [data]
            else:
                procedure.result.append(data)
            if IsLast:
                procedure.unlock()
        else:
            procedure.unlock()

    def operator_flow_context(broker=False, user=False, investor=False):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                if not func.__name__.startswith('Req'):
                    raise Exception('%s: 无匹配的响应函数!')
                callback = 'OnRsp%s' % func.__name__[3:]
                if not hasattr(self.__class__, callback):
                    setattr(self.__class__, callback, self.__class__.operator_flow_callback)
                field = kwargs.copy()
                field.update(zip(func.__code__.co_varnames[1:func.__code__.co_argcount], args))
                if broker:
                    field['BrokerID'] = self.broker
                if user:
                    field['UserID'] = self.user
                if investor:
                    field['InvestorID'] = self.investor
                op = getattr(super(self.__class__, self), func.__name__)
                procedure = self.pm.create_procedure(field=field, timeout=self.TIMEOUT)
                return self.operator_flow(op, procedure, field, procedure.request_id)
            return wrapper
        return decorator

    def operator_async(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if 'callback' in kwargs:
                callback = kwargs.pop('callback')
                tp = ThreadPool(1)
                return tp.apply_async(func, args, kwargs, callback=callback)
            else:
                return func(*args, **kwargs)
        return wrapper

    def Connect(self, front_address, resume_type=PyCTP.THOST_TERT_RESTART):
        self.RegisterSpi(self)
        self.SubscribePrivateTopic(resume_type)
        self.SubscribePublicTopic(resume_type)
        self.RegisterFront(front_address)
        field = dict(FrontAddr=front_address, ResumeType=resume_type)
        procedure = self.pm.create_procedure(method='connect', field=field, timeout=self.TIMEOUT)
        self.operator_flow(self.Init, procedure)
        return procedure

    def OnFrontConnected(self):
        self.operator_flow_callback(None, None, self.pm.get_procedure(self.pm.CONNECT_REQUEST_ID).request_id, True)
        self.is_connected = True

    def OnFrontDisconnected(self, Reason):
        print('CTP断开连接: %d!' % Reason)
        self.is_connected = False

    def Login(self, BrokerID, UserID, Password):
        procedure = self.ReqUserLogin(BrokerID, UserID, Password)
        self.broker = procedure.result['BrokerID']
        self.user = procedure.result['UserID']
        self.investor = self.user
        self.system_name = procedure.result['SystemName']
        self.trading_day = datetime.datetime.strptime(procedure.result['TradingDay'].decode('gbk'), '%Y%m%d').date()
        self.session = procedure.result['SessionID']
        self.max_order_ref = procedure.result['MaxOrderRef']
        self.order_ref = int(self.max_order_ref.decode('gbk'))
        self.front = procedure.result['FrontID']
        pattern = re.compile('^(\d+):(\d+):(\d+)$')
        match = pattern.match(procedure.result['LoginTime'].decode('gbk'))
        if match is None:
            self.login_time = None
        else:
            self.login_time = datetime.time(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        market_time = {b'INE': procedure.result['INETime'], b'DCE': procedure.result['DCETime'],
                       b'CFFEX': procedure.result['FFEXTime'], b'CZCE': procedure.result['CZCETime'],
                       b'SHFE': procedure.result['SHFETime']}
        for k, v in market_time.items():
            match = pattern.match(v.decode('gbk'))
            if match is None:
                self.market_time[k] = None
            else:
                self.market_time[k] = datetime.time(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        self.update_instrument_info()
        return procedure

    def Logout(self):
        if self.is_connected:
            return self.ReqUserLogout()

    def OnRspError(self, RspInfo, RequestID, IsLast):
        self.operator_flow_callback(None, RspInfo, RequestID, IsLast)

    def OnRtnInstrumentStatus(self, *args, **kwargs):
        pass

    @operator_flow_context()
    def ReqAuthenticate(self, BrokerID, UserID, AppID, AuthCode):
        pass

    @operator_flow_context()
    def ReqUserLogin(self, BrokerID, UserID, Password):
        pass

    @operator_flow_context(broker=True, user=True)
    def ReqUserLogout(self):
        pass

    @operator_flow_context(broker=True, investor=True)
    def ReqQrySettlementInfo(self, TradingDay=b''):
        pass

    @operator_flow_context(broker=True, investor=True)
    def ReqSettlementInfoConfirm(self):
        pass

    @operator_flow_context(broker=True, investor=True)
    def ReqQrySettlementInfoConfirm(self):
        pass

    @operator_flow_context(broker=True, investor=True)
    def ReqQryOrder(self, InstrumentID=b'', ExchangeID=b'', OrderSysID=b''):
        pass

    @operator_flow_context(broker=True, investor=True)
    def ReqQryTrade(self, InstrumentID=b'', ExchangeID=b'', TradeID=b'', TradeTimeStart=b'', TradeTimeEnd=b''):
        pass

    @operator_flow_context(broker=True, investor=True)
    def ReqQryInvestor(self):
        pass

    @operator_flow_context(broker=True, investor=True)
    def ReqQryInvestorPosition(self, InstrumentID=b''):
        pass

    @operator_flow_context(broker=True, investor=True)
    def ReqQryInvestorPositionDetail(self, InstrumentID=b''):
        pass

    @operator_flow_context(broker=True, investor=True)
    def ReqQryInvestorPositionCombineDetail(self, CombInstrumentID=b''):
        pass

    @operator_flow_context()
    def ReqQryInstrument(self, ExchangeInstID, InstrumentID, ExchangeID, ProductID):
        pass

    @operator_flow_context()
    def ReqQryProduct(self, ProductID, ProductClass):
        pass

    @operator_flow_context()
    def ReqQryDepthMarketData(self, InstrumentID):
        pass

    @operator_flow_context(broker=True, investor=True)
    def ReqQryInstrumentCommissionRate(self, InstrumentID):
        pass

    @operator_async
    def ReqOrderInsert(self, instrument, action, direction, volume, price, hedge_flag=PyCTP.THOST_FTDC_HF_Speculation, timeout='default'):
        order_ref = self.inc_order_ref
        field = dict(BrokerID=self.broker,
                     UserID=self.user,
                     InvestorID=self.investor,
                     InstrumentID=instrument,
                     OrderRef=order_ref,
                     Direction=direction,                                  # 买卖方向
                     CombOffsetFlag=action,                                # 组合开平标志
                     LimitPrice=price,                                     # 价格
                     VolumeTotalOriginal=volume,                           # 数量
                     MinVolume=1,                                          # 最小成交量
                     OrderPriceType=PyCTP.THOST_FTDC_OPT_LimitPrice,       # 报单价格条件:限价
                     CombHedgeFlag=hedge_flag,                             # 组合投机套保标志:默认投机
                     TimeCondition=PyCTP.THOST_FTDC_TC_GFD,                # 有效期类型:当日有效
                     VolumeCondition=PyCTP.THOST_FTDC_VC_AV,               # 成交量类型:任意数量
                     ContingentCondition=PyCTP.THOST_FTDC_CC_Immediately,  # 触发条件:立即
                     ForceCloseReason=PyCTP.THOST_FTDC_FCC_NotForceClose)  # 强平原因:非强平
        if timeout == 'default':
            timeout = self.TIMEOUT
        procedure = self.pm.create_procedure(method='order', front=self.front, session=self.session, order_ref=order_ref, instrument=instrument, field=field, timeout=timeout)
        self.pm.update_order_ref_id(procedure.request_id, front=self.front, session=self.session, order_ref=order_ref)
        procedure.status = OrderProcedure.ORDER_SUBMITTING
        return self.operator_flow(super().ReqOrderInsert, procedure, field, procedure.request_id)

    def OnRspOrderInsert(self, InputOrder, RspInfo, RequestID, IsLast):
        procedure = self.pm.get_procedure(RequestID)
        procedure.code, procedure.msg = RspInfo['ErrorID'], RspInfo['ErrorMsg'].decode('gbk')
        procedure.status = OrderProcedure.ORDER_CANCEL
        procedure.update(InputOrder, 'order')
        print('OnRspOrderInsert: CTP报单录入失败: %d %s %s!' % (procedure.code, procedure.msg, InputOrder))
        procedure.unlock()

    def OnErrRtnOrderInsert(self, InputOrder, RspInfo):
        code, msg = RspInfo['ErrorID'], RspInfo['ErrorMsg'].decode('gbk')
        print('OnErrRtnOrderInsert: %d %s %s!' % (code, msg, InputOrder))

    def OnRtnOrder(self, Trade):
        exchange, sys_id = Trade['ExchangeID'], Trade['OrderSysID']
        front, session, order_ref = Trade['FrontID'], Trade['SessionID'], Trade['OrderRef']
        procedure = self.pm.get_procedure(front=front, session=session, order_ref=order_ref)
        if procedure is None:
            print('OnRtnOrder: 其他前置/会话报单回报信息:%s %s %s' % (front, session, Trade))
            field = dict(BrokerID=self.broker, UserID=self.user, InvestorID=self.investor, InstrumentID=Trade['InstrumentID'],
                         OrderRef=Trade['OrderRef'], Direction=Trade['Direction'], CombOffsetFlag=Trade['CombOffsetFlag'],
                         LimitPrice=Trade['LimitPrice'], VolumeTotalOriginal=Trade['VolumeTotalOriginal'], MinVolume=Trade['MinVolume'],
                         OrderPriceType=Trade['OrderPriceType'], CombHedgeFlag=Trade['CombHedgeFlag'], TimeCondition=Trade['TimeCondition'],
                         VolumeCondition=Trade['VolumeCondition'], ContingentCondition=Trade['ContingentCondition'],
                         ForceCloseReason=Trade['ForceCloseReason'])
            procedure = self.pm.create_procedure(method='order', front=front, session=session, order_ref=order_ref, instrument=Trade['InstrumentID'], field=field, timeout=self.TIMEOUT)
        else:
            print('OnRtnOrder: 报单回报信息:%s %s %s' % (front, session, Trade))
        self.pm.update_order_ref_id(procedure.request_id, front, session, order_ref, exchange, sys_id)
        unlock, unlock_action = False, False
        submit_status, status = Trade['OrderSubmitStatus'], Trade['OrderStatus']
        if submit_status == PyCTP.THOST_FTDC_OSS_InsertSubmitted:
            print('OnRtnOrder: 报单 %s %s %s' % (submit_status, status, Trade))
            procedure.status = OrderProcedure.ORDER_SUBMITTED
        elif submit_status in (PyCTP.THOST_FTDC_OSS_CancelSubmitted, PyCTP.THOST_FTDC_OSS_ModifySubmitted):
            print('OnRtnOrder: 撤单/改单已提交 %s %s %s' % (submit_status, status, Trade))
        elif submit_status == PyCTP.THOST_FTDC_OSS_Accepted:
            if status == PyCTP.THOST_FTDC_OST_AllTraded:
                print('OnRtnOrder: 全部成交 %s %s %s' % (submit_status, status, Trade))
            elif status == PyCTP.THOST_FTDC_OST_PartTradedNotQueueing:
                print('OnRtnOrder: 挂单部分成交 %s %s %s' % (submit_status, status, Trade))
                procedure.status = OrderProcedure.ORDER_PART_TRADED
                unlock = True
            elif status in (PyCTP.THOST_FTDC_OST_NoTradeNotQueueing, PyCTP.THOST_FTDC_OST_Canceled):
                print('OnRtnOrder: 挂单失效 %s %s %s' % (submit_status, status, Trade))
                procedure.status = OrderProcedure.ORDER_CANCEL
                unlock = True
            elif status in (PyCTP.THOST_FTDC_OST_PartTradedQueueing, PyCTP.THOST_FTDC_OST_NoTradeQueueing, PyCTP.THOST_FTDC_OST_NotTouched, PyCTP.THOST_FTDC_OST_Touched, PyCTP.THOST_FTDC_OST_Unknown):
                print('OnRtnOrder: 挂单待成交 %s %s %s' % (submit_status, status, Trade))
                procedure.status = OrderProcedure.ORDER_QUEUEING
            else:
                print('OnRtnOrder: 无法处理的状态类型: %s %s %s' % (submit_status, status, Trade))
                raise Exception('OnRtnOrder: 无法处理的状态类型: %s %s %s' % (submit_status, status, Trade))
        elif submit_status == PyCTP.THOST_FTDC_OSS_InsertRejected:
            print('OnRtnOrder: 报单已被拒绝 %s %s %s' % (submit_status, status, Trade))
            procedure.status = OrderProcedure.ORDER_CANCEL
            unlock = True
        elif submit_status in (PyCTP.THOST_FTDC_OSS_ModifyRejected, PyCTP.THOST_FTDC_OSS_CancelRejected):
            print('OnRtnOrder: 撤单/修改已被拒绝 %s %s %s' % (submit_status, status, Trade))
            unlock_action = True
        else:
            print('OnRtnOrder: 无法处理类型: %s %s %s' % (submit_status, status, Trade))
            raise Exception('OnRtnOrder: 无法处理类型: %s %s %s' % (submit_status, status, Trade))
        procedure.update(Trade, 'order')
        if unlock:
            procedure.unlock()
        if unlock_action:
            procedure.unlock_action()

    def OnRtnTrade(self, Trade):
        procedure = self.pm.get_procedure(exchange=Trade['ExchangeID'], sys_id=Trade['OrderSysID'])
        if procedure.traded_volume + Trade['Volume'] == procedure.field['VolumeTotalOriginal']:
            procedure.status = OrderProcedure.ORDER_ALL_TRADED
            procedure.update(Trade, 'trade')
            procedure.unlock()
            print('OnRtnTrade: 全部成交:%s' % Trade)
        else:
            procedure.update(Trade, 'trade')
            print('OnRtnTrade: 部分成交:%s' % Trade)

    def ReqOrderAction(self, procedure):
        field = dict(FrontID=procedure.front, SessionID=procedure.session, OrderRef=procedure.order_ref, InstrumentID=procedure.instrument,
                     OrderActionRef=self.inc_order_action_ref, ActionFlag=PyCTP.THOST_FTDC_AF_Delete)
        p = self.pm.create_procedure(method='action', target_request_id=procedure.request_id, field=field, timeout=self.TIMEOUT)
        return self.operator_flow(super().ReqOrderAction, p, field, p.request_id)

    def OnRspOrderAction(self, OrderAction, RspInfo, RequestID, IsLast):
        procedure = self.pm.get_procedure(RequestID)
        procedure.code, procedure.msg, procedure.result = RspInfo['ErrorID'], RspInfo['ErrorMsg'].decode('gbk'), OrderAction
        print('OnRspOrderAction: CTP撤单录入失败: %d %s %s!' % (procedure.code, procedure.msg, OrderAction))
        procedure.unlock()

    def OnErrRtnOrderAction(self, OrderAction, RspInfo):
        code, msg = RspInfo['ErrorID'], RspInfo['ErrorMsg'].decode('gbk')
        print('OnErrRtnOrderAction: %d %s %s!' % (code, msg, OrderAction))
        target_procedure = self.pm.get_procedure(front=OrderAction['FrontID'], session=OrderAction['SessionID'], order_ref=OrderAction['OrderRef'])
        if target_procedure is not None:
            target_procedure.code, target_procedure.msg, target_procedure.result = code, msg, OrderAction
            target_procedure.unlock_action()

    def update_instrument_info(self):
        if self.instrument_info is None:
            for _ in range(5):
                p = self.ReqQryInstrument()
                if p.code == 0 and p.result is not None:
                    print('update_instrument_info: 查询合约信息成功')
                    msgs = p.result if isinstance(p.result, list) else [p.result]
                    self.instrument_info = dict((msg['InstrumentID'], msg) for msg in msgs)
                    break
                else:
                    print('update_instrument_info: 查询合约信息失败, 1s后重试')
                    time.sleep(1)
            else:
                print('update_instrument_info: 查询合约信息失败')

    def get_instrument_info(self, instrument):
        self.update_instrument_info()
        if self.instrument_info is not None:
            if instrument in self.instrument_info:
                return self.instrument_info[instrument]
            else:
                print('get_instrument_info: 无法查询到合约信息: %s' % instrument)
                return None
        else:
            print('get_instrument_info: 合约信息初始化失败 %s' % instrument)
            return None

    def get_instrument_multiplier(self, instrument):
        info = self.get_instrument_info(instrument)
        if info is not None:
            volume_multiple, underlying_multiple = info['VolumeMultiple'], info['UnderlyingMultiple']
            if volume_multiple <= 0 or volume_multiple >= HUGE_VAL:
                volume_multiple = 1.
            if underlying_multiple <= 0 or underlying_multiple >= HUGE_VAL:
                underlying_multiple = 1.
            return volume_multiple * underlying_multiple
        return None

    @operator_async
    def insert_time_limit_order(self, instrument, action, direction, volume, price, hedge_flag=PyCTP.THOST_FTDC_HF_Speculation, adjust_price=True, timeout=30):
        """规定时间内无论是否成交均撤单"""
        if adjust_price:
            tick = self.get_instrument_info(instrument)['PriceTick']
            adjusted_price = round(price / tick) * tick
            print('insert_time_limit_order: 调整合约价格: %f %f' % (price, adjusted_price))
        procedure = self.ReqOrderInsert(instrument, action, direction, volume, adjusted_price, hedge_flag, timeout=timeout)
        if procedure.status in (OrderProcedure.ORDER_SUBMITTING, OrderProcedure.ORDER_SUBMITTED, OrderProcedure.ORDER_QUEUEING):
            self.ReqOrderAction(procedure)
        return procedure

    @operator_async
    def insert_tick_order(self, instrument, action, direction, volume, tick='Middle', hedge_flag=PyCTP.THOST_FTDC_HF_Speculation, adjust_price=True, timeout=30):
        p = self.ReqQryDepthMarketData(instrument)
        if p.code != 0 or p.result is None:
            print('insert_tick_order: 无法获取tick数据: %s' % instrument)
            return None
        if isinstance(p.result, list):
            for r in p.result:
                if r['InstrumentID'] == instrument:
                    md = r
                    print('模糊查询，匹配合约代码: %s %s' % (instrument, md))
                    break
            else:
                print('模糊查询，未匹配合约代码: %s' % instrument)
                return None
        else:
            md = p.result
            print('严格查询: %s %s' % (instrument, md))
        if tick == 'Middle':
            ask_1, bid_1 = md['AskPrice1'], md['BidPrice1']
            if pd.notnull(ask_1) and 0 < ask_1 < HUGE_VAL and pd.notnull(bid_1) and 0 < bid_1 < HUGE_VAL:
                price = (ask_1 + bid_1) / 2
            else:
                print('insert_tick_order: tick数据异常: %s %s %s' % (instrument, ask_1, bid_1))
                return None
        else:
            if tick not in md:
                print('insert_tick_order: 不存在该档位: %s %s' % (instrument, tick))
                return None
            price = md[tick]
            if pd.isnull(price) or price <= 0 or price >= HUGE_VAL:
                print('insert_tick_order: tick数据异常: %s %s' % (instrument, price))
                return None
        return self.insert_time_limit_order(instrument, action, direction, volume, price, hedge_flag, adjust_price, timeout)

    @operator_async
    def insert_tick_order_repeat(self, instrument, action, direction, volume, tick='Middle', hedge_flag=PyCTP.THOST_FTDC_HF_Speculation, adjust_price=True, timeout=30, retry=5):
        assert timeout is not None and timeout > 0
        procedures = []
        trading_volume = volume
        for _ in range(retry):
            p = self.insert_tick_order(instrument, action, direction, trading_volume, tick, hedge_flag, adjust_price, timeout)
            if p is None:
                return None, None
            procedures.append(p)
            assert p.status in (OrderProcedure.ORDER_CANCEL, OrderProcedure.ORDER_PART_TRADED, OrderProcedure.ORDER_ALL_TRADED)
            trading_volume -= p.traded_volume
            if trading_volume == 0:
                break
        return procedures, trading_volume


class MarketData(PyCTP.CThostFtdcMdApi):


    def __new__(cls, flow_cache_path=b'_tmp_m_'):
        return cls.CreateFtdcMdApi(flow_cache_path)

    def __init__(self, flow_cache_path=b'_tmp_m_'):
        self.TIMEOUT = 30
        self.flow_cache_path = flow_cache_path
        self.broker, self.user, self.trading_day = None, None, None
        self.conn_lock, self.login_lock, self.logout_lock = Event(), Event(), Event()
        self.login_result, self.logout_result = None, None
        self.subscribes, self.subscribes_info,  = defaultdict(OrderedDict), {}
        self._subscribe_id, self._subscribe_id_lock = 1, Lock()
        self._request_id, self._request_id_lock = 1, Lock()

    def inc_subscribe_id(self):
        with self._subscribe_id_lock:
            subscribe_id = self._subscribe_id
            self._subscribe_id += 1
        return subscribe_id

    @property
    def inc_request_id(self):
        with self._request_id_lock:
            request_id = self._request_id
            self._request_id += 1
        return request_id

    def Connect(self, front_address):
        self.RegisterSpi(self)
        self.RegisterFront(front_address)
        self.conn_lock.clear()
        self.Init()
        if self.conn_lock.wait(self.TIMEOUT):
            print('连接行情服务器: %s' % front_address)
        else:
            print('连接行情服务器超时: %s' % front_address)

    def OnFrontConnected(self):
        self.conn_lock.set()

    def ReqUserLogin(self, BrokerID, UserID, Password):
        self.login_lock.clear()
        super().ReqUserLogin(dict(BrokerID=BrokerID, UserID=UserID, Password=Password), self.inc_request_id)
        if self.login_lock.wait(self.TIMEOUT):
            self.broker, self.user, self.trading_day = BrokerID, UserID, self.login_result[-1]['TradingDay']
            print('登陆行情服务器: %d %s %s' % self.login_result)

        else:
            print('登陆行情服务器超时')

    def OnRspUserLogin(self, UserLogin, RspInfo, RequestID, IsLast):
        if RspInfo is not None:
            code, msg = RspInfo['ErrorID'], RspInfo['ErrorMsg'].decode('gbk')
        else:
            code, msg = 0, None
        self.login_result = code, msg, UserLogin
        self.login_lock.set()

    def ReqUserLogout(self):
        """
            接口已被CTP弃用?
        """
        self.logout_lock.clear()
        super().ReqUserLogout(dict(BrokerID=self.broker, UserID=self.user), self.inc_request_id)
        if self.logout_lock.wait(self.TIMEOUT):
            print('登出行情服务器: %d %s %s' % self.logout_result)
        else:
            print('登出行情服务器超时')

    def OnRspUserLogout(self, UserLogout, RspInfo, RequestID, IsLast):
        if RspInfo is not None:
            code, msg = RspInfo['ErrorID'], RspInfo['ErrorMsg'].decode('gbk')
        else:
            code, msg = 0, None
        self.logout_result = code, msg, UserLogout
        self.logout_lock.set()

    def OnRspError(self, RspInfo, RequestID, IsLast):
        if RspInfo is not None:
            code, msg = RspInfo['ErrorID'], RspInfo['ErrorMsg'].decode('gbk')
        else:
            code, msg = 0, None
        print('OnRspError: %d %s' % (code, msg))

    def Login(self, BrokerID, UserID, Password):
        return self.ReqUserLogin(BrokerID, UserID, Password)

    def Logout(self):
        return self.ReqUserLogout()

    def subscribe_md(self, instrument, callback):
        subscribe = self.subscribes[instrument]
        subscribe_id = self.inc_subscribe_id()
        subscribe[subscribe_id] = callback
        self.subscribes_info[subscribe_id] = instrument
        if len(subscribe) == 1:
            self.SubscribeMarketData([instrument], 1)
        return subscribe_id

    def unsubscribe_md(self, subscribe_id):
        instrument = self.subscribes_info.pop(subscribe_id, None)
        if instrument is not None:
            subscribe = self.subscribes[instrument]
            del subscribe[subscribe_id]
            if len(subscribe) == 0:
                self.UnSubscribeMarketData([instrument], 1)

    def OnRspSubMarketData(self, SpecificInstrument, RspInfo, RequestID, bIsLast):
        if RspInfo is not None:
            code, msg = RspInfo['ErrorID'], RspInfo['ErrorMsg'].decode('gbk')
        else:
            code, msg = 0, None
        print('OnRspSubMarketData: %d %s %s' % (code, msg, SpecificInstrument))

    def OnRspUnSubMarketData(self, SpecificInstrument, RspInfo, RequestID, bIsLast):
        if RspInfo is not None:
            code, msg = RspInfo['ErrorID'], RspInfo['ErrorMsg'].decode('gbk')
        else:
            code, msg = 0, None
        print('OnRspUnSubMarketData: %d %s %s' % (code, msg, SpecificInstrument))

    def OnRtnDepthMarketData(self, DepthMarketData):
        msg = dict()
        for k, v in DepthMarketData.items():
            if isinstance(v, (int, float)) and (abs(v) > HUGE_VAL):
                msg[k] = None
            else:
                msg[k] = v
        for callback in self.subscribes[msg['InstrumentID']].values():
            callback(msg)


if __name__ == '__main__':
    t = Trading(b'_tmp_ctp_t_')
    t.Connect(b'tcp://180.168.146.187:10001')
    t.Login(b'9999', b'107674', b'_quark')
    t.ReqSettlementInfoConfirm()
    z = t.ReqQryDepthMarketData(b'MA806')
    t.insert_time_limit_order(b'MA806', PyCTP.THOST_FTDC_OF_Open, PyCTP.THOST_FTDC_D_Buy, 1, 2600, timeout=10)
    time.sleep(10)
    t.insert_tick_order(b'al1804', PyCTP.THOST_FTDC_OF_Open, PyCTP.THOST_FTDC_D_Sell, 1, tick='BidPrice1', timeout=30)
    # time.sleep(10)
    # t.insert_tick_order(b'MA804', PyCTP.THOST_FTDC_OF_Close, PyCTP.THOST_FTDC_D_Buy, 1, timeout=10)
    t.Logout()
