# -*- coding: utf-8 -*-
# @Time    : 2018/5/4 17:09
# @Author  : xujunyong
from source.ctp import Order, OrderTrader, PyCTP, Trading


def order_callback(ps):
    for p in ps:
        instrument, direction, vol, traded_vol = p.field['InstrumentID'].decode('gbk'), Trading.direction_map[p.field['Direction']], p.field['VolumeTotalOriginal'], p.traded_volume
        print('合约: %s 方向: %s 挂单手数: %d 成交手数: %d!' % (instrument, direction, vol, traded_vol))


if __name__ == '__main__':
    order = Order(b'IF1809', PyCTP.THOST_FTDC_D_Buy, PyCTP.THOST_FTDC_OF_Open, PyCTP.THOST_FTDC_HF_Speculation, 1, 'MidPrice', timeout=4, repeat=3)
    ot = OrderTrader('ctp.json', 'simnow2')
    # 下单
    print('单笔下单')
    r = ot.insert_order(order)
    print('单笔下单返回')

    print('单笔后台下单')
    # 下单并后台运行
    r = ot.insert_order(order, callback=order_callback)
    print('单笔后台下单直接返回')
    # 等待后台进程返回结果，不加此行会不会阻塞当前进程
    p = r.get()
    print('单笔后台下单结果返回')

    order1 = Order(b'SM809', PyCTP.THOST_FTDC_D_Buy, PyCTP.THOST_FTDC_OF_Open, PyCTP.THOST_FTDC_HF_Speculation, 2, 'AskPrice1', timeout=5, repeat=3)
    order2 = Order(b'SF809', PyCTP.THOST_FTDC_D_Buy, PyCTP.THOST_FTDC_OF_Open, PyCTP.THOST_FTDC_HF_Speculation, 2, 'BidPrice1', timeout=5, repeat=3)
    order3 = Order(b'v1809', PyCTP.THOST_FTDC_D_Buy, PyCTP.THOST_FTDC_OF_Open, PyCTP.THOST_FTDC_HF_Speculation, 2, 'MidPrice', timeout=5, repeat=3)
    # 多笔同时下单
    print('多笔同时下单')
    r = ot.insert_orders([order1, order2, order3], callback=order_callback)
    print('多笔同时下单返回')
