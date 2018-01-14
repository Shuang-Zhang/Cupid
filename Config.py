__author__='Shuang'

from datetime import datetime, timedelta

from utils import map_to_kl_dir, map_to_md_dir, create_equiv_classes

class CupidConfig(object):
    """
    This is the configuration object for Cupid Python-Analytic module.

    All the configurations are stored as a class attributes.
    Class __str__ and __repr__ methods are redefined to
    print out configurations.

    We simply do not need instances of CupidConfig,
    hence __init__ method is missing.
    """
    sql_host=''
    sql_port=0000
    sql_usr=''
    sql_pwd=''
    sql_historical_db=''

    redis_host_local = '127.0.0.1'
    redis_host_remote_1 = '10.88.26.26'
    redis_port = 6379
    redis_protect_db = 0
    hist_stream_db_index = 3
    Cupid_db_index = 1

    redis_md_dir = 'md:md_backtest'
    redis_temp_hist_stream_dir = 'temp_stream:hermes'

    redis_md_max_records = 1e9
    redis_key_max_digits = 9
    redis_md_end_flag = 'md_end'

    protect_md_sep_char = '|'

    class RealtimeInstrumentsList(object):
        api01 = [
            'Ag(T+D)',
            'Au99.5'
        ]

        api02 = [
            '300682.SZ',
            '600571.SH'
        ]
        all = api01+api02

    class RealtimeMdDirectory(object):
        #Data directory prefix of different md APIs. Distinguish them by md and kl.
        apimd01 = 'md.api01'
        apimd02 = 'md.api02'
        apikl01 = 'kl.api01'
        apikl02 = 'kl.api02'

    Realtime_associative_list_md = (
        (RealtimeInstrumentsList.api01, RealtimeMdDirectory.apimd01),
        (RealtimeInstrumentsList.api02, RealtimeMdDirectory.apimd02)
    )

    Realtime_associative_list_kl = (
        (RealtimeInstrumentsList.api01, RealtimeMdDirectory.apikl01),
        (RealtimeInstrumentsList.api02, RealtimeMdDirectory.apikl02)
    )

    # md directory mapping
    Realtime_md_mapping = map_to_md_dir(Realtime_associative_list_md)

    Realtime_kl_dur = [
        'clock',
        '1s', '3s', '5s', '10s', '15s', '30s',
        '1m', '3m', '5m', '10m', '15m', '30m',
        '1h', '3h'
    ]
    Realtime_kl_dur_to_seconds = {
        '1s': 1,
        '3s': 3,
        '5s': 5,
        '10s': 10,
        '15s': 15,
        '30s': 30,
        '1m': 60,
        '3m': 180,
        '5m': 300,
        '10m': 600,
        '15m': 900,
        '30m': 1800,
        '1h': 3600,
        '3h': 10800
    }

    # the inverse mapping
    Realtime_kl_seconds_to_dur = dict()
    for k, v in Realtime_kl_dur_to_seconds.items():
        Realtime_kl_seconds_to_dur[v] = k

    # kline directory mapping.
    Realtime_kl_mapping = dict()
    for dur in Realtime_kl_dur:
        Realtime_kl_mapping[dur] = map_to_kl_dir(Realtime_associative_list_kl, dur)

    class CupidMessageTypes(object):
        # value of tag field in athena db's dict data
        md = 'md'
        kl = 'kl'

    # The following section is to configure Hermes raw data stream
    # ---------------------------------------------------------------------
    class RealtimeTickFields(object):
        # headers of hermes raw tick data.

        Realtime_tick_headers = (
            'day',
            'systime',
            'subtime',
            'updatems',
            'exchangeid',
            'contract',
            'lastprice',
            'bid1',
            'bid2',
            'bid3',
            'bid4',
            'bid5',
            'bid6',
            'bid7',
            'bid8',
            'bid9',
            'bid10',
            'bidvol1',
            'bidvol2',
            'bidvol3',
            'bidvol4',
            'bidvol5',
            'bidvol6',
            'bidvol7',
            'bidvol8',
            'bidvol9',
            'bidvol10',
            'ask1',
            'ask2',
            'ask3',
            'ask4',
            'ask5',
            'ask6',
            'ask7',
            'ask8',
            'ask9',
            'ask10',
            'askvol1',
            'askvol2',
            'askvol3',
            'askvol4',
            'askvol5',
            'askvol6',
            'askvol7',
            'askvol8',
            'askvol9',
            'askvol10',
            'avgprx',
            'openprx',
            'highprx',
            'lowprx',
            'closeprx',
            'clearprx',
            'preclearprx',
            'precloseprx',
            'openinterest',
            'preopeninterest',
            'volume',
            'turnover',
            'key'
        )
        (
            day,
            ex_time,
            local_time,
            update_ms,
            exchange,
            contract,
            last_price,
            bid_1,
            bid_2,
            bid_3,
            bid_4,
            bid_5,
            bid_6,
            bid_7,
            bid_8,
            bid_9,
            bid_10,
            bid_vol_1,
            bid_vol_2,
            bid_vol_3,
            bid_vol_4,
            bid_vol_5,
            bid_vol_6,
            bid_vol_7,
            bid_vol_8,
            bid_vol_9,
            bid_vol_10,
            ask_1,
            ask_2,
            ask_3,
            ask_4,
            ask_5,
            ask_6,
            ask_7,
            ask_8,
            ask_9,
            ask_10,
            ask_vol_1,
            ask_vol_2,
            ask_vol_3,
            ask_vol_4,
            ask_vol_5,
            ask_vol_6,
            ask_vol_7,
            ask_vol_8,
            ask_vol_9,
            ask_vol_10,
            average_price,
            open_price,
            high_price,
            low_price,
            close_price,
            clear_price,
            pre_clear_price,
            pre_close_price,
            open_interest,
            pre_open_interest,
            volume,
            turnover,
            key
        ) = Realtime_tick_headers

        asks = (
            ask_1,
            ask_2,
            ask_3,
            ask_4,
            ask_5,
            ask_6,
            ask_7,
            ask_8,
            ask_9,
            ask_10,
        )

        bids = (
            bid_1,
            bid_2,
            bid_3,
            bid_4,
            bid_5,
            bid_6,
            bid_7,
            bid_8,
            bid_9,
            bid_10,
        )

        ask_vols = (
            ask_vol_1,
            ask_vol_2,
            ask_vol_3,
            ask_vol_4,
            ask_vol_5,
            ask_vol_6,
            ask_vol_7,
            ask_vol_8,
            ask_vol_9,
            ask_vol_10,
        )

        bid_vols = (
            bid_vol_1,
            bid_vol_2,
            bid_vol_3,
            bid_vol_4,
            bid_vol_5,
            bid_vol_6,
            bid_vol_7,
            bid_vol_8,
            bid_vol_9,
            bid_vol_10,
        )

        floats = asks + bids + (last_price, average_price, open_price,
                                high_price, low_price, close_price,
                                clear_price, pre_clear_price, pre_close_price,)

        integers = ask_vols + bid_vols + (volume, update_ms, turnover,
                                          open_interest, pre_open_interest,)

    class RealtimeKLineFields(object):
        # headers of hermes raw k-line data

        Realtime_kline_headers = (
            'day',
            'updatetime',
            'localtime',
            'exchangeid',
            'contract',
            'dur',
            'dur_specifier',
            'openprx',
            'highprx',
            'lowprx',
            'closeprx',
            'volume',
            'turnover',
            'openinterest',
            'avgprx',
            'precloseprx',
            'totalvolume',
            'totalturnover',
            'opentime',
            'hightime',
            'lowtime',
            'closetime',
            'key',
            'count'
        )
        (
            day,
            ex_time,
            local_time,
            exchange,
            contract,
            duration,
            duration_specifier,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            turnover,
            open_interest,
            average_price,
            pre_close_price,
            total_volume,
            total_turnover,
            open_time,
            high_time,
            low_time,
            close_time,
            key,
            count
        ) = Realtime_kline_headers

        ohlc = (
            open_price,
            high_price,
            low_price,
            close_price
        )

        ohlc_time = (
            open_time,
            high_time,
            low_time,
            close_time
        )

        floats = ohlc + (average_price, pre_close_price,)
        integers = (volume, turnover, total_volume, total_turnover,
                    open_interest, duration)
        times = (ex_time, local_time, open_time,
                 high_time, low_time, close_time, 'timeframe')

    class SQLKlineFields(object):
        # table headers in SQL
        kline_headers = (
            'RowId',
            'TradingDay',
            'ExUpdateTime',
            'LocalUpdateTime',
            'ExchangeID',
            'Category',
            'Symbol',
            'TimeFrame',
            'Open',
            'High',
            'Low',
            'Close',
            'Volume',
            'Turnover',
            'OpenInterest',
            'Average',
            'TotalVolume',
            'TotalTurnover',
            'DayAveragePrice',
            'OpenTime',
            'HighTime',
            'LowTime',
            'CloseTime',
            'Rank',
            'UniqueIndex'
        )
        (id, day, ex_update_time, update_time, exchange,
         category, contract, duration,
         open_price, high_price, low_price, close_price,
         volume, turnover, open_interest, average_price,
         total_volume, total_turnover, daily_avg_price,
         open_time, high_time, low_time, close_time,
         rank, index) = kline_headers
        ohlc = (open_price, high_price, low_price, close_price)

    class SQLTickFields(object):
        # table headers in SQL
        tick_headers = (
            'RowId',
            'TradingDay',
            'ExUpdateTime',
            'LocalUpdateTime',
            'ExchangeID',
            'Category',
            'Symbol',
            'LastPrice',
            'BidPrice1',
            'BidPrice2',
            'BidPrice3',
            'BidPrice4',
            'BidPrice5',
            'BidPrice6',
            'BidPrice7',
            'BidPrice8',
            'BidPrice9',
            'BidPrice10',
            'BidVolume1',
            'BidVolume2',
            'BidVolume3',
            'BidVolume4',
            'BidVolume5',
            'BidVolume6',
            'BidVolume7',
            'BidVolume8',
            'BidVolume9',
            'BidVolume10',
            'AskPrice1',
            'AskPrice2',
            'AskPrice3',
            'AskPrice4',
            'AskPrice5',
            'AskPrice6',
            'AskPrice7',
            'AskPrice8',
            'AskPrice9',
            'AskPrice10',
            'AskVolume1',
            'AskVolume2',
            'AskVolume3',
            'AskVolume4',
            'AskVolume5',
            'AskVolume6',
            'AskVolume7',
            'AskVolume8',
            'AskVolume9',
            'AskVolume10',
            'AveragePrice',
            'HighestPrice',
            'LowestPrice',
            'PreClosePrice',
            'OpenInterest',
            'Volume',
            'Turnover',
            'Rank',
            'UniqueIndex'
        )
        (id, day, ex_update_time, local_update_time, exchange,
         category, contract, last_price,
         bid_1, bid_2, bid_3, bid_4, bid_5,
         bid_6, bid_7, bid_8, bid_9, bid_10,
         bid_vol_1, bid_vol_2, bid_vol_3, bid_vol_4, bid_vol_5,
         bid_vol_6, bid_vol_7, bid_vol_8, bid_vol_9, bid_vol_10,
         ask_1, ask_2, ask_3, ask_4, ask_5,
         ask_6, ask_7, ask_8, ask_9, ask_10,
         ask_vol_1, ask_vol_2, ask_vol_3, ask_vol_4, ask_vol_5,
         ask_vol_6, ask_vol_7, ask_vol_8, ask_vol_9, ask_vol_10,
         average_price, highest_price, lowest_price, preclose_price,
         open_int, volume, turnover, rank, index) = tick_headers

    class OrderFields(object):
        # proper headers of order data used in Athena (Python PEP-8)
        athena_order_headers = (
            'direction',
            'type',
            'subtype',
            'quantity',
            'price',
            'contract',
            'commission',
            'update_time',
            'bar_count'
        )
        (direction, type, subtype, quantity, price,
         contract, commission, update_time, bar_count) = athena_order_headers

    dt_format = '%Y-%m-%d %H:%M:%S'
    sql_storage_dt_format = '%Y-%m-%d %H:%M:%S.%f'

    sql_instruments_list = [
        'ag1607',
        'ag1608',
        'ag1609',
        'ag1610',
        'ag1611',
        'ag1612',
        'ag1701',
        'ag1702',
        'ag1703',
        'ag1704',
        'ag1705',
        'ag1706'
        'Au(T+D)',
        'mAu(T+D)'
        'Au(T+N1)',
        'Au(T+N2)',
        'au1612',
        'au1706',
        'Au99.95',
        'Au99.99'
    ]

    associative_list_exchange = (
        (RealtimeInstrumentsList.api01, 'exchange01'),
        (RealtimeInstrumentsList.api02, 'exchange02')
    )
    exchange_mapping = create_equiv_classes(associative_list_exchange)

def is_in_trade_time(dt,instrument):
    """

    :param dt:
    :param instrument:
    :return:
    """
    exchange = CupidConfig.exchange_mapping[instrument]
    day_start = datetime(dt.year, dt.month, dt.day)
    if exchange == 'exchange01':
        # uft
        if day_start <= dt \
                <= day_start + timedelta(hours=2, minutes=30):
            # 00:00 - 02:30
            return True
        elif day_start + timedelta(hours=9) <= dt \
                <= day_start + timedelta(hours=10, minutes=15):
            # 09:00 - 10:15
            return True
        elif day_start + timedelta(hours=10, minutes=30) <= dt \
                <= day_start + timedelta(hours=11, minutes=30):
            # 10:30 - 11:30
            return True
        elif day_start + timedelta(hours=13, minutes=29) <= dt \
                <= day_start + timedelta(hours=15):
            # 13:30 - 15:00
            return True
        elif day_start + timedelta(hours=21) <= dt \
                <= day_start + timedelta(hours=24):
            # 21:00 - 24:00
            return True
        else:
            return False

    elif exchange == 'exchange02':
        # ksd
        if day_start <= dt \
                <= day_start + timedelta(hours=2, minutes=30):
            # 00:00 - 02:30
            return True
        elif day_start + timedelta(hours=9) <= dt \
                <= day_start + timedelta(hours=11, minutes=30):
            # 09:00 - 11:30
            return True
        elif day_start + timedelta(hours=13, minutes=29) <= dt \
                <= day_start + timedelta(hours=15, minutes=30):
            # 13:30 - 15:30
            return True
        elif day_start + timedelta(hours=20) <= dt \
                <= day_start + timedelta(hours=24):
            # 20:00 - 24:00
            return True
        else:
            return False

    else:
        raise ValueError

