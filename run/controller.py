__author__='Shuang'

from itertools import groupby

from data_handler.data_handler import TotalDataHandler

mode = 'replay' # (paper, replay), paper trade or replay (backtest)

strategy_batch = [
    {
        'type': 'cta',
        'contract': 'Au(T+D)',
        'params': (1,2,3),
        'tick_size': 0.1,
        'kl_duration': '1m'
    }
]

contract_list = sorted([
    (b['contract'], b['kl_duration'], b['tick_size'])
    for b in strategy_batch if 'contract' in b
])

contract_counts = [
    len(list(group)) for k, group in groupby(sorted(contract_list))
]

contract_set = sorted(list(set(contract_list)))

contract_counts_dict = dict(zip(
    [t[0] for t in contract_set], contract_counts
))

def data_pub():
    contracts = list(set([b[0] for b in contract_list]))
    data_handler = TotalDataHandler()
    for c in contracts:
        data_handler.add_instrument(c, duplicate=contract_counts_dict[c],
                                    kline_dur_specifiers=('1m',))
    if mode == 'replay':
        data_handler.replay_data(attach_end_flag=True)
    elif mode == 'paper':
        data_handler.distribute_data()


if __name__ == '__main__':
    data_pub()