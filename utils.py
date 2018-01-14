__author__ = 'Shuang'

MAX_DIGITS = 9


def append_digits_suffix_for_redis_key(prefix, counter):
    """
    return a structured string as a key in redis db.
    prefix is the name of folder, # of digits is in
    Config.redis_key_max_digits
    :param prefix: string
    :param counter: string
    :return: string like dir:00000###,
    """
    return (
        prefix + ':' +
        (MAX_DIGITS - len(str(counter)))*'0' +
        str(counter)
    )

def create_equiv_classes(associative_list):
    """
    create equivalent class from associative list.
    :param associative_list:
    :return:
    """
    equiv_dict = dict()
    for keys, val in associative_list:
        for k in keys:
            equiv_dict[k] = val
    return equiv_dict

def map_to_md_dir(associative_list):
    """
    map from (instrument, API) associative list
    to {instrument -> md_directory} mapping.
    :param associative_list:
    :return:
    """
    equiv_dict = create_equiv_classes(associative_list)
    for k,v in equiv_dict.items():
        equiv_dict[k] = v + k
    return equiv_dict

def map_to_kl_dir(associative_list, dur_specifier):
    """
    :param associative_list:
    :param dur_specifier: string, duration specifier of the kline,
        - 'clock': 3 seconds
        - '1m'
        - '3m' ...
    :return:
    """
    equiv_dict = create_equiv_classes(associative_list)
    for k,v in equiv_dict.items():
        equiv_dict[k] = v + k + '.' + dur_specifier
    return equiv_dict
