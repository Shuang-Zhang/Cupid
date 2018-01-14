__author__='Shuang'

def sort_md_kl_timestamp(md_keys_dict,kl_keys_dict):
    """

    :param md_keys_dict: dict, in the form of {instrument: [list of keys], ...}
    :param kl_keys_dict: dict,
        in the form of {instrument: {dur: [list of keys], ...}}
    :return:
    """
    all_keys = []
    # make (key, time) tuples list for md.
    for inst in md_keys_dict:
        keys_this_inst = [(k, int(k.decode('utf8').split(":")[1]))
                          for k in md_keys_dict[inst]]
        all_keys.extend(keys_this_inst)

    all_kl_keys = []
    # make (key, time) tuples list for kl.
    for inst in kl_keys_dict:
        for dur in kl_keys_dict[inst]:
            keys = [(k, int(k.decode('utf8').split(":")[1]))
                    for k in kl_keys_dict[inst][dur]]
            all_kl_keys.extend(keys)
    all_keys.extend(all_kl_keys)

    # sort by time
    sorted_keys = sorted(
        all_keys,
        key=lambda tup: tup[1]
    )

    sorted_keys = [k[0] for k in sorted_keys]
    return sorted_keys