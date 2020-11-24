def format_pic_name(pic_name):
    return '-'.join(pic_name.strip().split())


def format_url_key(string):
    return '-'.join(string.strip().lower().split())


def decode(obj):
    return obj.decode() if isinstance(obj, bytes) else obj


def decode_dict(d):
    return {decode(k): decode(v) for k, v in d.items()}


def batch(iterable, n=1):
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx:min(ndx + n, length)]
