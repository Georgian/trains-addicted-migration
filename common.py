import os.path
import re

filename_regex = re.compile('[^0-9a-zA-Z]+')


def sanitize(string):
    return '-'.join(filename_regex.sub(' ', string).strip().split())


def sanitize_pic_name(pic_name):
    splitext = os.path.splitext(pic_name)
    pic_name_extensionless = sanitize(splitext[0])
    return pic_name_extensionless + splitext[1]


def sanitize_url_key(string):
    return sanitize(string).lower()


def decode(obj):
    return obj.decode() if isinstance(obj, bytes) else obj


def decode_dict(d):
    return {decode(k): decode(v) for k, v in d.items()}


def batch(iterable, n=1):
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx:min(ndx + n, length)]
