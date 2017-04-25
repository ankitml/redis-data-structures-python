from collections import Iterable
import string

DECODER = lambda byte:byte.decode("utf-8")
CHARACTERS = string.ascii_letters + string.punctuation  + string.digits
IS_ITERABLE = lambda v:isinstance(v, Iterable)


class RedisDSBase(object):

    def __init__(self, connection, key):
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        self.key = key
        self.c = connection
