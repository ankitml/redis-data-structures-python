from collections.abc import MutableSequence
import string
import random
from redis import ResponseError
import uuid

DECODER = lambda byte:byte.decode("utf-8")
CHARACTERS = string.ascii_letters + string.punctuation  + string.digits


class RedisDSBase(object):
    ...


class RedisList(MutableSequence, RedisDSBase):
    """
    Redis backed list, feels like python, behaves like python list
    Drop in replacement for python lists. Persists in redis.
    connection is redis-py connection

    >>> sample_list = RedisList(connection, 'users')
    >>> sample_list.append(44)
    >>> sample_list.extend([22,11,10])
    >>> 22 in sample_list 
    >>> True
    >>> print(sample_list)
    >>> [44, 22, 11, 10]
    # have an option of marking it non necessary, all commands will have retry logic or ignore
    # till what frequency version management is not needed. probably that is too high according to redis configuration if all functions idempodent
    """


    def __init__(self, connection, key=None):
        if key is None:
            self.key = "".join(random.choice(CHARACTERS) for x in range(random.randint(12, 16)))
        self.key = key
        self.c = connection

    def append(self, *values):
        self.c.rpush(self.key, *values)

    def extend(self, iterable):
        self.c.rpush(self.key, *list(iterable))

    def remove(self, v):
        response = self.c.lrem(self.key, 1, v)
        if response == 0:
            raise ValueError('RedisList.remove(x): x not in list')

    def __repr__(self):
        l =  [DECODER(i) for i in self.c.lrange(self.key, 0, -1)]
        return str(l)

    def __getitem__(self, index):
        val = self.c.lindex(self.key, index)
        if val:
            return DECODER(val)
        raise IndexError('RedisList index out of range')
        
    def __setitem__(self, i, v):
        try:
            self.c.lset(self.key, i, v)
        except ResponseError:
            raise IndexError('RedisList index out of range')

    def __delitem__(self, index):
        if index == 0:
            self.c.lpop(self.key)
        elif index == -1:
            self.c.rpop(self.key)
        else:
            uid = uuid.uuid1().int
            self[index] = uid
            self.remove(uid)


    def __len__(self):
        return self.c.llen(self.key)

    def insert(self, i, v):
        pass



