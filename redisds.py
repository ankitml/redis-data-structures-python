from collections import Iterable, abc
import string
import random
from redis import ResponseError
import uuid

DECODER = lambda byte:byte.decode("utf-8")
CHARACTERS = string.ascii_letters + string.punctuation  + string.digits
IS_ITERABLE = lambda v:isinstance(v, Iterable)


class RedisDSBase(object):

    def __init__(self, connection, key):
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        self.key = key
        self.c = connection


class RedisList(abc.MutableSequence, RedisDSBase):
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

    def append(self, *values):
        self.c.rpush(self.key, *values)

    def extend(self, iterable):
        self.c.rpush(self.key, *list(iterable))

    def remove(self, v):
        response = self.c.lrem(self.key, 1, v)
        if response == 0:
            raise ValueError('RedisList.remove(x): x not in list')

    def _clear(self):
        """
        remove the list from redis
        """
        self.c.delete(self.key)

    def __repr__(self):
        l =  [DECODER(i) for i in self.c.lrange(self.key, 0, -1)]
        return str(l)

    def sliced(self, slice_obj):
        start = slice_obj.start
        end = slice_obj.stop
        step = slice_obj.step
        if step is not None:
            raise NotImplementedError
        return [DECODER(i) for i in self.c.lrange(self.key, start, end)]

    def __getitem__(self, index):
        if isinstance(index, int):
            val = self.c.lindex(self.key, index)
            if not val:
                raise IndexError('RedisList index out of range')
            return DECODER(val)
        if isinstance(index, slice):
            val = self.sliced(index)
            return val

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

    def __iadd__(self, new_list):
        """
        self += new_list
        """
        raise_if_of_type(new_list, Iterable)
        if not isinstance(new_list, list):
            new_list = list(new_list)
        self.append(new_list)

    def __imul__(self, value):
        """
        Implements 
        self *= value
        interface
        """
        raise_if_of_type(value, int)
        if value < 1:
            self._clear()
        if value > 1:
            current = self
            for i in range(0, value - 1):
                self.extend(current)

    @classmethod
    def copy(cls):
        """
        copies the list into a new key. returns the copied redis list object
        """
        key = uuid.uuid1().int
        other = cls(self.c, key)
        other.extend(list(self))
        return other

    def count(self, value):
        """
        returns integer. Number of occurrences of given value
        """
        return sum(1 for i in self if i == value)

    def reverse(self):
        """
        """
        current = list(self)
        self._clear()
        current.reverse()
        self.extend(current)

    def sort(self, key=None, reverse=False):
        """
        *IN PLACE* sorting of redis list
        """
        pass

    def __add__(self, other):
        """
        self + other. Other can be a list or a redis list object
        """
        return list(self) + other

    def __contains__(self, val):
        """
        returns key in self (index for the value)
        """
        return val in list(self)

    def __eq__(self, val):
        """
        """
        return val == list(self)

    def __mul__(self, integer):
        """
        list * int
        """
        current = list(self)
        return current * integer

    def __ne__(self, val):
        """
        not equal to
        """
        return not self.__eq__(val)

    def insert(self, i, v):
        """
        redis_list.insert(index, element) -- inset this element befre index
        """
        full_list = list(self)
        try:
            post = full_list[i:]
            res = self.c.ltrim(self.key,0,i-1)
            self.append(v)
            self.extend(post)
        except:
            raise ValueError('some problem occured')


class RedisSet(abc.MutableSet, RedisDSBase):

    def __contains__(self, element):
        return self.c.sismember(self.key, element)

    def __iter__(self):
        s = self.c.smembers(self.key)
        return (DECODER(i) for i in s)
        
    def __len__(self):
        return self.c.scard(self.key)

    def add(self, element):
        self.c.sadd(self.key, element)

    def discard(self, element):
        self.c.srem(self.key, element)

    def copy(self):
        cls = type(self)
        key = uuid.uuid1().int
        other = cls(self.con, key)
        self.c.sunionstore(key, self.key)
        return other

    def clear(self):
        self.c.delete(self.key)

    def difference(self, *others):
        """
        Return a new set with elements in the set that are not in *other*.
        *others* is a list of redis set objects
        """
        cls = type(self)
        other_keys = [o.key for o in others if isinstance(o, cls)]
        key = uuid.uuid1().int
        other = cls(self.con, key)
        self.c.sdiffstore(key, [self.key, *other_keys])
        return other

    def difference_update(self, *others):
        cls = type(self)
        other_keys = [o.key for o in others if isinstance(o, cls)]
        if not isinstance(other, cls):
            raise TypeError('Other should be of type redis set')
        self.c.sdiffstore(self.key, other_keys)

    def intersection(self, *others):
        cls = type(self)
        other_keys = [o.key for o in others if isinstance(o, cls)]
        key = uuid.uuid1().int
        other = cls(self.con, key)
        self.c.sinterstore(key, [self.key, *other_keys])
        return other
        
    def intersection_update(self, *others):
        cls = type(self)
        other_keys = [o.key for o in others if isinstance(o, cls)]
        if not isinstance(other, cls):
            raise TypeError('Other should be of type redis set')
        self.c.sdiffstore(self.key, other_keys)

    def isdisjoint(self):
        

    def issubset(self):
        pass

    def issuperset(self):
        pass

    def pop(self):
        pass

    def remove(self):
        pass

    def symmetric_difference(self):
        pass

    def symmetric_difference_update(self):
        pass

    def union(self, *others):
        cls = type(self)
        other_keys = [o.key for o in others if isinstance(o, cls)]
        key = uuid.uuid1().int
        final = cls(self.con, key)
        self.db.sunionstore(key, [self.key, *other_keys])
        return final
        

    def update(self, *others):
        other_keys = [o.key for o in others if isinstance(o, cls)]
        self.db.sunionstore(self.key, [self.key, *other_keys])
        


def raise_if_of_type(v, typ):
    if not isinstance(v, typ):
        raise TypeError("{0} is not of type ".format(v, typ))
