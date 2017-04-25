from collections import abc
from base import RedisDSBase, IS_ITERABLE, CHARACTERS, DECODER
from redis import ResponseError
import uuid


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

    def isdisjoint(self, other):
        cls = type(self)
        if not isinstance(other, cls):
            raise TypeError('Other should be of type redis set')
        return not bool(self.c.sinter([self.key, other.key]))

    def issubset(self, other):
        return self <= other

    def issuperset(self, other):
        return self >= other

    def pop(self):
        return self.c.spop(self.key)

    def remove(self, element):
        r = self.c.srem(self.key, element)
        if r == 0:
            raise KeyError(element)

    def symmetric_difference(self, other):
        union = self.union(other)
        intersection = self.intersection(other)
        return union.difference(intersection)
        
    def symmetric_difference_update(self):
        union = self.union(other)
        intersection = self.intersection(other)
        final_set = union.difference(intersection)
        self.clear()
        for i in final_set:
            self.add(i)

    def union(self, *others):
        cls = type(self)
        other_keys = [o.key for o in others if isinstance(o, cls)]
        key = uuid.uuid1().int
        final = cls(self.con, key)
        self.c.sunionstore(key, [self.key, *other_keys])
        return final
        
    def update(self, *others):
        other_keys = [o.key for o in others if isinstance(o, cls)]
        self.c.sunionstore(self.key, [self.key, *other_keys])

    def __le__(self, other):
        return self.c.sinter([self.key, other.key]) == set(self)

    def __lt__(self, other):
        return self <= other and self != other

    def __ge__(self, other):
        return self.c.sinter([self.key, other.key]) == set(other)

    def __gt__(self, other):
        return self >= other and self != other

    def __eq__(self, other):
        if len(self) == len(other):
            if set(self) == set(other):
                return True
        return False

    def __ne__(self, other):
        if type(self) is not type(other):
            raise TypeError('Other should be of type redis set')
        return not self == other


class RedisSortedSet(RedisSet):

    pass
