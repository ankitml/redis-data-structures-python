from collections import abc
from redis import ResponseError
import uuid


class RedisDict(abc.MutableMapping, RedisDSBase):

    def __getitem__(self, field):
        val = self.c.hget(self.key, field)
        if val is not None:
            return val
        raise KeyError(str(key))

    def __setitem__(self, field, val):
        self.c.hset(self.key, field, val)

    def __delitem__(self, k):
        if not self.c.hdel(self.key, k):
            raise KeyError(str(k))
        
    def __len__(self):
        return self.c.hlen(self.key)

    def __iter__(self):
        return self._fetch_dict().__iter__()

    def __contains__(self, k):
        return self.hexists(self.key, k)

    def _fetch_dict(self):
        return self.c.hgetall(self.key)

    def __repr__(self):
        return "<{klass} '{key}' {dictionary}>".format(klass='RedisDict', 
                                                       key=self.key,
                                                       dictionary=self._fetch_dict())

    def keys(self):
        return self.c.hkeys(self.key)

    def values(self):
        return self.c.hvals(self.key)

    def copy(self):
        cls = type(self)
        key = uuid.uuid1().int
        other = cls(self.con, key)
        other.c.hmset(self.items())
        return other

    def items(self):
        return self._fetch_dict()


    def get(self, k, default=None):
        try:
            return self[k]
        except KeyError:
            return default

    def pop(self, k, d):
        pass

    def update(self, other):
        pass

    def clear(self):
        pass

    def fromkeys(self):
        pass

    def popitem(self, item):
        pass

    def setdefault(self, v):
        pass

    def update(self, other):
        pass


class RedisCounter(RedisDict):

    def update(self, other):
        pass
