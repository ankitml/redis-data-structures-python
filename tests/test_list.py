import unittest
from redisds.redislist import RedisList
import pytest

INT_TO_STR = lambda x: str(x)


class TestRedisList:

    @pytest.fixture(scope='function')
    def rlist(self):
        import redis
        con = redis.StrictRedis(host='localhost', port=6379, db=9)
        r = RedisList(con, 'b')
        yield r
        r._clear()

    def test_append(self, rlist):
        l = [1,2,3]
        rlist.append(1)
        rlist.append(2)
        rlist.append(3)
        assert list(map(INT_TO_STR, l)) == rlist

    def test_extend(self, rlist):
        l = [1,2,3]
        rlist.extend(l)
        assert list(map(INT_TO_STR, l)) == rlist

    def test_remove(self, rlist):
        l = [1,2,3, 4]
        rlist.extend(l)
        l.remove(2)
        rlist.remove('2')
        assert list(map(INT_TO_STR, l)) == rlist



