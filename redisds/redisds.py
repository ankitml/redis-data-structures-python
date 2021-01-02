import ast
from collections import Iterable, abc
import redis
import string
import uuid

DECODER = lambda byte:byte.decode("utf-8")
CHARACTERS = string.ascii_letters + string.punctuation  + string.digits
IS_ITERABLE = lambda v:isinstance(v, Iterable)
scrub_parent_id = lambda d: {k:v for k,v in d.items() if k != 'parent_id'}


def raise_if_of_type(v, typ):
    if not isinstance(v, typ):
        raise TypeError("{0} is not of type ".format(v, typ))


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
        except redis.ResponseError:
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

    def copy(self):
        """
        copies the list into a new key. returns the copied redis list object
        """
        key = uuid.uuid1().int
        cls = type(self)
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


class RedisDeque(RedisList):
    """
    list-like container with fast appends and pops on either end
    """

    def __init__(self, connection, key, maxlen=None):
        self.maxlen = maxlen or 0
        super().__init__(connection, key)

    def appendleft(self, x):
        pass

    def clear(self):
        self._clear()

    def copy(self):
        pass

    def extendleft(self, arr):
        pass

    def index(self, x, start=None, stop=None):
        pass

    def insert(self, i, x):
        pass

    def pop(self):
        """
        Remove and return an element from the right side of the deque. If no elements are present, raises an IndexErrorj
        """
        pass

    def popleft(self):
        """
        Remove and return an element from the left side of the deque. If no elements are present, raises an IndexError.

        """
        pass

    def reverse(self):
        """
        in place reversal of deque elements
        """
        pass

    def rotate(self, n):
        pass

    @property
    def maxlen(self):
        return self.maxlen


class RedisDict(abc.MutableMapping, RedisDSBase):

    def __getitem__(self, field):
        val = self.c.hget(self.key, field)
        if val is not None:
            return DECODER(val)
        raise KeyError(str(field))

    def __setitem__(self, field, val):
        self.c.hset(self.key, field, val)

    def __delitem__(self, k):
        if not self.c.hdel(self.key, k):
            raise KeyError(str(k))

    def __len__(self):
        return self.c.hlen(self.key)

    def __iter__(self):
        return self.get_local_dict().__iter__()

    def __contains__(self, k):
        return self.c.hexists(self.key, k)

    def get_local_dict(self):
        return {DECODER(k):DECODER(v) for k,v in self.c.hgetall(self.key).items()}

    def __repr__(self):
        return "<{klass} '{key}' {dictionary}>".format(klass='RedisDict', 
                                                       key=self.key,
                                                       dictionary=self.get_local_dict())

    def keys(self):
        return [DECODER(i) for i in self.c.hkeys(self.key)]

    def values(self):
        return [DECODER(i) for i in self.c.hvals(self.key)]

    def copy(self):
        cls = type(self)
        key = uuid.uuid1().int
        other = cls(self.con, key)
        other.c.hmset(self.items())
        return other

    def items(self):
        return self.get_local_dict().items()

    def get(self, k, default=None):
        try:
            return DECODER(self[k])
        except KeyError:
            return default

    def pop(self, k, d):
        pass

    def update(self, other):
        pass

    def clear(self):
        self.c.delete(self.key)

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
        """
        adds the values instead of replcing them from other
        other is also key value pair supporting .items() protocol
        """
        if isinstance(other, abc.MutableMapping):
            for key, value in other.items():
                self.c.hincrby(self.key, key, value)
        elif isinstance(other, Iterable):
            for i in other:
                self.c.hincrby(self.key, i, 1)
        else:
            self.c.hincrby(self.key, other)

    def elements(self):
        pass

    def most_common(self, n=None):
        pass

    def subtract(self, d):
        pass

    def __repr__(self):
        return "<{klass} '{key}' {dictionary}>".format(klass='RedisCounter', 
                                                       key=self.key,
                                                       dictionary=self.get_local_dict())


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


class RedisTree(RedisDSBase):
    """
    There can be only on root node
    """

    def __init__(self, connection, key, root_node_data=None):
        if root_node_data is None:
            root_node_data = {}

        key = "tree:{0}".format(key)
        super().__init__(connection, key)
        self.data = RedisDict(connection, key)
        self._setup_root(root_node_data)

    def _setup_root(self, root_node_data):
        root_node_id = self._extract_root_node()
        # if the tree already exists.. root would be there and root_node_data is untouched
        if not root_node_id:
            root_node_id = self._add_node(root_node_data)
        self.root_node_id = root_node_id

    def _get_local_data(self):
        local_data = self.data.get_local_dict()
        return {k: ast.literal_eval(v) for k,v in local_data.items()}

    def _extract_root_node(self):
        local_data = self._get_local_data()
        nodes_without_parent = [key for key, val in local_data.items() if val["parent_id"] is None]
        if len(nodes_without_parent) > 1:
            # allw for garbage collection here
            raise Exception('Malformed Tree, many root nodes found')
        try:
            return nodes_without_parent[0]
        except IndexError:
            return None

    def __repr__(self):
        return self.data.__repr__()

    def _clean_id_field(self, node):
        if 'id' in node:
            del node['id']
        return node

    def _add_node(self, node, parent=None):
        node = self._clean_id_field(node)
        uid = uuid.uuid4().hex
        node.update({'parent_id':parent})
        self.data[uid] = node
        return uid

    def add_node(self, node, parent=None):
        if parent is None:
            parent = self.root_node_id
        if parent not in self.data:
            raise KeyError('incorrect parent id')
        return self._add_node(node, parent)

    def update_node(self, node):
        uid = self._extract_id(node)
        self.data[uid] = node

    def delete_node(self, node):
        uid = self._extract_id(node)
        del self.data[uid]

    def get_children(self, node_id):
        """
        assumes existence of local dict copy
        """
        child_ids = [k for k,v in self.local_data.items() if v["parent_id"] == node_id]
        children = []
        for child_id in child_ids:
            child_children = self.get_children(child_id)
            if child_children:
                child = {**scrub_parent_id(self.local_data[child_id]), "children": child_children}
            else:
                child = scrub_parent_id(self.local_data[child_id])
            children.append(child)
        return children or None

    def get_tree(self):
        self.local_data = self._get_local_data()
        children = self.get_children(self.root_node_id)
        return {**scrub_parent_id(self.local_data[self.root_node_id]), "children": self.get_children(self.root_node_id)}

    def move_node(self, node):
        pass

