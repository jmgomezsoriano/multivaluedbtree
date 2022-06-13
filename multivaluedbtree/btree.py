import enum
from enum import unique
from typing import Tuple, List, Any

from BTrees.OOBTree import OOBTree
from multiprocessing import Lock


@unique
class QueueType(enum.Enum):
    """ Enumeration with the available queue types. """
    LIFO = 1
    FIFO = 2


class MultivaluedBTree(OOBTree):
    """ A multivalued implementation of a BTree. That means, a BTree which the value of a (key, value) pair can
    save several values in the same key instead of just one. Therefore, when you execute::

       tree = MultivaluedBTree()
       tree['a'] = 1
       tree['a'] = 2

    The second value does not replace the first one, but it store the two. Therefore, if you do the following::

       print(tree['a'])

    The list [1, 2] is printed. However, if we use _popitem()_ or _pop()_ methods, only one value are get::

        tree.popitem()  # Returns the pair ('a', 2)
        tree.pop()  # Returns 1

    The key order can be changed by the class parameter _reverse_ and the element order for each key with
    _queue_type_. For example::

        tree = MultivaluedBTree(reverse=True, queue_type=QueueType.FIFO)

    By default, the key order is incremental ant the queue type for each key values is LIFO.
    """
    def __init__(self, reverse: bool = False, queue_type: QueueType = QueueType.LIFO, *args, **kwargs) -> None:
        """ Constructor.
        :param reverse: False if the keys are ordered incrementally, if True, the order is decremental.
        :param queue_type: For each key, how to get the values with pop() and popitem() methods.
            By default, its value is QueueType.LIFO (Last In, First Out),
            but it can be also QueueType.FIFO (First In, First Out).
        :param kwargs: Extra parameters for OOBTree object.
        """
        super().__init__(*args, **kwargs)
        self._queue_type = queue_type
        self._reverse = reverse
        self._lock = Lock()
        self._length = 0

    def pop(self, key: Any, default: Any = None) -> Any:
        """ Extract one element of the key. If the key does not exist or return the default if it is defined,
           otherwise raise KeyError exception.

        :param key: The key.
        :param default: The default value, if it is not given, an exception is raise if the key does not exist.
        :return: The first element ot extract of that key.
           You can change this order with the queue_type parameter of the class constructor.
        """
        self._lock.acquire()
        try:
            return self.__pop(key, default)
        finally:
            self._lock.release()

    def popitem(self) -> Tuple[object, object]:
        """ Extract a tuple with the key and key value of the first key and first key value.
           The order of the keys and the key values can be changed with the constructor parameters
           "reverse" and "queue_type", respectively.

        :return: A tuple with the first key and the first key value.
        """
        self._lock.acquire()
        try:
            key = self.maxKey() if self._reverse else self.minKey()
            value = self.__pop(key)
            return key, value
        finally:
            self._lock.release()

    def __pop(self, key: Any, default: Any = None) -> Any:
        """ Extract one element of the key. If the key does not exist or return the default if it is defined,
           otherwise raise KeyError exception. This method is not process synchronized.

        :param key: The key.
        :param default: The default value, if it is not given, an exception is raise if the key does not exist.
        :return: The first element ot extract of that key.
           You can change this order with the queue_type parameter of the class constructor.
        """
        values = self[key] if default is None else self.get(key, [default])
        value = values.pop()
        if not values and key in self:
            del self[key]
        self._length -= 1
        return value

    def __delete__(self, instance: object) -> None:
        """ Delete an instance of this BTree, even if the key has assigned several values.

        :param instance: The key to delete.
        """
        self._lock.acquire()
        try:
            self._length -= len(self[instance])
            del self[instance]
        finally:
            self._lock.release()

    def __setitem__(self, key: object, value: object) -> None:
        """ Assign or add a value to that key.
        :param key: The key.
        :param value: The value to assign or add to that key.
        """
        self._lock.acquire()
        try:
            values = self[key] if key in self else []
            if self._queue_type == QueueType.LIFO:
                values.append(value)
            else:
                values.insert(0, value)
            super().__setitem__(key, values)
            self._length += 1
        finally:
            self._lock.release()

    def __len__(self) -> int:
        """
        :return: The number of values of this tree.
        """
        return self._length

    def __repr__(self):
        """
        :return: A representation of this object.
        """
        return repr(self.to_dict())

    def to_dict(self) -> dict:
        """
        :return: A dictionary representation of this tree.
        """
        return {key: values for key, values in self.items()}

    def values(self, minimum: object = None, maximum: object = None) -> List[object]:
        """
        values([minimum, maximum]) -> list of values which the key is >= minimum and <= maximum.

        Returns the values of the BTree.  If min and max are supplied, only
        values corresponding to keys greater than min and less than max are
        returned.
        """
        results = []
        for values in super().values(minimum, maximum):
            results.extend(values)
        if self._reverse:
            results.reverse()
        return results

    def update(self, collection: dict) -> None:
        """ Update this collection with other in dictionary format.
        :param collection: The other collection.
        """
        for key, value in collection.items():
            if isinstance(collection, MultivaluedBTree):
                for v in value:
                    self[key] = v
            else:
                self[key] = value
