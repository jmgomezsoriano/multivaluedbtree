import enum
from enum import unique
from typing import Tuple, List, Any

from BTrees.OOBTree import OOBTree
from multiprocessing import Lock


@unique
class QueueType(enum.Enum):
    LIFO = 1
    FIFO = 2


class MultivaluedBTree(OOBTree):
    """ A multivalued implementation of a BTree. That means, a BTree which the value of a (key, value) pair can
    save several values in the same key instead of just one. Therefore, when you execute:

       <code>
       tree = MultivaluedBTree()
       tree['a'] = 1
       tree['a'] = 2
       <code/>

    The second value does not replace the first one, but it store the two. Therefore, if you do the following:
       ```python
       print(tree['a'])
       ```
    The list [1, 2] is printed.
    """
    def __init__(self, reverse: bool = False, queue_type: QueueType = QueueType.LIFO, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._queue_type = queue_type
        self._reverse = reverse
        self._lock = Lock()
        self._length = 0

    def pop(self, key: Any, default: Any = None) -> Any:
        self._lock.acquire()
        try:
            return self.__pop(key, default)
        finally:
            self._lock.release()

    def popitem(self) -> Tuple[object, object]:
        self._lock.acquire()
        try:
            key = self.maxKey() if self._reverse else self.minKey()
            value = self.__pop(key)
            return key, value
        finally:
            self._lock.release()

    def __pop(self, key: Any, default: Any = None) -> Any:
        values = self[key] if default is None else self.get(key, [default])
        value = values.pop()
        if not values and key in self:
            del self[key]
        self._length -= 1
        return value

    def __delete__(self, instance: object) -> None:
        self._lock.acquire()
        try:
            self._length -= len(self[instance])
            del self[instance]
        finally:
            self._lock.release()

    def __setitem__(self, key: object, value: object) -> None:
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
        return self._length

    def __repr__(self):
        return repr({key: values for key, values in self.items()})

    def values(self, minimum: object = None, maximum: object = None) -> List[object]:
        """
        values([minimum, maximum]) -> list of values

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
