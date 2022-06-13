# Multivalued BTree
A multivalued BTree that allows to store several values in the same node.
This is based on [Python BTrees 4.10 module](https://pypi.org/project/BTrees/).

# How to use

It is very easy to use. You only have a MultivaluedBTree() class that has two optional parameters _reverse_ and 
_queue_type_ to define how the elements are extracted with pop(), popitem() or get() methods.
This class is thought to implement an ordered queue in a BTree structure that can have several values
for each key. Therefore, the assignment operator does not replace the previous value but add a new one
when the tree has already that key. Therefore, if you do:

```python
from multivaluedbtree import MultivaluedBTree

tree = MultivaluedBTree()
tree['a'] = 1  # Set 1 in the key 'a'
tree['a'] = 2 # Added 2 to the key 'a'
```

The tree will have the list [1, 2] for the key 'a' and its length will be 2.
The keys are ordered in increase order, but this order can be changed with _reverse_ parameter.
By default, the values in the same key are stored with LIFO (Last In, First Out),
but this order can be changed with the _queue_type_ parameter.

Example of use without parameters (keys in incremental order and values in LIFO queues):

```python
from multivaluedbtree import MultivaluedBTree

tree = MultivaluedBTree()
tree['a'] = 1  # Set 1 in the key 'a'
r1 = tree['a']  # Gets [1]
tree['a'] = 2  # Add 2 to the key 'a'
r2 = tree['a']  # Gets [1, 2]
len(tree)  # Returns 2 although it only has 1 key
tree['b'] = 3  # Set 3 to the key 'b'
len(tree)  # Returns 3 although it only has 2 key
tree['c'] = 4  # Sets 3 to the key 'c'
# Adds 5 and 6 to the key 'd'
tree['d'] = 5
tree['d'] = 6
tree.values()  # Returns [1, 2, 3]
tree.values('b', 'c')  # Returns [3, 4]
tree.values('c', 'd')  # Returns [4, 5, 6]
r3 = tree.popitem()  # Returns ('a', 2) and remove the item 2 from 'a'
r4 = tree['a']  # Returns [1]
r5 = tree.pop('a')  # Returns 1
r6 = tree.pop('a')  # Raises an KeyError because the key 'a' has no values.
r7 = tree.pop('a', 10)  # Returns 10 because is the default value if the key 'a' does not exist.
```

Example with FIFO queues as values:

```python
from multivaluedbtree import MultivaluedBTree, QueueType

tree = MultivaluedBTree(queue_type=QueueType.FIFO)
tree['a'] = 1
tree['a'] = 2
tree['b'] = 3
r1 = tree['a']  # Returns [2, 1]
len(tree)  # Returns 3
r2 = tree.values()  # Returns [2, 1, 3]
tree['c'] = 4
tree['d'] = 5
tree['d'] = 6
r3 = tree.values('b', 'c')  # Returns [3, 4]
r4 = tree.values('c', 'd'), # Returns [4, 6, 5]
r5 = tree.popitem()  # Returns ('a', 1))
r6 = tree['a']  # Returns [2]
r7 = tree.pop('a')  # Returns 2
tree.pop('a')  # Raises an KeyError because the key 'a' has no values.
tree.pop('a', 10)  # Returns 10 because is the default value if the key 'a' does not exist.
```

Example in decremental key order and LIFO queues:

```python
from multivaluedbtree import MultivaluedBTree

tree = MultivaluedBTree(reverse=True)
tree['a'] = 1
tree['a'] = 2
tree['b'] = 3
tree['c'] = 4
tree['d'] = 5
tree['d'] = 6
r1 = tree['a']  # Returns [1, 2]
r2 = tree['b']  # Returns [3]
len(tree)  # Returns 6
r3 = tree.values()  # Returns [6, 5, 4, 3, 2, 1]
r4 = tree.values('b', 'c')  # Returns [4, 3]
r5 = tree.values('c', 'd')  # Returns [6, 5, 4]
```
Example in decremental key order and LIFO queues:

```python
from multivaluedbtree import MultivaluedBTree, QueueType

tree = MultivaluedBTree(reverse=True, queue_type=QueueType.FIFO)
tree['a'] = 1
tree['a'] = 2
tree['b'] = 3
tree['c'] = 4
tree['d'] = 5
tree['d'] = 6
r1 = tree['a']  # Returns [2, 1]
r2 = tree['b']  # Returns [3]
len(tree)  # Returns 6
r3 = tree.values()  # Returns [5, 6, 4, 3, 1, 2]
r4 = tree.values('b', 'c')  # Returns [4, 3]
r5 = tree.values('c', 'd')  # Returns [5, 6, 4]
```