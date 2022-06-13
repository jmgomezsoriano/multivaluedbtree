import unittest

from multivaluedbtree import MultivaluedBTree, QueueType


class MyTestCase(unittest.TestCase):
    def test_normal_btree(self):
        tree = MultivaluedBTree()
        tree['a'] = 1
        tree['a'] = 2
        tree['b'] = 3
        self.assertListEqual(tree['a'], [1, 2])
        self.assertListEqual(tree['b'], [3])
        self.assertEqual(len(tree), 3)
        self.assertEqual(tree.values(), [1, 2, 3])
        tree['c'] = 4
        tree['d'] = 5
        tree['d'] = 6
        self.assertEqual(len(tree), 6)
        self.assertListEqual(tree.values('b', 'c'), [3, 4])
        self.assertListEqual(tree.values('c', 'd'), [4, 5, 6])
        self.assertTupleEqual(tree.popitem(), ('a', 2))
        self.assertEqual(tree['a'], [1])
        self.assertEqual(tree.pop('a'), 1)
        with self.assertRaises(KeyError):
            tree.pop('a')
        self.assertEqual(tree.pop('a', 10), 10)

    def test_fifo_btree(self):
        tree = MultivaluedBTree(queue_type=QueueType.FIFO)
        tree['a'] = 1
        tree['a'] = 2
        tree['b'] = 3
        self.assertListEqual(tree['a'], [2, 1])
        self.assertListEqual(tree['b'], [3])
        self.assertEqual(len(tree), 3)
        self.assertEqual(tree.values(), [2, 1, 3])
        tree['c'] = 4
        tree['d'] = 5
        tree['d'] = 6
        self.assertListEqual(tree.values('b', 'c'), [3, 4])
        self.assertListEqual(tree.values('c', 'd'), [4, 6, 5])
        self.assertTupleEqual(tree.popitem(), ('a', 1))
        self.assertEqual(tree['a'], [2])
        self.assertEqual(tree.pop('a'), 2)
        with self.assertRaises(KeyError):
            tree.pop('a')
        self.assertEqual(tree.pop('a', 10), 10)

    def test_reverse_btree(self):
        tree = MultivaluedBTree(True)
        tree['a'] = 1
        tree['a'] = 2
        tree['b'] = 3
        tree['c'] = 4
        tree['d'] = 5
        tree['d'] = 6
        self.assertListEqual(tree['a'], [1, 2])
        self.assertListEqual(tree['b'], [3])
        self.assertEqual(len(tree), 6)
        self.assertEqual(tree.values(), [6, 5, 4, 3, 2, 1])
        self.assertListEqual(tree.values('b', 'c'), [4, 3])
        self.assertListEqual(tree.values('c', 'd'), [6, 5, 4])

    def test_reverse_fifo_btree(self):
        tree = MultivaluedBTree(reverse=True, queue_type=QueueType.FIFO)
        tree['a'] = 1
        tree['a'] = 2
        tree['b'] = 3
        tree['c'] = 4
        tree['d'] = 5
        tree['d'] = 6
        self.assertListEqual(tree['a'], [2, 1])
        self.assertListEqual(tree['b'], [3])
        self.assertEqual(len(tree), 6)
        self.assertEqual(tree.values(), [5, 6, 4, 3, 1, 2])
        self.assertListEqual(tree.values('b', 'c'), [4, 3])
        self.assertListEqual(tree.values('c', 'd'), [5, 6, 4])


if __name__ == '__main__':
    unittest.main()
