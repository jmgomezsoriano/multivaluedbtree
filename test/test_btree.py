import unittest

from multivaluedbtree import MultivaluedBTree


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
        self.assertListEqual(tree.values('b', 'c'), [3, 4])
        self.assertListEqual(tree.values('c', 'd'), [4, 5, 6])
        self.assertTupleEqual(('a', 2), tree.popitem())
        self.assertEqual(tree['a'], [1])
        self.assertEqual(1, tree.pop('a'))

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


if __name__ == '__main__':
    unittest.main()
