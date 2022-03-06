import unittest

from tree_structure import TreeStructure
from treelib import Tree

class TreeStructureTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.treeStructure = TreeStructure()
        return super().setUp()

    def test_basic_tree(self):
        expected_tree = Tree()
        expected_tree.create_node("From", "from")
        expected_tree.create_node("To", "to", "from")

        actual_tree = self.treeStructure.create_tree([('To', 'From')])
        self.assertTreesAreEqual(expected_tree, actual_tree)

    def test_double_nested(self):
        expected_tree = Tree()
        expected_tree.create_node("Level2", "level2")
        expected_tree.create_node("Level1", "level1", "level2")
        expected_tree.create_node("Level0", "level0", "level1")

        actual_tree = self.treeStructure.create_tree([("Level1", "Level2"), ("Level0", "Level1")])
        self.assertTreesAreEqual(expected_tree, actual_tree)

    def test_double_nested_reversed_data(self):
        expected_tree = Tree()
        expected_tree.create_node("Level2", "level2")
        expected_tree.create_node("Level1", "level1", "level2")
        expected_tree.create_node("Level0", "level0", "level1")

        actual_tree = self.treeStructure.create_tree([("Level0", "Level1"), ("Level1", "Level2"),])
        self.assertTreesAreEqual(expected_tree, actual_tree)

    def test_mismatched_data_order(self):
        expected_tree = Tree()
        expected_tree.create_node("Level3", "level3")
        expected_tree.create_node("Level2", "level2", "level3")
        expected_tree.create_node("Level1", "level1", "level2")
        expected_tree.create_node("Level0", "level0", "level1")

        test_data = [
            ("Level0", "Level1"),
            ("Level2", "Level3"),
            ("Level1", "Level2")
        ]
        trees = self.treeStructure.create_trees(test_data)
        self.assertEqual(len(trees), 1)
        self.assertTreesAreEqual(expected_tree, trees[0])

    def test_mismatched_data_sets(self):
        expected_tree = Tree()
        expected_tree.create_node("Level1", "level1")
        expected_tree.create_node("Level0", "level0", "level1")

        expected_tree1 = Tree()
        expected_tree1.create_node("LevelB", "levelB")
        expected_tree1.create_node("LevelA", "levelA", "levelB")

        test_data = [
            ("Level0", "Level1"),
            ("LevelA", "LevelB")
        ]
        trees = self.treeStructure.create_trees(test_data)
        self.assertEqual(len(trees), 2)
        self.assertTreesAreEqual(expected_tree, trees[0])
        self.assertTreesAreEqual(expected_tree1, trees[1])

    def test_empty_list(self):
        expected_tree = Tree()

        actual_tree = self.treeStructure.create_tree([])
        self.assertTreesAreEqual(expected_tree, actual_tree)

    def assertTreesAreEqual(self, expected_tree: Tree, actual_tree: Tree):
        expected_tree_data = self._extract_tree_data_to_list(expected_tree)
        actual_tree_data = self._extract_tree_data_to_list(actual_tree)
        self.assertEqual(expected_tree_data, actual_tree_data)

    def _extract_tree_data_to_list(self, tree: Tree) -> list:
        if tree.depth() == 0: 
            return []
        return [tree[node].tag for node in tree.expand_tree(mode=Tree.DEPTH)]

if __name__ == '__main__':
    unittest.main()