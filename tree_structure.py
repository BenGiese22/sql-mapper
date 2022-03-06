from typing import Tuple
from treelib import Node, Tree


class TreeStructure:

    def __init__(self) -> None:
        pass

    def create_trees(self, insert_into_from_data: list) -> list:
        trees = []
        tree, remaining_data = self.create_tree(Tree(), insert_into_from_data)
        trees.append(tree)
        while len(remaining_data) > 0:
            tree, remaining_data = self.create_tree(Tree(), remaining_data)
            trees.append(tree)
        return trees

    def create_tree(self, tree: Tree, data_subset: list) -> Tuple[Tree, list]:
        remaining_data = []

        for subset in data_subset:
            parent_node = subset[1]
            child_node = subset[0]
            if tree.depth() == 0: # tree is empty, start one
                tree.create_node(parent_node, parent_node)
                tree.create_node(child_node, child_node, parent_node)
                continue
            
            existing_nodes = self._get_existing_nodes(tree)
            if parent_node in existing_nodes:
                tree.create_node(child_node, child_node, parent_node)
            elif child_node in existing_nodes:
                #create a new tree and add this tree to it.
                parent_tree = Tree()
                parent_tree.create_node(parent_node, parent_node)
                # parent_tree.create_node(child_node, child_node, parent_node)
                parent_tree.paste(parent_node, tree)
                tree = parent_tree
            else:
                print('Data not relevant to this tree')
                remaining_data.append(subset)
        
        if len(remaining_data) == len(data_subset): #nothing matched.
            return tree, remaining_data
        return self.create_tree_temp(tree, remaining_data)

    def _get_existing_nodes(self, tree) -> list:
        return [tree[node].tag for node in tree.expand_tree(mode=Tree.DEPTH)]

