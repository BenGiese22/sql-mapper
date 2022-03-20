from typing import Tuple
from treelib import Tree
from treelib.exceptions import DuplicatedNodeIdError, MultipleRootError
from enums.search_pattern import SearchPattern

class TreeStructure:

    def __init__(self, search_pattern: SearchPattern) -> None:
        self.search_pattern = search_pattern


    def create_trees(self, data_set: list) -> list:
        if self.search_pattern is SearchPattern.INSERT_INTO_FROM:
            return self.create_insert_into_from_trees(data_set)
        return self.create_create_tree(data_set)

    def create_create_tree(self, data_set: list) -> list:
        tree = Tree()
        if len(data_set) == 0:
            tree.create_node("No results", "no_results")
            return tree
        
        tree.create_node("Create Table Instances", "create_table_instances")
        for create_table_instance in data_set:
            tree.create_node(str(create_table_instance), str(create_table_instance).lower(), "create_table_instances")
        return [tree]

    def create_insert_into_from_trees(self, insert_into_from_data: list) -> list:
        trees = []
        tree, remaining_data = self._create_insert_into_from_tree(Tree(), insert_into_from_data)
        trees.append(tree)
        while len(remaining_data) > 0:
            tree, remaining_data = self._create_insert_into_from_tree(Tree(), remaining_data)
            trees.append(tree)
        return trees

    def _create_insert_into_from_tree(self, tree: Tree, data_subset: list) -> Tuple[Tree, list]:
        remaining_data = []

        for subset in data_subset:
            parent_node = subset[1]
            child_node = subset[0]
            if tree.depth() == 0: # tree is empty, start one
                try:
                    tree.create_node(parent_node, parent_node)
                    tree.create_node(child_node, child_node, parent_node)
                except (DuplicatedNodeIdError, MultipleRootError):
                    print(data_subset)
                    print(parent_node)
                    print(child_node)
                    print(subset)
                    tree.show()
                continue
            
            existing_nodes = self._get_existing_nodes(tree)
            if parent_node in existing_nodes:
                try:
                    tree.create_node(child_node, child_node, parent_node)
                except DuplicatedNodeIdError:
                    tree.show()
            elif child_node in existing_nodes:
                #create a new tree and add this tree to it.
                parent_tree = Tree()
                parent_tree.create_node(parent_node, parent_node)
                # parent_tree.create_node(child_node, child_node, parent_node)
                parent_tree.paste(parent_node, tree)
                tree = parent_tree
            else:
                # print('Data not relevant to this tree')
                remaining_data.append(subset)
        
        if len(remaining_data) == len(data_subset): #nothing matched.
            return tree, remaining_data
        return self._create_insert_into_from_tree(tree, remaining_data)

    def _get_existing_nodes(self, tree) -> list:
        return [tree[node].tag for node in tree.expand_tree(mode=Tree.DEPTH)]

