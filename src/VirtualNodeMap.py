import random
import math


# Stores the vnode to node mapping
# Composed within a node so that every node has its own vnode mapping
class VirtualNodeMap:
    def __init__(self, node_names, TOTAL_VIRTUAL_NODES):
        self._vnode_map = {}
        self._node_names = node_names
        self._TOTAL_VIRTUAL_NODES = TOTAL_VIRTUAL_NODES
        self._node_vnode_map = {}

    @property
    def vnode_map(self):
        return self._vnode_map

    @property
    def node_names(self):
        return self._node_names

    # Populates the Virtual Node Nap, given the set of Node names.
    # Creates a mapping of Virtual Node to corresponding assigned physical Node
    def populate_map(self):

        # Problem statement 1
        # Generate a dict of vnode ids (0 to (TOTAL_VIRTUAL_NODES - 1) mapped randomly 
        # but equally (as far as maths permits) to node names

        """ This assigns node to vnode in a sequential manner.
        Meaning  -
        Lets say we have 4 nodes : node-1, node-2, node-3, node-4
        Allocation happens as follows:
        node-1 -> 0,4,8,12..
        node-2 -> 1,5,9,13..
        node-3 -> 2,6,10,14..
        node-4 -> 3,7,11,15..
        """
        total_node_count = len(self._node_names)
        for v in range(-1, self._TOTAL_VIRTUAL_NODES, total_node_count):  # The counter is increased by
            # total_node_count. The outer loop increase by this integer.
            t = v  # This 't' is increased by 1 in inner loop for sequential effect and is assigned to each node
            for node in self._node_names:
                t = t + 1
                self._vnode_map[t] = node
                if node not in self._node_vnode_map:
                    self._node_vnode_map[node] = [t]
                else:
                    self._node_vnode_map[node].append(t)

    # Return the vnode name mapped to a particular vnode
    def get_node_for_vnode(self, vnode):
        return self._vnode_map[vnode]

    # Returns the vnode name where a particular key is stored
    # It finds the vnode for the key through modulo mapping, and then looks up the physical node
    def get_assigned_node(self, key):
        vnode = key % self._TOTAL_VIRTUAL_NODES
        return self._vnode_map[vnode]

    # Assign a new node name as mapping for a particular vnode
    # This is useful when vnodes are remapped during node addition or removal
    def set_new_assigned_node(self, vnode, new_node_name):
        self._vnode_map[vnode] = new_node_name
