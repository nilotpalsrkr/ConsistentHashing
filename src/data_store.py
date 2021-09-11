import random


from InfoGenerator import InfoGenerator
from User import UserInfo, UserData
from Node import Node

TOTAL_VIRTUAL_NODES = 200
DEFAULT_NUM_NODES = 4
INITIAL_NUM_KEYS = 10000
RANDOM_STRING_LENGTH = 8
PASSWORD_LENGTH = 32


# Client Context
node_names = []
node_dict = {}

print('###############  INITIAL CONFIGURATION ###############\n')

# Generates random node names for the initial default nodes
node_names = [InfoGenerator.generate_node_name(RANDOM_STRING_LENGTH) for i in range(DEFAULT_NUM_NODES)]

# Creates the first node and initializes the vnode mapping
# Also keeps a copy of the vnode map instance in the client for later use
first_name = node_names[0]
first_node = Node(first_name, TOTAL_VIRTUAL_NODES)
node_dict[first_name] = first_node
first_node.initialize_vnode_map(node_names)
vnode_map = first_node.clone_vnode_map()

# Creates other nodes, intializing them with the same vnode mapping
# Also updates the complete node mapping in all nodes
for i in range(1, len(node_names)):
    node_dict[node_names[i]] = Node(node_names[i], TOTAL_VIRTUAL_NODES, first_node.clone_vnode_map())

for name in node_names:
    node_dict[name].populate_nodes(node_dict)


# Populates the distributed data store
for i in range(INITIAL_NUM_KEYS):
    user_info = UserInfo(InfoGenerator.generate_user_id(),
                             UserData(InfoGenerator.generate_email(RANDOM_STRING_LENGTH),
                                        InfoGenerator.generate_password(PASSWORD_LENGTH)))

    # Finds the right node and adds data to it
    # In the final solution, this is not necessary as sending data
    # to any node will internally set it on the appropriate node
    # We are explicitly sending to the owner node here since you still have 
    # to implement get_data and set_data in Node to be masterless
    node_name = vnode_map.get_assigned_node(user_info.user_id)
    node_dict[node_name].set_data(user_info.user_id, user_info.user_data, False)

# List node names and key counts
for node in node_dict.values():
    print(node)
print('\n\n')

# List random keys by calling any node
print('Random pickup of various keys on any node')
for i in range(10):
    user_id = random.randint(0, INITIAL_NUM_KEYS - 1)
    print(first_node.get_data(user_id))
print('\n\n')

# Add a new node to the Data Store
print('###############  ADDING A NODE ###############\n')

new_node_name = InfoGenerator.generate_node_name(RANDOM_STRING_LENGTH)
node_names.append(new_node_name)

# New node creation similar to the other existing nodes
new_node = Node(new_node_name, TOTAL_VIRTUAL_NODES, first_node.clone_vnode_map())
node_dict[new_node_name] = new_node
new_node.populate_nodes(node_dict)

# Updates node mapping of other nodes to add this new node
for node_name, node in node_dict.items():
    if node_name == new_node_name:
        continue
    node.add_new_node(new_node_name, new_node)

# List node names and key counts
for node in node_dict.values():
    print(node)
print('\n\n')

# List random keys by calling any node
print('Random pickup of various keys on any node')
for i in range(10):
    user_id = random.randint(0, INITIAL_NUM_KEYS - 1)
    print(new_node.get_data(user_id))
print('\n\n')


# Planned removal of a node from Data Store and reassignment of its vnodes
print('###############  REMOVING A NODE ###############\n')

# Remove any random node
# remove_current_node is only called on the to-be removed node
random.shuffle(node_names)
del_node_name = node_names.pop(0)
del_node = node_dict.pop(del_node_name, 'key not found')
del_node.remove_current_node(node_dict)


# List node names and key counts
for node in node_dict.values():
    print(node)
print('\n\n')

# Pick any node from the remaining ones
node_iter = iter(node_dict.values())
any_node = next(node_iter)
any_other_node = next(node_iter)


# List random keys by calling any node
print('Random pickup of various keys on any node')
for i in range(10):
    user_id = random.randint(0, INITIAL_NUM_KEYS - 1)
    print(any_node.get_data(user_id))
print('\n\n')

# Test read/write on a new key
user_info = UserInfo(InfoGenerator.generate_user_id(),
                             UserData(InfoGenerator.generate_email(RANDOM_STRING_LENGTH),
                                        InfoGenerator.generate_password(PASSWORD_LENGTH)))

print(f'Generated user data: {user_info.user_data}')
any_node.set_data(user_info.user_id, user_info.user_data)
fetched_user_data = any_other_node.get_data(user_info.user_id)
print(f'Fetched user data: {fetched_user_data}')
