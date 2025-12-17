from opcua import Client, ua

OPC_ENDPOINT = "opc.tcp://172.30.2.55:48080/uOPC/"

# Nodes to ignore (infrastructure noise)
EXCLUDE_KEYWORDS = [
    "Server",
    "Diagnostics",
    "Security",
    "AggregateFunctions",
    "Namespaces",
    "BacNet",
    "DNP3",
    "Redundancy"
]

def is_excluded(node):
    name = node.get_browse_name().Name
    return any(k.lower() in name.lower() for k in EXCLUDE_KEYWORDS)

def find_tags(node, tags):
    try:
        if is_excluded(node):
            return

        node_class = node.get_node_class()

        if node_class == ua.NodeClass.Variable:
            tags.append({
                "name": node.get_browse_name().Name,
                "node_id": node.nodeid.to_string()
            })
            return

        for child in node.get_children():
            find_tags(child, tags)

    except Exception:
        pass

def main():
    client = Client(OPC_ENDPOINT)
    client.connect()

    print("Connected to OPC UA server\n")

    tags = []
    objects = client.get_objects_node()
    find_tags(objects, tags)

    client.disconnect()

    print(f"Found {len(tags)} tags:\n")
    for t in tags:
        print(f"{t['name']} -> {t['node_id']}")

if __name__ == "__main__":
    main()
