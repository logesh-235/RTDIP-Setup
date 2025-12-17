from opcua import Client, ua
import time

OPC_ENDPOINT = "opc.tcp://172.30.2.55:48080/uOPC/"
SAMPLES_PER_TAG = 5
DELAY_SECONDS = 1

# Infrastructure noise
EXCLUDE_KEYWORDS = [
    "Server",
    "Diagnostics",
    "Security",
    "AggregateFunctions",
    "Namespaces",
    "BacNet",
    "DNP3",
    "Redundancy",
    "InputArguments",
    "OutputArguments",
    "LastChange"
]

def is_valid_tag(tag):
    name = tag["name"]
    node_id = tag["node_id"]

    # Exclude method-related and system nodes
    if any(k.lower() in name.lower() for k in EXCLUDE_KEYWORDS):
        return False

    # Only keep string NodeIds (real data)
    if not node_id.startswith("ns="):
        return False

    return True

def main():
    client = Client(OPC_ENDPOINT)
    client.connect()
    print("Connected to OPC UA server\n")

    objects = client.get_objects_node()
    tags = []

    # Discover tags
    def find_tags(node):
        try:
            if node.get_node_class() == ua.NodeClass.Variable:
                tags.append({
                    "name": node.get_browse_name().Name,
                    "node_id": node.nodeid.to_string()
                })
                return

            for child in node.get_children():
                find_tags(child)

        except Exception:
            pass

    find_tags(objects)

    # Filter valid tags
    tags = [t for t in tags if is_valid_tag(t)]

    print(f"Reading {SAMPLES_PER_TAG} samples from {len(tags)} tags:\n")

    # Read 5 values from each tag
    for tag in tags:
        node = client.get_node(tag["node_id"])
        print(f"Tag: {tag['name']}")

        for i in range(SAMPLES_PER_TAG):
            try:
                dv = node.get_data_value()
                value = dv.Value.Value
                ts = dv.SourceTimestamp
                status = dv.StatusCode

                print(f"  [{i+1}] Value={value} | Status={status} | Time={ts}")

            except Exception as e:
                print(f"  [{i+1}] Read failed: {e}")

            time.sleep(DELAY_SECONDS)

        print("-" * 60)

    client.disconnect()
    print("\nDisconnected")

if __name__ == "__main__":
    main()
