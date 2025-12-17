"""
Single-file OPC UA client script
- Connects to OPC UA server
- Prints namespaces
- Browses full node tree from Objects
- Reads values from Variable nodes
- Stores full output into a TXT file

Endpoint used:
    opc.tcp://172.30.2.55:48080/uOPC/

Requirements:
    pip install opcua
"""

from opcua import Client, ua
import sys
from datetime import datetime

OPC_ENDPOINT = "opc.tcp://172.30.2.55:48080/uOPC/"
OUTPUT_FILE = f"opcua_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"


def browse_node(node, file, level=0, max_depth=10):
    """Recursively browse OPC UA nodes and write output to file"""
    if level > max_depth:
        return

    try:
        node_class = node.get_node_class()
        browse_name = node.get_browse_name()
        display_name = node.get_display_name().Text

        indent = "  " * level
        file.write(f"{indent}- {browse_name} | {node_class.name}\n")

        # If this is a Variable, try reading its value
        if node_class == ua.NodeClass.Variable:
            try:
                dv = node.get_data_value()
                value = dv.Value.Value
                dtype = dv.Value.VariantType
                status = dv.StatusCode
                ts = dv.SourceTimestamp

                file.write(f"{indent}    Value      : {value}\n")
                file.write(f"{indent}    DataType   : {dtype}\n")
                file.write(f"{indent}    StatusCode : {status}\n")
                file.write(f"{indent}    Timestamp  : {ts}\n")
            except Exception as e:
                file.write(f"{indent}    [Value read failed: {e}]\n")

        # Browse children
        for child in node.get_children():
            browse_node(child, file, level + 1, max_depth)

    except Exception as e:
        file.write(f"{'  ' * level}[Browse failed: {e}]\n")


def main():
    print("Connecting to OPC UA server...")
    client = Client(OPC_ENDPOINT)

    try:
        client.connect()
        print("Connected successfully")

        with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
            file.write("OPC UA SERVER BROWSE OUTPUT\n")
            file.write(f"Endpoint : {OPC_ENDPOINT}\n")
            file.write(f"Generated: {datetime.now()}\n\n")

            # Write namespaces
            file.write("Namespaces:\n")
            namespaces = client.get_namespace_array()
            for idx, ns in enumerate(namespaces):
                file.write(f"  ns={idx} -> {ns}\n")

            file.write("\nBrowsing OPC UA Address Space:\n\n")

            # Start browsing from Objects node
            objects = client.get_objects_node()
            browse_node(objects, file)

        print(f"Output successfully written to: {OUTPUT_FILE}")

    except KeyboardInterrupt:
        print("Interrupted by user")

    except Exception as e:
        print("Error:", e)

    finally:
        print("Disconnecting...")
        client.disconnect()
        print("Disconnected")


if __name__ == "__main__":
    main()
