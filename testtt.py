
# pip install kafka-python
import os, json
from kafka import KafkaConsumer

# In-cluster: 'kafka:9092' ; Host-side: '172.30.121.84:9094'
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

consumer = KafkaConsumer(
    "OPCUA",
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="compressor_probe"
)

print("Listening on", BOOTSTRAP)
count = 0
for msg in consumer:
    payload = msg.value
    node_id = payload.get("node_id","")
    disp = payload.get("display_name","")
    # show only Compressor ns=2
    if node_id.startswith("ns=2;s=PLC_S7_200_SMART.Compressor."):
        print(json.dumps(payload, indent=2))
        count += 1
        if count >= 25:
           
