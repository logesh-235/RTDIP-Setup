
# opcua_kafka_connector.py (All-Data Mode) — corrected type hints
# - Discovers & subscribes to ALL variable nodes in batches
# - Robust JSON serialization (incl. ExtensionObject)
# - Status change handling + watchdog reconcile/resubscribe
# - Event subscription (Server events -> Kafka)
# - Optional namespace filtering & discovery caps

import asyncio
import json
import logging
import os
import signal
import sys
import yaml
from datetime import datetime, timezone
from enum import Enum, IntEnum, IntFlag
from typing import List

from asyncua import Client, ua
from asyncua.common import ua_utils  # val_to_string fallback
from asyncua.common.node import Node  # <-- FIXED: proper Node import
from kafka import KafkaProducer
from kafka.errors import KafkaError


# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
CONFIG_FILE = "./config/nodes.yaml"

OPCUA_ENDPOINT = os.getenv("OPCUA_ENDPOINT", "opc.tcp://localhost:48010")
OPCUA_SECURITY_MODE = os.getenv("OPCUA_SECURITY_MODE", "None")
OPCUA_SECURITY_POLICY = os.getenv("OPCUA_SECURITY_POLICY", "None")
OPCUA_CLIENT_TIMEOUT_SEC = float(os.getenv("OPCUA_CLIENT_TIMEOUT_SEC", "30.0"))
OPCUA_USERNAME = os.getenv("OPCUA_USERNAME")
OPCUA_PASSWORD = os.getenv("OPCUA_PASSWORD")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_VALUES = os.getenv("KAFKA_TOPIC", "OPCUA_VALUES")
KAFKA_TOPIC_EVENTS = os.getenv("KAFKA_TOPIC_EVENTS", "OPCUA_EVENTS")

SUBSCRIPTION_PERIOD_MS = int(os.getenv("SUBSCRIPTION_PERIOD_MS", "500"))
SUBSCRIPTION_BATCH_SIZE = int(os.getenv("SUBSCRIPTION_BATCH_SIZE", "250"))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", "20"))
TRIGGER_MODE = os.getenv("TRIGGER_MODE", "StatusValueTimestamp").strip()

NAMESPACE_ALLOWLIST = {
    int(x) for x in os.getenv("NAMESPACE_ALLOWLIST", "").split(",") if x.strip() != ""
}
MAX_DISCOVER_NODES = int(os.getenv("MAX_DISCOVER_NODES", "50000"))

HEARTBEAT_INTERVAL_SEC = int(os.getenv("HEARTBEAT_INTERVAL_SEC", "30"))
AUTO_REDISCOVER_INTERVAL_SEC = int(os.getenv("AUTO_REDISCOVER_INTERVAL_SEC", "0"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# ------------------------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("opcua_kafka_connector")


# ------------------------------------------------------------------------------
# Helpers: JSON serialization for OPC UA values
# ------------------------------------------------------------------------------
def to_plain_json(value):
    """Make any OPC UA SDK object JSON-serializable."""
    try:
        if isinstance(value, ua.DataValue):
            return {
                "value": to_plain_json(value.Value.Value),
                "status": int(value.StatusCode.value)
                if isinstance(value.StatusCode, ua.StatusCode)
                else value.StatusCode,
                "source_ts": value.SourceTimestamp.isoformat() if value.SourceTimestamp else None,
                "server_ts": value.ServerTimestamp.isoformat() if value.ServerTimestamp else None,
            }
        if isinstance(value, ua.Variant):
            return to_plain_json(value.Value)
        if isinstance(value, ua.StatusCode):
            return int(value.value)
        if isinstance(value, ua.LocalizedText):
            return {"text": value.Text, "locale": value.Locale}
        if isinstance(value, (Enum, IntEnum, IntFlag)):
            return value.name

        # extension object or complex classes
        if hasattr(value, "to_dict"):
            d = value.to_dict()
            return {k: to_plain_json(v) for k, v in d.items()} if isinstance(d, dict) else d
        elif hasattr(value, "to_json"):
            j = value.to_json()
            return j if isinstance(j, (dict, list, str, int, float, bool, type(None))) else ua_utils.val_to_string(value)
        elif hasattr(value, "__dict__"):
            d = {k: v for k, v in value.__dict__.items() if not k.startswith("_")}
            return {k: to_plain_json(v) for k, v in d.items()}

        if isinstance(value, (bytes, bytearray)):
            import base64
            return {"base64": base64.b64encode(value).decode("ascii")}
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                pass
        if isinstance(value, (list, tuple)):
            return [to_plain_json(v) for v in value]
        if isinstance(value, dict):
            return {k: to_plain_json(v) for k, v in value.items()}
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return ua_utils.val_to_string(value)
    except Exception:
        return ua_utils.val_to_string(value)


def chunk_list(items: List, size: int) -> List[List]:
    return [items[i : i + size] for i in range(0, len(items), size)]


# ------------------------------------------------------------------------------
# Load Node Configuration
# ------------------------------------------------------------------------------
def load_nodes_config():
    """Load OPC UA nodes from YAML configuration, if present."""
    if not os.path.exists(CONFIG_FILE):
        return []
    try:
        with open(CONFIG_FILE, "r") as f:
            config = yaml.safe_load(f) or {}
            return config.get("nodes", [])
    except Exception as e:
        logger.warning(f"Failed to read config {CONFIG_FILE}: {e}")
        return []


# ------------------------------------------------------------------------------
# Kafka Producer Setup
# ------------------------------------------------------------------------------
def create_kafka_producer():
    """Initialize Kafka producer with robust JSON serializer."""
    try:
        def json_serializer(obj):
            return json.dumps(obj, default=to_plain_json).encode("utf-8")

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=json_serializer,
            acks="all",
            retries=5,
            linger_ms=20,
            compression_type="lz4",
        )
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except KafkaError as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        sys.exit(1)


# ------------------------------------------------------------------------------
# OPC UA Subscription Handler
# ------------------------------------------------------------------------------
class SubHandler:
    """Handle data change and status for one subscription."""

    def __init__(self, producer: KafkaProducer, topic_values: str):
        self.producer = producer
        self.topic_values = topic_values

    def datachange_notification(self, node, val, data):
        try:
            # Build stable NodeId string
            try:
                identifier = node.nodeid.Identifier
                ns = node.nodeid.NamespaceIndex
                node_id_str = f"ns={ns};s={identifier}"
            except Exception:
                node_id_str = str(node.nodeid)

            dv = data.monitored_item.Value

            # Skip bad/null payloads from server, publish health instead
            is_bad = hasattr(dv.StatusCode, "is_bad") and dv.StatusCode.is_bad()
            if is_bad or val is None:
                health = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "type": "opcua_bad_value",
                    "node_id": node_id_str,
                    "status_code": int(dv.StatusCode.value)
                    if hasattr(dv.StatusCode, "value") else dv.StatusCode,
                    "source_timestamp": dv.SourceTimestamp.isoformat() if dv.SourceTimestamp else None,
                    "server_timestamp": dv.ServerTimestamp.isoformat() if dv.ServerTimestamp else None,
                }
                self.producer.send(self.topic_values, health)
                return

            payload = {
                "timestamp": datetime.utcnow().isoformat(),
                "node_id": node_id_str,
                "display_name": str(node),
                "value": to_plain_json(val),
                "status_code": int(dv.StatusCode.value)
                if hasattr(dv.StatusCode, "value") else dv.StatusCode,
                "source_timestamp": dv.SourceTimestamp.isoformat() if dv.SourceTimestamp else None,
                "server_timestamp": dv.ServerTimestamp.isoformat() if dv.ServerTimestamp else None,
            }
            self.producer.send(self.topic_values, payload)
        except Exception as e:
            logger.error(f"Error publishing message to Kafka: {e}")

    def status_change_notification(self, status: ua.StatusChangeNotification):
        try:
            diag = {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "opcua_subscription_status",
                "status": to_plain_json(status),
            }
            self.producer.send(self.topic_values, diag)
            logger.warning(f"Subscription status changed: {status}")
        except Exception as e:
            logger.error(f"Error handling status change: {e}")

    def event_notification(self, event: ua.EventNotificationList):
        # Optionally implement in a separate handler (we publish events elsewhere)
        pass


# ------------------------------------------------------------------------------
# Event Handler (Server events -> Kafka)
# ------------------------------------------------------------------------------
class EventHandler:
    def __init__(self, producer: KafkaProducer, topic_events: str):
        self.producer = producer
        self.topic_events = topic_events

    def event_notification(self, event: ua.EventNotificationList):
        try:
            payload = {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "opcua_event",
                "event": to_plain_json(event),
            }
            self.producer.send(self.topic_events, payload)
        except Exception as e:
            logger.error(f"Error publishing event: {e}")

    def status_change_notification(self, status: ua.StatusChangeNotification):
        # Optional: log status changes for event subscription
        logger.info(f"Event subscription status: {status}")


# ------------------------------------------------------------------------------
# Discovery with filters & caps
# ------------------------------------------------------------------------------
async def discover_nodes(client: Client) -> List[Node]:  # <-- FIXED
    discovered_nodes: List[Node] = []

    async def recurse(node: Node):  # <-- FIXED
        nonlocal discovered_nodes
        if len(discovered_nodes) >= MAX_DISCOVER_NODES:
            return
        for child in await node.get_children():
            if len(discovered_nodes) >= MAX_DISCOVER_NODES:
                break
            node_class = await child.read_node_class()
            ns = child.nodeid.NamespaceIndex
            if NAMESPACE_ALLOWLIST and ns not in NAMESPACE_ALLOWLIST:
                continue
            if node_class == ua.NodeClass.Variable:
                discovered_nodes.append(child)
            elif node_class == ua.NodeClass.Object:
                await recurse(child)

    await recurse(client.nodes.objects)
    logger.info(f"Discovered {len(discovered_nodes)} variable nodes (cap={MAX_DISCOVER_NODES})")
    return discovered_nodes


# ------------------------------------------------------------------------------
# Subscribe a batch with DataChangeFilter & queue settings
# ------------------------------------------------------------------------------
async def subscribe_batch(client: Client, nodes_batch: List[Node],  # <-- FIXED
                          topic_values: str, producer: KafkaProducer):
    handler = SubHandler(producer, topic_values)
    subscription = await client.create_subscription(SUBSCRIPTION_PERIOD_MS, handler)

    # Configure DataChangeFilter
    trigger_map = {
        "StatusValueTimestamp": ua.DataChangeTrigger.StatusValueTimestamp,
        "Status": ua.DataChangeTrigger.Status,
        "Value": ua.DataChangeTrigger.Value,
    }
    trigger = trigger_map.get(TRIGGER_MODE, ua.DataChangeTrigger.StatusValueTimestamp)
    dcf = ua.DataChangeFilter(Trigger=trigger)

    # Subscribe with small queue to avoid overflow
    subscribed = 0
    for node in nodes_batch:
        try:
            mi_params = ua.MonitoringParameters()
            mi_params.QueueSize = QUEUE_SIZE
            await subscription._subscribe(node, ua.AttributeIds.Value, mfilter=dcf, parameters=mi_params)
            subscribed += 1
        except Exception as e:
            logger.error(f"Failed to subscribe node {node}: {e}")

    logger.info(f"Subscription created for {subscribed}/{len(nodes_batch)} nodes; period={SUBSCRIPTION_PERIOD_MS}ms")
    return subscription


# ------------------------------------------------------------------------------
# Main Connector Logic
# ------------------------------------------------------------------------------
async def main():
    nodes_config = load_nodes_config()
    producer = create_kafka_producer()

    client = Client(url=OPCUA_ENDPOINT, timeout=OPCUA_CLIENT_TIMEOUT_SEC)

    # Security
    sec_mode = OPCUA_SECURITY_MODE.strip().lower()
    sec_policy = OPCUA_SECURITY_POLICY.strip().lower()
    if sec_mode != "none" and sec_policy != "none":
        cert_path = "./config/client_cert.der"
        key_path = "./config/client_key.pem"
        if os.path.exists(cert_path) and os.path.exists(key_path):
            try:
                client.set_security(
                    getattr(ua.SecurityPolicyType, OPCUA_SECURITY_POLICY),
                    getattr(ua.MessageSecurityMode, OPCUA_SECURITY_MODE),
                    certificate=cert_path,
                    private_key=key_path,
                )
                logger.info(f"Using OPC UA security: {OPCUA_SECURITY_POLICY}/{OPCUA_SECURITY_MODE}")
            except Exception as e:
                logger.warning(f"Failed to set security; falling back to none: {e}")
        else:
            logger.warning("Security enabled but cert/key missing; using none.")

    # Auth
    if OPCUA_USERNAME and OPCUA_PASSWORD:
        try:
            client.set_user_string(OPCUA_USERNAME, OPCUA_PASSWORD)
        except Exception as e:
            logger.warning(f"Failed to set OPC UA user credentials: {e}")

    async with client:
        logger.info(f"Connected to OPC UA server {OPCUA_ENDPOINT}")

        # Load type definitions for ExtensionObjects
        try:
            await client.load_data_type_definitions()
            logger.info("Loaded OPC UA type definitions")
        except Exception as e:
            logger.warning(f"Could not load type definitions: {e}")

        # Determine nodes
        if nodes_config:
            nodes: List[Node] = []
            for node_cfg in nodes_config:
                try:
                    nodes.append(client.get_node(node_cfg["node_id"]))
                except Exception as e:
                    logger.error(f"Invalid node in config {node_cfg}: {e}")
            logger.info(f"Monitoring {len(nodes)} nodes from config")
        else:
            nodes = await discover_nodes(client)
            if not nodes:
                logger.warning("No nodes found to monitor. Exiting.")
                return

        # Shard nodes across multiple subscriptions
        batches = chunk_list(nodes, SUBSCRIPTION_BATCH_SIZE)
        subscriptions = []
        for idx, batch in enumerate(batches, 1):
            sub = await subscribe_batch(client, batch, KAFKA_TOPIC_VALUES, producer)
            subscriptions.append(sub)
            logger.info(f"Subscribed batch {idx}/{len(batches)} (size={len(batch)})")

        # Subscribe to server events (optional)
        try:
            ev_handler = EventHandler(producer, KAFKA_TOPIC_EVENTS)
            ev_sub = await client.create_subscription(SUBSCRIPTION_PERIOD_MS, ev_handler)
            await ev_sub.subscribe_events(client.nodes.server)
            logger.info("Subscribed to Server events")
        except Exception as e:
            logger.warning(f"Event subscription skipped: {e}")

        # Heartbeat task
        async def heartbeat():
            while True:
                hb = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "type": "connector_heartbeat",
                    "endpoint": OPCUA_ENDPOINT,
                    "sub_count": len(subscriptions),
                    "batch_size": SUBSCRIPTION_BATCH_SIZE,
                }
                try:
                    producer.send(KAFKA_TOPIC_VALUES, hb)
                except Exception as e:
                    logger.error(f"Heartbeat publish failed: {e}")
                await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)

        # Watchdog: reconnect/resubscribe
        async def watchdog():
            while True:
                try:
                    await client.check_connection()
                except Exception as e:
                    logger.warning(f"Connection issue detected: {e}. Recreating subscriptions...")
                    try:
                        for s in subscriptions:
                            try:
                                await s.delete()
                            except Exception:
                                pass
                        subscriptions.clear()
                        for idx, batch in enumerate(batches, 1):
                            sub = await subscribe_batch(client, batch, KAFKA_TOPIC_VALUES, producer)
                            subscriptions.append(sub)
                            logger.info(f"[rebuild] batch {idx}/{len(batches)} resubscribed")
                    except Exception as ee:
                        logger.error(f"Resubscribe failed: {ee}")
                await asyncio.sleep(2)

        # Optional periodic re-discovery
        async def periodic_rediscover():
            if AUTO_REDISCOVER_INTERVAL_SEC <= 0:
                return
            while True:
                await asyncio.sleep(AUTO_REDISCOVER_INTERVAL_SEC)
                try:
                    latest = await discover_nodes(client)
                    existing_ids = {str(n.nodeid) for n in nodes}
                    new_nodes = [n for n in latest if str(n.nodeid) not in existing_ids]
                    if new_nodes:
                        logger.info(f"Discovered {len(new_nodes)} NEW nodes; subscribing...")
                        nodes.extend(new_nodes)
                        new_batches = chunk_list(new_nodes, SUBSCRIPTION_BATCH_SIZE)
                        for nb in new_batches:
                            sub = await subscribe_batch(client, nb, KAFKA_TOPIC_VALUES, producer)
                            subscriptions.append(sub)
                    else:
                        logger.info("No new nodes found.")
                except Exception as e:
                    logger.warning(f"Periodic rediscover failed: {e}")

        # Graceful shutdown
        stop_event = asyncio.Event()

        def stop_handler(*_):
            logger.info("Shutdown signal received. Stopping connector...")
            stop_event.set()

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_handler)
            except NotImplementedError:
                pass

        tasks = [
            asyncio.create_task(heartbeat()),
            asyncio.create_task(watchdog()),
            asyncio.create_task(periodic_rediscover()),
        ]
        await stop_event.wait()

        # Cleanup
        logger.info("Closing OPC UA connection...")
        for t in tasks:
            t.cancel()
        try:
            for s in subscriptions:
                try:
                    await s.delete()
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"Cleanup error: {e}")
        try:
            producer.close()
        except Exception:
            pass
        logger.info("Connector stopped cleanly.")


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
    except Exception as e:
        logger.exception(f"Unhandled error: {e}")
        sys.exit(1)
