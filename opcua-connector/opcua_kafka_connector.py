
# opcua_kafka_connector.compressor_only.py
# Auto-restarting OPC UA → Kafka connector focused on:
# Objects → Modbus → PLC_S7_200_SMART → PLC_S7_200_SMART.Compressor
# Restarts from beginning on connection/token errors.
import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Dict, List, Optional

from asyncua import Client, ua
from asyncua.common.node import Node
from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Config (env) ---
OPCUA_ENDPOINT = os.getenv("OPCUA_ENDPOINT", "opc.tcp://172.30.2.55:48080/uOPC/")
OPCUA_SECURITY_MODE = os.getenv("OPCUA_SECURITY_MODE", "None")
OPCUA_SECURITY_POLICY = os.getenv("OPCUA_SECURITY_POLICY", "None")
OPCUA_CLIENT_TIMEOUT_SEC = float(os.getenv("OPCUA_CLIENT_TIMEOUT_SEC", "30.0"))
OPCUA_USERNAME = os.getenv("OPCUA_USERNAME")
OPCUA_PASSWORD = os.getenv("OPCUA_PASSWORD")

# Path filter: Modbus → PLC_S7_200_SMART → PLC_S7_200_SMART.Compressor (namespace = 2)
BROWSE_PATH = [
    ua.QualifiedName("Modbus", 2),
    ua.QualifiedName("PLC_S7_200_SMART", 2),
    ua.QualifiedName("PLC_S7_200_SMART.Compressor", 2),
]
INCLUDE_DIAGNOSTICS = os.getenv("INCLUDE_DIAGNOSTICS", "false").strip().lower() in {"1", "true", "yes"}

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")  # in-cluster
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "OPCUA")
KAFKA_TOPIC_EVENTS = os.getenv("KAFKA_TOPIC_EVENTS", "OPCUA_EVENTS")
KAFKA_COMPRESSION_TYPE = os.getenv("KAFKA_COMPRESSION_TYPE", "lz4")

# Subscription
SUBSCRIPTION_PERIOD_MS = int(os.getenv("SUBSCRIPTION_PERIOD_MS", "1000"))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", "20"))
TRIGGER_MODE = os.getenv("TRIGGER_MODE", "StatusValueTimestamp").strip()

# Enrichment
PRODUCTION_LINE = os.getenv("PRODUCTION_LINE", "Line-1")
EQUIPMENT_NAME = os.getenv("EQUIPMENT_NAME", "Compressor")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("opcua_kafka_connector_compressor")

# --- Helpers ---
def to_plain_json(value):
    try:
        if isinstance(value, ua.DataValue):
            return {
                "value": to_plain_json(value.Value.Value),
                "status": int(value.StatusCode.value) if isinstance(value.StatusCode, ua.StatusCode) else value.StatusCode,
                "source_ts": value.SourceTimestamp.isoformat() if value.SourceTimestamp else None,
                "server_ts": value.ServerTimestamp.isoformat() if value.ServerTimestamp else None,
            }
        if isinstance(value, ua.Variant):
            return to_plain_json(value.Value)
        if isinstance(value, ua.StatusCode):
            return int(value.value)
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                pass
        if isinstance(value, (list, tuple)):
            return [to_plain_json(v) for v in value]
        if isinstance(value, dict):
            return {k: to_plain_json(v) for k, v in value.items()}
        return value
    except Exception:
        return str(value) if value is not None else None

def create_kafka_producer() -> KafkaProducer:
    try:
        def value_serializer(obj):
            return json.dumps(obj, default=to_plain_json).encode("utf-8")
        def key_serializer(key: str):
            return key.encode("utf-8") if isinstance(key, str) else None

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=value_serializer,
            key_serializer=key_serializer,
            acks="all",
            retries=5,
            linger_ms=20,
            compression_type=KAFKA_COMPRESSION_TYPE,
        )
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except KafkaError as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        raise

class SubHandler:
    """
    Data-change subscription handler with status-change handling.
    If we receive a bad status, we trigger a restart via restart_event.
    """
    def __init__(self, producer: KafkaProducer, topic_values: str, display_names: Dict[str, str], restart_event: asyncio.Event):
        self.producer = producer
        self.topic_values = topic_values
        self.display_names = display_names
        self.restart_event = restart_event

    def _node_id_str(self, node: Node) -> str:
        try:
            identifier = node.nodeid.Identifier
            ns = node.nodeid.NamespaceIndex
            return f"ns={ns};s={identifier}"
        except Exception:
            return str(node.nodeid)

    def datachange_notification(self, node, val, data):
        try:
            node_id_str = self._node_id_str(node)
            dv = data.monitored_item.Value
            is_bad = hasattr(dv.StatusCode, "is_bad") and dv.StatusCode.is_bad()
            key = node_id_str

            if is_bad or val is None:
                health = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "type": "opcua_bad_value",
                    "node_id": node_id_str,
                    "equipment_name": EQUIPMENT_NAME,
                    "production_line": PRODUCTION_LINE,
                    "status_code": int(dv.StatusCode.value) if hasattr(dv.StatusCode, "value") else dv.StatusCode,
                    "source_timestamp": dv.SourceTimestamp.isoformat() if dv.SourceTimestamp else None,
                    "server_timestamp": dv.ServerTimestamp.isoformat() if dv.ServerTimestamp else None,
                }
                self.producer.send(self.topic_values, health, key=key)
                # If we consistently get bad values, the channel may be broken; restart will be triggered by status_change_notification
                return

            display_name = self.display_names.get(node_id_str) or str(node)
            payload = {
                "timestamp": datetime.utcnow().isoformat(),
                "node_id": node_id_str,
                "display_name": display_name,
                "equipment_name": EQUIPMENT_NAME,
                "production_line": PRODUCTION_LINE,
                "value": to_plain_json(val),
                "status_code": int(dv.StatusCode.value) if hasattr(dv.StatusCode, "value") else dv.StatusCode,
                "source_timestamp": dv.SourceTimestamp.isoformat() if dv.SourceTimestamp else None,
                "server_timestamp": dv.ServerTimestamp.isoformat() if dv.ServerTimestamp else None,
            }
            self.producer.send(self.topic_values, payload, key=key)
        except Exception as e:
            logger.error(f"Error publishing message to Kafka: {e}")

    def status_change_notification(self, status):
        """
        Called by asyncua when the subscription status changes.
        If status is not Good, trigger a restart.
        """
        try:
            code = None
            if isinstance(status, ua.StatusChangeNotification):
                code = status.Status
            elif hasattr(status, "Status"):
                code = status.Status
            msg = {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "opcua_subscription_status",
                "status": str(code) if code else str(status),
                "equipment_name": EQUIPMENT_NAME,
                "production_line": PRODUCTION_LINE,
            }
            self.producer.send(self.topic_values, msg)

            is_bad = False
            if isinstance(code, ua.StatusCode):
                is_bad = code.is_bad()
            elif hasattr(code, "is_bad"):
                try:
                    is_bad = code.is_bad()
                except Exception:
                    is_bad = False

            if is_bad:
                logger.warning(f"DataChange subscription status is BAD ({code}). Triggering restart.")
                self.restart_event.set()
        except Exception as e:
            logger.error(f"status_change_notification error: {e}")

class EventHandler:
    """
    Event subscription handler WITH status-change handling to avoid missing method logs.
    """
    def __init__(self, producer: KafkaProducer, topic_events: str, restart_event: asyncio.Event):
        self.producer = producer
        self.topic_events = topic_events
        self.restart_event = restart_event

    def event_notification(self, event):
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "type": "opcua_event",
            "event": str(event),
            "equipment_name": EQUIPMENT_NAME,
            "production_line": PRODUCTION_LINE,
        }
        self.producer.send(self.topic_events, payload)

    def status_change_notification(self, status):
        try:
            code = None
            if isinstance(status, ua.StatusChangeNotification):
                code = status.Status
            elif hasattr(status, "Status"):
                code = status.Status
            msg = {
                "timestamp": datetime.utcnow().isoformat(),
                "type": "opcua_event_subscription_status",
                "status": str(code) if code else str(status),
                "equipment_name": EQUIPMENT_NAME,
                "production_line": PRODUCTION_LINE,
            }
            self.producer.send(self.topic_events, msg)

            is_bad = False
            if isinstance(code, ua.StatusCode):
                is_bad = code.is_bad()
            elif hasattr(code, "is_bad"):
                try:
                    is_bad = code.is_bad()
                except Exception:
                    is_bad = False

            if is_bad:
                logger.warning(f"Event subscription status is BAD ({code}). Triggering restart.")
                self.restart_event.set()
        except Exception as e:
            logger.error(f"event status_change_notification error: {e}")

# --- Targeted discovery: ONLY Compressor (ns=2 path) ---
async def get_compressor_node(client: Client) -> Node:
    node = await client.nodes.objects.get_child(BROWSE_PATH)
    return node

async def list_compressor_variables(client: Client, compressor: Node, include_diagnostics: bool) -> List[Node]:
    vars_list: List[Node] = []
    children = await compressor.get_children()
    for ch in children:
        try:
            nclass = await ch.read_node_class()
        except Exception:
            nclass = None

        if nclass == ua.NodeClass.Variable:
            vars_list.append(ch)
        elif include_diagnostics and nclass == ua.NodeClass.Object:
            dn = await ch.read_display_name()
            name = getattr(dn, 'Text', str(dn))
            if 'Diagnostics' in name:
                for dch in await ch.get_children():
                    try:
                        if await dch.read_node_class() == ua.NodeClass.Variable:
                            vars_list.append(dch)
                    except Exception:
                        pass
    return vars_list

# --- One-run function: builds everything and blocks until stopped or error ---
async def run_once(stop_event: asyncio.Event) -> None:
    producer: Optional[KafkaProducer] = None
    client: Optional[Client] = None
    sub = None
    ev_sub = None
    restart_event = asyncio.Event()

    try:
        # Re-create Kafka producer each run to avoid stale sockets
        producer = create_kafka_producer()

        client = Client(url=OPCUA_ENDPOINT, timeout=OPCUA_CLIENT_TIMEOUT_SEC)

        if OPCUA_SECURITY_MODE.lower() != "none" and OPCUA_SECURITY_POLICY.lower() != "none":
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
        if OPCUA_USERNAME and OPCUA_PASSWORD:
            try:
                client.set_user_string(OPCUA_USERNAME, OPCUA_PASSWORD)
            except Exception as e:
                logger.warning(f"Failed to set OPC UA user credentials: {e}")

        async with client:
            logger.info(f"Connected to OPC UA server {OPCUA_ENDPOINT}")
            try:
                await client.load_data_type_definitions()
            except Exception:
                pass

            compressor = await get_compressor_node(client)
            logger.info("Located Compressor node: %s", compressor)

            display_names: Dict[str, str] = {}
            try:
                dn = await compressor.read_display_name()
                display_names[str(compressor.nodeid)] = dn.Text if hasattr(dn, "Text") else str(dn)
            except Exception:
                display_names[str(compressor.nodeid)] = str(compressor)

            variables = await list_compressor_variables(client, compressor, INCLUDE_DIAGNOSTICS)
            logger.info("Subscribing to %d Compressor variables (include_diagnostics=%s)", len(variables), INCLUDE_DIAGNOSTICS)

            for v in variables:
                try:
                    dn = await v.read_display_name()
                    display_names[str(v.nodeid)] = dn.Text if hasattr(dn, "Text") else str(dn)
                except Exception:
                    display_names[str(v.nodeid)] = str(v)

            handler = SubHandler(producer, KAFKA_TOPIC, display_names, restart_event)
            sub = await client.create_subscription(SUBSCRIPTION_PERIOD_MS, handler)

            subscribed = 0
            for node in variables:
                try:
                    await sub.subscribe_data_change(node, queuesize=QUEUE_SIZE)
                    subscribed += 1
                except Exception as e:
                    logger.error("Failed to subscribe %s: %s", node, e)
            logger.info("Subscribed %d/%d Compressor variables", subscribed, len(variables))

            # Optional: events, with status-change handler
            try:
                ev_handler = EventHandler(producer, KAFKA_TOPIC_EVENTS, restart_event)
                ev_sub = await client.create_subscription(SUBSCRIPTION_PERIOD_MS, ev_handler)
                await ev_sub.subscribe_events(client.nodes.server)
                logger.info("Subscribed to Server events")
            except Exception as e:
                logger.warning("Event subscription skipped: %s", e)

            # Wait until either a stop signal or a restart is requested

            # Wait until either a stop signal or a restart is requested
            done = asyncio.Event()

            async def waiter():
                # Create tasks explicitly; asyncio.wait forbids raw coroutines
                stop_task = asyncio.create_task(stop_event.wait())
                restart_task = asyncio.create_task(restart_event.wait())
                try:
                    done_set, pending = await asyncio.wait(
                        {stop_task, restart_task},
                        return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    # Cancel the other task to avoid leaking background waiters
                    for t in (stop_task, restart_task):
                        if not t.done():
                            t.cancel()
                done.set()

            await waiter()
    except (ConnectionError, ua.UaError) as e:
        # Typical for token rollover / secure channel invalid token
        logger.error(f"OPC UA connection error: {e}. Will restart connector.")
    except KafkaError as e:
        logger.error(f"Kafka error: {e}. Will restart connector.")
    except Exception as e:
        logger.exception(f"Unhandled error inside run_once: {e}. Will restart connector.")
    finally:
        # Clean teardown to avoid stale subscriptions and sockets
        try:
            if sub is not None:
                await sub.delete()
        except Exception:
            pass
        try:
            if ev_sub is not None:
                await ev_sub.delete()
        except Exception:
            pass
        try:
            if client is not None:
                # asyncua Client is context-managed above; still ensure closed
                pass
        except Exception:
            pass
        try:
            if producer is not None:
                producer.flush(timeout=5)
                producer.close()
        except Exception:
            pass
        logger.info("Run teardown complete.")

# --- Main restart loop with exponential backoff ---
async def main():
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

    backoff_sec = 1
    max_backoff_sec = 30  # cap the backoff
    attempt = 0

    while not stop_event.is_set():
        attempt += 1
        logger.info(f"Connector run attempt #{attempt} starting...")
        await run_once(stop_event)

        if stop_event.is_set():
            break

        # If we reached here, run_once exited due to error or restart_event.
        logger.info(f"Connector will restart. Backoff {backoff_sec}s.")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=backoff_sec)
        except asyncio.TimeoutError:
            pass

        backoff_sec = min(max_backoff_sec, backoff_sec * 2)  # exponential backoff

    logger.info("Connector stopped cleanly.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
    except Exception as e:
        logger.info("User Defined Error")
