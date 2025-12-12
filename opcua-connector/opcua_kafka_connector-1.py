
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OPC UA -> Kafka connector (async) that ALWAYS ingests ALL Variable nodes.

- Ignores any nodes config file; auto-discovers the address space under Objects
  and subscribes to every Variable node found. (Optional namespace allowlist.)
- Publishes uniform JSON payloads to Kafka (shape matches RTDIP pipeline):
  {
    "timestamp": "<ingest UTC ISO8601>",
    "source_timestamp": "<device/source UTC ISO8601 or null>",
    "display_name": "<human-friendly tag>",
    "node_id": "<OPC UA NodeId>",
    "quality": "<StatusCode string>",
    "value": <number|string|boolean>
  }
- Uses asyncua Client with generous timeouts and a modest watchdog to
  avoid overlapping secure-channel renew calls; auto-reconnects with backoff.

Env vars:
  OPCUA_ENDPOINT=opc.tcp://host:port/endpoint
  OPCUA_USERNAME / OPCUA_PASSWORD          (optional)
  OPCUA_SECURITY_POLICY / OPCUA_SECURITY_MODE  (optional; defaults None)
  KAFKA_BOOTSTRAP_SERVERS=kafka:9092
  KAFKA_TOPIC=OPCUA
  LOG_LEVEL=INFO
  SUBSCRIPTION_PERIOD_MS=1000
  OPCUA_CLIENT_TIMEOUT_SEC=30.0
  OPCUA_WATCHDOG_INTERVAL=2.0
  NAMESPACE_ALLOWLIST=""   e.g. "0,2" to only include these namespaces; empty=all
  MAX_DISCOVER_NODES=0     0 = no cap; set >0 to cap number of subscribed variables
"""

import os
import sys
import json
import signal
import asyncio
import logging
import contextlib
from typing import Dict, List, Optional
from datetime import datetime, timezone

from asyncua import Client, ua
from kafka import KafkaProducer

# ----------------------- Logging -----------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("opcua-connector")

# ----------------------- Env -----------------------
OPCUA_ENDPOINT           = os.getenv("OPCUA_ENDPOINT", "opc.tcp://127.0.0.1:4840")
OPCUA_SECURITY_MODE      = os.getenv("OPCUA_SECURITY_MODE", "None")          # "None"|"Sign"|"SignAndEncrypt"
OPCUA_SECURITY_POLICY    = os.getenv("OPCUA_SECURITY_POLICY", "None")        # e.g. "Basic256Sha256"
OPCUA_USERNAME           = os.getenv("OPCUA_USERNAME")
OPCUA_PASSWORD           = os.getenv("OPCUA_PASSWORD")

KAFKA_BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC              = os.getenv("KAFKA_TOPIC", "OPCUA")

SUBSCRIPTION_PERIOD_MS   = int(os.getenv("SUBSCRIPTION_PERIOD_MS", "1000"))
OPCUA_CLIENT_TIMEOUT_SEC = float(os.getenv("OPCUA_CLIENT_TIMEOUT_SEC", "30.0"))
OPCUA_WATCHDOG_INTERVAL  = float(os.getenv("OPCUA_WATCHDOG_INTERVAL", "2.0"))
NAMESPACE_ALLOWLIST_STR  = os.getenv("NAMESPACE_ALLOWLIST", "").strip()
MAX_DISCOVER_NODES       = int(os.getenv("MAX_DISCOVER_NODES", "0"))  # 0 = no cap

NAMESPACE_ALLOWLIST = set()
if NAMESPACE_ALLOWLIST_STR:
    try:
        NAMESPACE_ALLOWLIST = {int(x) for x in NAMESPACE_ALLOWLIST_STR.split(",") if x.strip() != ""}
    except Exception:
        log.warning("Invalid NAMESPACE_ALLOWLIST='%s'; ignoring.", NAMESPACE_ALLOWLIST_STR)

# ----------------------- Kafka Producer -----------------------
def create_kafka_producer() -> KafkaProducer:
    """
    Create a tuned Kafka producer. Note: kafka-python does not accept enable_idempotence,
    so we rely on acks=all + retries + low max_in_flight to preserve ordering and durability.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=20,
        batch_size=524288,                    # ~512 KB
        compression_type="lz4",               # install 'lz4' package; or use 'gzip' without extra dep
        max_in_flight_requests_per_connection=2,
        retries=10,
    )

# ----------------------- Helpers -----------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def variant_to_python(val) -> Optional[object]:
    """Convert OPC UA Variant Value to JSON-friendly Python type."""
    try:
        return val
    except Exception:
        return str(val) if val is not None else None

# ----------------------- Subscription Handler -----------------------
class SubscriptionHandler:
    def __init__(self, producer: KafkaProducer, topic: str, node_display: Dict[str, str]):
        self.producer = producer
        self.topic = topic
        self.node_display = node_display

    def datachange_notification(self, node, val, data):
        try:
            node_id = str(node.nodeid)
            display_name = self.node_display.get(node_id) or node_id

            # Extract source timestamp + quality if present
            src_ts = None
            quality = None
            try:
                dv = data.monitored_item.Value  # DataValue
                if dv.SourceTimestamp:
                    src_ts = dv.SourceTimestamp.replace(tzinfo=timezone.utc)\
                               .isoformat(timespec="milliseconds").replace("+00:00", "Z")
                if dv.StatusCode:
                    quality = str(dv.StatusCode)  # e.g., 'Good'
            except Exception:
                pass

            payload = {
                "timestamp": utc_now_iso(),
                "source_timestamp": src_ts,
                "display_name": display_name,
                "node_id": node_id,
                "quality": quality or "Good",
                "value": variant_to_python(val),
            }
            self.producer.send(self.topic, key=node_id.encode("utf-8"), value=payload)
        except Exception as e:
            log.error("Error in datachange_notification: %s", e)

# ----------------------- Discovery -----------------------
async def discover_variable_nodes(client: Client) -> List[Dict[str, str]]:
    """
    Recursively browse from Objects folder and collect all Variable nodes.
    Optional filters: namespace allowlist, cap number of discovered nodes.
    Returns: list of dicts {"node_id": "...", "display_name": "..."}
    """
    results: List[Dict[str, str]] = []
    try:
        objects = client.nodes.objects
        stack = [objects]
        ns_array = await client.get_namespace_array()  # informative; not strictly required
        log.info("Server namespaces: %s", ns_array)

        while stack:
            parent = stack.pop()
            children = await parent.get_children()
            for child in children:
                try:
                    nodeclass = await child.read_node_class()
                    if nodeclass == ua.NodeClass.Variable:
                        ns_idx = child.nodeid.NamespaceIndex
                        if NAMESPACE_ALLOWLIST and ns_idx not in NAMESPACE_ALLOWLIST:
                            continue
                        dn = await child.read_display_name()
                        text = getattr(dn, "Text", None) or getattr(dn, "text", None) or str(dn)
                        results.append({"node_id": str(child.nodeid), "display_name": text or str(child.nodeid)})
                        if MAX_DISCOVER_NODES and len(results) >= MAX_DISCOVER_NODES:
                            log.warning("Discovery capped at %s nodes.", MAX_DISCOVER_NODES)
                            return results
                    elif nodeclass in (ua.NodeClass.Object, ua.NodeClass.View):
                        stack.append(child)
                except Exception:
                    # skip unreadable children
                    continue
    except Exception as e:
        log.error("Auto-discovery failed: %s", e)

    log.info("Discovered %s variable nodes.", len(results))
    return results

# ----------------------- OPC UA Connector -----------------------
class OPCUAConnector:
    def __init__(self, endpoint: str, nodes_cfg: List[Dict[str, str]]):
        self.endpoint = endpoint
        self.nodes_cfg = nodes_cfg
        self.client: Optional[Client] = None
        self.producer = create_kafka_producer()
        self.health_task: Optional[asyncio.Task] = None
        self.subscription: Optional[ua.Subscription] = None
        self.sub_handler: Optional[SubscriptionHandler] = None
        self._stop = asyncio.Event()

    async def _new_client(self) -> Client:
        client = Client(url=self.endpoint, timeout=OPCUA_CLIENT_TIMEOUT_SEC, watchdog_intervall=OPCUA_WATCHDOG_INTERVAL)

        # Optional username/password
        if OPCUA_USERNAME and OPCUA_PASSWORD:
            client.set_user(OPCUA_USERNAME)
            client.set_password(OPCUA_PASSWORD)

        # Optional security
        if OPCUA_SECURITY_POLICY and OPCUA_SECURITY_POLICY != "None":
            try:
                sec = OPCUA_SECURITY_POLICY
                mode = OPCUA_SECURITY_MODE if OPCUA_SECURITY_MODE != "None" else "SignAndEncrypt"
                client.set_security_string(f"{sec},{mode}")
                log.info("OPC UA security set: policy=%s, mode=%s", sec, mode)
            except Exception as e:
                log.warning("Failed to set OPC UA security (%s). Proceeding without.", e)

        return client

    async def _prefetch_display_names(self, client: Client) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for item in self.nodes_cfg:
            node_id = item.get("node_id")
            if not node_id:
                continue
            disp = item.get("display_name")
            if disp:
                mapping[node_id] = disp
                continue
            try:
                node = client.get_node(node_id)
                dn = await node.read_display_name()
                text = getattr(dn, "Text", None) or getattr(dn, "text", None) or str(dn)
                mapping[node_id] = text or node_id
            except Exception:
                mapping[node_id] = node_id
        return mapping

    async def _create_subscription(self, client: Client):
        node_display = await self._prefetch_display_names(client)
        self.sub_handler = SubscriptionHandler(self.producer, KAFKA_TOPIC, node_display)
        self.subscription = await client.create_subscription(SUBSCRIPTION_PERIOD_MS, self.sub_handler)

        for item in self.nodes_cfg:
            node_id = item.get("node_id")
            if not node_id:
                continue
            try:
                node = client.get_node(node_id)
                await self.subscription.subscribe_data_change(node)
                log.info("Subscribed data change: %s", node_id)
            except Exception as e:
                log.error("Failed to subscribe %s: %s", node_id, e)

    async def _health_probe(self):
        while not self._stop.is_set():
            try:
                if self.client:
                    await self.client.check_connection()  # raises if broken
            except Exception as e:
                log.error("OPC UA health probe failed: %s", e)
                await self._reconnect()
            await asyncio.sleep(5.0)

    async def _disconnect(self):
        try:
            if self.subscription:
                with contextlib.suppress(Exception):
                    await self.subscription.delete()
            self.subscription = None
            self.sub_handler = None

            if self.client:
                with contextlib.suppress(Exception):
                    await self.client.disconnect()
            self.client = None
        except Exception as e:
            log.warning("Error while disconnecting: %s", e)

    async def _reconnect(self):
        await self._disconnect()
        backoff = [1, 2, 5, 10, 20, 30]
        for delay in backoff:
            if self._stop.is_set():
                return
            log.info("Reconnecting OPC UA in %ss…", delay)
            await asyncio.sleep(delay)
            try:
                self.client = await self._new_client()
                await self.client.connect()
                log.info("OPC UA reconnected: %s", OPCUA_ENDPOINT)
                await self._create_subscription(self.client)
                return
            except Exception as e:
                log.error("Reconnect failed (%s)", e)
                continue
        log.error("Reconnect attempts exhausted; will continue retrying in background.")

    async def run(self):
        # Initial connect only to discover address space
        self.client = await self._new_client()
        await self.client.connect()
        log.info("OPC UA connected: %s", OPCUA_ENDPOINT)

        # ALWAYS auto-discover regardless of nodes file
        self.nodes_cfg = await discover_variable_nodes(self.client)
        if not self.nodes_cfg:
            log.error("No Variable nodes discovered. Exiting.")
            await self._disconnect()
            return

        # Create subscription and start health probe
        await self._create_subscription(self.client)
        self.health_task = asyncio.create_task(self._health_probe())

        await self._stop.wait()  # until signaled to stop

        if self.health_task:
            self.health_task.cancel()
            with contextlib.suppress(Exception):
                await self.health_task
        await self._disconnect()

    def stop(self):
        self._stop.set()

# ----------------------- Signal Handling -----------------------
def install_signal_handlers(connector: OPCUAConnector):
    def _handler(signum, frame):
        log.info("Received signal %s; shutting down…", signum)
        connector.stop()
    signal.signal(signal.SIGINT,  _handler)
    signal.signal(signal.SIGTERM, _handler)

# ----------------------- Main -----------------------
async def main():
    connector = OPCUAConnector(OPCUA_ENDPOINT, nodes_cfg=[])
    install_signal_handlers(connector)
    try:
        await connector.run()
    except Exception as e:
        log.error("Fatal OPC UA connector error: %s", e)
    finally:
        with contextlib.suppress(Exception):
            connector.producer.flush()
        log.info("Connector stopped.")

if __name__ == "__main__":
    asyncio.run(main())
