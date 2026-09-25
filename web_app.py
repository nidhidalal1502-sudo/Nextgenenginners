import base64
import json
import os
import signal
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer




# =========================
# WEB SERVER CONFIGURATION
# =========================

HOST = "127.0.0.1"
WEB_PORT = 8090

DATA_ROOT = "web_data"

NODE_CONFIGS = [
    ("node1", HOST, 9001),
    ("node2", HOST, 9002),
    ("node3", HOST, 9003),
    ("node4", HOST, 9004),
]


# =========================
# GLOBAL VARIABLES
# =========================

nodes = {}
addresses = []

ring = None
client = None
membership = None
repair = None


# =========================
# START DISTRIBUTED CLUSTER
# =========================

def start_cluster():
    global addresses, ring, client, membership, repair

    os.makedirs(DATA_ROOT, exist_ok=True)

    # Start all storage nodes
    for node_id, host, port in NODE_CONFIGS:

        data_dir = os.path.join(DATA_ROOT, node_id)

        node = Node(
            node_id,
            host,
            port,
            data_dir
        )

        node.start()

        nodes[node_id] = node

    # Give nodes time to start
    time.sleep(0.3)

    # Create node addresses
    addresses = [
        f"{host}:{port}"
        for _, host, port in NODE_CONFIGS
    ]

    # =========================
    # CONSISTENT HASHING RING
    # =========================

    ring = PlacementRing(addresses)

    # =========================
    # QUORUM CLIENT
    # N = 3
    # W = 2
    # R = 2
    # =========================

    client = QuorumClient(
        ring,
        replication_factor=3,
        write_quorum=2,
        read_quorum=2
    )

    # =========================
    # MEMBERSHIP TRACKER
    # =========================

    membership = src.cluster.membership.MembershipTracker(
        addresses,
        check_interval=1,
        timeout=0.5,
        failure_threshold=2
    )

    membership.start()

    time.sleep(0.4)

    # =========================
    # REPAIR SERVICE
    # =========================

    repair = RepairService(
        ring,
        client,
        replication_factor=3,
        interval=5,
        membership=membership
    )

    # Start automatic repair
    repair.start()


# =========================
# STOP CLUSTER
# =========================

def stop_cluster(*_args):

    global membership, repair

    # Stop repair service
    if repair:
        try:
            repair.stop()
        except Exception:
            pass

    # Stop membership tracker
    if membership:
        try:
            membership.stop()
        except Exception:
            pass

    # Stop all nodes
    for node in list(nodes.values()):
        try:
            node.stop()
        except Exception:
            pass

    raise SystemExit(0)


# =========================
# OBJECT SUMMARY
# =========================

def object_summary():

    all_ids = set()
    per_node = {}

    for node_id, host, port in NODE_CONFIGS:

        addr = f"{host}:{port}"

        ids = repair._list_objects_on_node(addr)

        per_node[node_id] = ids

        all_ids.update(ids)

    return all_ids, per_node


# =========================
# JSON RESPONSE
# =========================

def json_response(handler, code, payload):

    body = json.dumps(payload).encode()

    handler.send_response(code)

    handler.send_header(
        "Content-Type",
        "application/json"
    )

    handler.send_header(
        "Content-Length",
        str(len(body))
    )

    handler.end_headers()

    handler.wfile.write(body)


# =========================
# WEB REQUEST HANDLER
# =========================

class WebHandler(BaseHTTPRequestHandler):

    def log_message(self, *_args):
        pass

    # =========================
    # GET REQUESTS
    # =========================

    def do_GET(self):

        # -------------------------
        # DASHBOARD
        # -------------------------

        if self.path == "/":

            with open("dashboard.html", "rb") as f:
                body = f.read()

            self.send_response(200)

            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8"
            )

            self.send_header(
                "Content-Length",
                str(len(body))
            )

            self.end_headers()

            self.wfile.write(body)

            return

        # -------------------------
        # SYSTEM STATUS
        # -------------------------

        if self.path == "/api/status":

            try:

                all_ids, per_node = object_summary()

                node_data = []

                for node_id, host, port in NODE_CONFIGS:

                    addr = f"{host}:{port}"

                    alive = membership.is_alive(addr)

                    node_data.append({
                        "id": node_id,
                        "address": addr,
                        "alive": alive,
                        "objects": len(
                            per_node[node_id]
                        ),
                    })

                json_response(
                    self,
                    200,
                    {
                        "nodes": node_data,

                        "object_count": len(all_ids),

                        "replication_factor": client.n,

                        "write_quorum": client.w,

                        "read_quorum": client.r,

                        "last_repair": repair.last_report,
                    }
                )

            except Exception as exc:

                json_response(
                    self,
                    500,
                    {
                        "error": str(exc)
                    }
                )

            return

        # -------------------------
        # GET OBJECT
        # -------------------------

        if self.path.startswith("/api/get/"):

            object_id = self.path.split(
                "/api/get/",
                1
            )[1]

            try:

                data = client.get(object_id)

                json_response(
                    self,
                    200,
                    {
                        "object_id": object_id,

                        "data_base64":
                            base64.b64encode(
                                data
                            ).decode(),

                        "size": len(data),
                    }
                )

            except Exception as exc:

                json_response(
                    self,
                    404,
                    {
                        "error": str(exc)
                    }
                )

            return

        # -------------------------
        # OBJECT PLACEMENT
        # -------------------------

        if self.path.startswith(
            "/api/placement/"
        ):

            object_id = self.path.split(
                "/api/placement/",
                1
            )[1]

            replicas = ring.get_preferred_nodes(
                object_id,
                client.n
            )

            json_response(
                self,
                200,
                {
                    "object_id": object_id,

                    "replicas": replicas
                }
            )

            return

        # -------------------------
        # NOT FOUND
        # -------------------------

        json_response(
            self,
            404,
            {
                "error": "not_found"
            }
        )

    # =========================
    # POST REQUESTS
    # =========================

    def do_POST(self):

        length = int(
            self.headers.get(
                "Content-Length",
                0
            )
        )

        raw = self.rfile.read(length)

        try:

            payload = json.loads(
                raw or b"{}"
            )

        except json.JSONDecodeError:

            json_response(
                self,
                400,
                {
                    "error": "invalid_json"
                }
            )

            return

        # =========================
        # PUT / UPLOAD OBJECT
        # =========================

        if self.path == "/api/put":

            try:

                # Base64 data
                if payload.get("data_base64"):

                    data = base64.b64decode(
                        payload["data_base64"]
                    )

                # Normal text
                else:

                    data = payload.get(
                        "text",
                        ""
                    ).encode()

                # Store object
                object_id = client.put(data)

                # Find replicas
                replicas = ring.get_preferred_nodes(
                    object_id,
                    client.n
                )

                json_response(
                    self,
                    200,
                    {
                        "object_id": object_id,

                        "size": len(data),

                        "replicas": replicas
                    }
                )

            except Exception as exc:

                json_response(
                    self,
                    503,
                    {
                        "error": str(exc)
                    }
                )

            return

        # =========================
        # MANUAL REPAIR
        # =========================

        if self.path == "/api/repair":

            try:

                report = repair.run_once()

                json_response(
                    self,
                    200,
                    report
                )

            except Exception as exc:

                json_response(
                    self,
                    500,
                    {
                        "error": str(exc)
                    }
                )

            return

        # =========================
        # NOT FOUND
        # =========================

        json_response(
            self,
            404,
            {
                "error": "not_found"
            }
        )


# =========================
# MAIN
# =========================

if __name__ == "__main__":

    start_cluster()

    signal.signal(
        signal.SIGINT,
        stop_cluster
    )

    signal.signal(
        signal.SIGTERM,
        stop_cluster
    )

    print(
        "Distributed Object Store UI running at "
        "http://127.0.0.1:8090"
    )

    print(
        "Storage nodes: "
        "9001, 9002, 9003, 9004"
    )

    server = ThreadingHTTPServer(
        (HOST, WEB_PORT),
        WebHandler
    )

    try:

        server.serve_forever()

    except KeyboardInterrupt:

        stop_cluster()