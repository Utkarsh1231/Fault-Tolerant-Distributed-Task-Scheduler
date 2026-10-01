"""
hybrid_node.py
==============================================================================
Unified Distributed Fault-Tolerant Task Scheduler
  = Efficient Bully-Raft Leader Election  (control plane, full mesh)
  + Chandy-Lamport Global Snapshot         (data plane, star per leader)
  + Durable snapshot replication + restore (closes the recovery loop)

Every node runs the SAME program. A node is a FOLLOWER (acts as a worker) by
default and promotes itself to LEADER (acts as the scheduler master) when it
wins an election. When a leader dies, the survivors elect a new leader, and the
new leader RESTORES the last consistent snapshot and resumes scheduling — so
in-flight work is not lost across a master failover.

Two planes:
  * CONTROL PLANE  (election_port): ephemeral, framed messages between peers.
      ELECTION / OK / YOU_ARE_LEADER / COORDINATOR / HEARTBEAT / SNAPSHOT_STORE
      -> detects LEADER failure, elects a new one, replicates snapshots.
  * DATA PLANE     (data_port): persistent worker<->leader connections.
      task / result / heartbeat / heartbeat_ack / marker / state_report
      -> assigns tasks, detects WORKER failure, records global snapshots.

Run (3 terminals):
    python hybrid_node.py 1
    python hybrid_node.py 2
    python hybrid_node.py 3

Then kill the leader's terminal (Ctrl-C) and watch a new leader take over,
restore the snapshot, and keep going. Revive it and the highest-ID node
reclaims leadership (Bully precedence).
==============================================================================
"""

import socket
import threading
import pickle
import time
import sys
import random
import uuid
import json
import queue

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
ALL_NODES = {
    1: {"host": "localhost", "election_port": 5001, "data_port": 6001},
    2: {"host": "localhost", "election_port": 5002, "data_port": 6002},
    3: {"host": "localhost", "election_port": 5003, "data_port": 6003},
    # Add more nodes here to test the O(n) election, e.g.:
    # 4: {"host": "localhost", "election_port": 5004, "data_port": 6004},
}

# --- Election (control plane) ---
ELECTION_TIMEOUT_MIN = 3.0
ELECTION_TIMEOUT_MAX = 5.0
CONTROL_HEARTBEAT_INTERVAL = 1.0     # leader -> peers (leader liveness)
MANAGER_REPLY_TIMEOUT = 0.5          # manager waits this long for OK replies

# --- Scheduling (data plane) ---
WORKER_HEARTBEAT_INTERVAL = 2.0      # worker -> leader (worker liveness)
WORKER_HEARTBEAT_TIMEOUT = 6.0       # leader declares a silent worker dead
SNAPSHOT_INTERVAL = 15.0             # leader initiates a snapshot this often
TASK_DURATION = 8                    # seconds each sleep-task runs
MAX_RETRIES = 3
NUM_TASKS = 10                       # seeded once, by the very first leader


# --------------------------------------------------------------------------- #
# Length-framed send/recv for the control plane (snapshots can exceed 4 KB)
# --------------------------------------------------------------------------- #
def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def _send_framed(sock, obj):
    data = pickle.dumps(obj)
    sock.sendall(len(data).to_bytes(4, "big") + data)


def _recv_framed(sock):
    hdr = _recv_exact(sock, 4)
    if not hdr:
        return None
    n = int.from_bytes(hdr, "big")
    body = _recv_exact(sock, n)
    if body is None:
        return None
    return pickle.loads(body)


# --------------------------------------------------------------------------- #
# The unified node
# --------------------------------------------------------------------------- #
class HybridNode:
    def __init__(self, node_id):
        self.node_id = node_id
        cfg = ALL_NODES[node_id]
        self.host = cfg["host"]
        self.election_port = cfg["election_port"]
        self.data_port = cfg["data_port"]

        # ---- Election / Raft state ----
        self.state = "FOLLOWER"          # FOLLOWER | CANDIDATE | LEADER
        self.current_term = 0
        self.current_leader_id = None
        self.last_heartbeat_time = time.time()
        self.election_replies = {}       # {term: [ids]}
        self.lock = threading.RLock()

        # loop guards so we never spawn duplicate monitor/heartbeat threads
        self._monitor_running = False
        self._ctrl_hb_running = False

        # ---- Durable snapshot store (replicated to every node) ----
        self.latest_snapshot = None      # most recent COMPLETED snapshot

        # ---- Scheduler / master state (meaningful only while LEADER) ----
        self.sched_lock = threading.RLock()
        self.is_leader = False
        self.data_server = None          # listening socket for the data plane
        self.task_queue = queue.Queue()
        self.workers = {}                # wid -> {conn, last_heartbeat, assigned_tasks, send_lock}
        self.worker_list = []
        self.rr_index = 0
        self.task_attempts = {}
        self.snapshots = {}
        self.active_snapshot = {}

        # ---- Local worker-agent state (this node executing a task) ----
        self._wk_state_lock = threading.Lock()
        self._wk_current_task = None
        self._wk_is_processing = False
        # serialize ALL worker->leader sends on the shared data socket
        # (heartbeat / result / state_report run on different threads)
        self._wk_send_lock = threading.Lock()

    def _wk_send(self, conn, message):
        try:
            with self._wk_send_lock:
                _send_framed(conn, message)
            return True
        except Exception:
            return False

        print(f"[Node {self.node_id}] Starting up. State: FOLLOWER, Term: 0")

    # ======================================================================= #
    # CONTROL PLANE  (election + snapshot replication)
    # ======================================================================= #
    def run_control_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.election_port))
        srv.listen()
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle_control_connection,
                             args=(conn,), daemon=True).start()

    def handle_control_connection(self, conn):
        should_start_election = False
        become_leader_now = False
        try:
            msg = _recv_framed(conn)
            if msg is None:
                return
            msg_type = msg.get("type")
            sender_term = msg.get("term", 0)
            sender_id = msg.get("sender_id")

            with self.lock:
                # --- Rule 1 (Raft guard): reject stale terms ---
                if sender_term < self.current_term:
                    return
                # --- Rule 2 (Raft guard): adopt future terms ---
                if sender_term > self.current_term:
                    print(f"[Node {self.node_id}] Received new term {sender_term} "
                          f"from {sender_id}. Becoming FOLLOWER.")
                    self.become_follower(sender_term)

                # --- Message handling (now in the same term) ---
                if msg_type == "ELECTION":
                    # Manager-based election: just reply OK, do NOT cascade.
                    print(f"[Node {self.node_id}] Received ELECTION from "
                          f"{sender_id}. Replying OK.")
                    reply = {"type": "OK", "term": self.current_term,
                             "sender_id": self.node_id}
                    try:
                        _send_framed(conn, reply)
                    except Exception:
                        pass

                elif msg_type == "OK":
                    if self.state == "CANDIDATE" and sender_term == self.current_term:
                        self.election_replies.setdefault(sender_term, []).append(sender_id)

                elif msg_type == "YOU_ARE_LEADER":
                    print(f"[Node {self.node_id}] Appointed LEADER by {sender_id} "
                          f"for Term {self.current_term}")
                    become_leader_now = True

                elif msg_type == "HEARTBEAT":
                    if sender_id < self.node_id:
                        print(f"[Node {self.node_id}] Bully violation! {sender_id} "
                              f"(lower ID) claims leadership. Challenging.")
                        should_start_election = True
                    else:
                        self.last_heartbeat_time = time.time()
                        self.current_leader_id = sender_id
                        if self.state != "FOLLOWER":
                            self.become_follower(sender_term)

                elif msg_type == "COORDINATOR":
                    if sender_id < self.node_id:
                        print(f"[Node {self.node_id}] Bully violation! {sender_id} "
                              f"(lower ID) claims leadership. Challenging.")
                        should_start_election = True
                    else:
                        print(f"[Node {self.node_id}] New leader is {sender_id} "
                              f"for Term {self.current_term}.")
                        self.current_leader_id = sender_id
                        self.last_heartbeat_time = time.time()
                        if self.state != "FOLLOWER":
                            self.become_follower(self.current_term)

                elif msg_type == "SNAPSHOT_STORE":
                    # Durable replication: every node keeps the newest snapshot
                    # so any future leader can restore from it.
                    snap = msg.get("snapshot")
                    if snap is not None:
                        self.latest_snapshot = snap
                        print(f"[Node {self.node_id}] Stored replicated snapshot "
                              f"{snap['id']} (durable).")
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

        # Act on flags OUTSIDE the lock to avoid deadlock
        if become_leader_now:
            self._promote_to_leader()
        if should_start_election:
            self.start_election()

    def send_control_message(self, host, port, message, expect_reply=False):
        """Ephemeral connection per message (Bully-style)."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                s.connect((host, port))
                _send_framed(s, message)
                if expect_reply:
                    return _recv_framed(s)
        except Exception:
            return None

    def broadcast_control(self, message):
        for nid, cfg in ALL_NODES.items():
            if nid != self.node_id:
                self.send_control_message(cfg["host"], cfg["election_port"], message)

    # ----------------------- Efficient O(n) election ----------------------- #
    def start_election(self):
        with self.lock:
            if self.state in ("LEADER", "CANDIDATE"):
                return
            self.state = "CANDIDATE"
            self.current_term += 1
            self.current_leader_id = None
            self.last_heartbeat_time = time.time()
            self.election_replies[self.current_term] = []
            term = self.current_term
            print(f"[Node {self.node_id}] Starting ELECTION for Term {term} "
                  f"(acting as manager)")
            election_msg = {"type": "ELECTION", "term": term,
                            "sender_id": self.node_id}

        # Send to ALL peers (manager collects replies) — outside the lock
        for nid, cfg in ALL_NODES.items():
            if nid != self.node_id:
                reply = self.send_control_message(
                    cfg["host"], cfg["election_port"], election_msg,
                    expect_reply=True)
                if reply and reply.get("type") == "OK" \
                        and reply.get("term") == term:
                    with self.lock:
                        self.election_replies.setdefault(term, []).append(
                            reply["sender_id"])

        time.sleep(MANAGER_REPLY_TIMEOUT)

        appoint = None
        with self.lock:
            if self.state != "CANDIDATE" or self.current_term != term:
                return  # overruled by a higher term
            replies = self.election_replies.get(term, [])
            all_candidates = replies + [self.node_id]
            new_leader_id = max(all_candidates)
            print(f"[Node {self.node_id}] Election complete. Candidates: "
                  f"{sorted(set(all_candidates))}. Appointing {new_leader_id}.")
            if new_leader_id == self.node_id:
                win = True
            else:
                win = False
                appoint = new_leader_id
                self.become_follower(term)

        if win:
            self._promote_to_leader()
        elif appoint is not None:
            cfg = ALL_NODES[appoint]
            self.send_control_message(cfg["host"], cfg["election_port"],
                                      {"type": "YOU_ARE_LEADER", "term": term,
                                       "sender_id": self.node_id})

    # --------------------------- State transitions ------------------------- #
    def _promote_to_leader(self):
        with self.lock:
            if self.state == "LEADER":
                return
            self.state = "LEADER"
            self.current_leader_id = self.node_id
            term = self.current_term
            print(f"[Node {self.node_id}] --- Became LEADER for Term {term} ---")
        # Heavy lifting off the lock
        threading.Thread(target=self._leader_startup, args=(term,),
                         daemon=True).start()

    def _leader_startup(self, term):
        # 1) Restore prior work (or seed if we are the very first leader)
        self._init_scheduler_state()
        # 2) Bring up the data-plane scheduler
        self.start_scheduler()
        # 3) Announce leadership
        self.broadcast_control({"type": "COORDINATOR", "term": term,
                                "sender_id": self.node_id})
        # 4) Start control heartbeats (leader liveness)
        if not self._ctrl_hb_running:
            self._ctrl_hb_running = True
            threading.Thread(target=self.send_control_heartbeats,
                             daemon=True).start()

    def become_follower(self, term):
        """MUST be called while holding self.lock."""
        was_leader = self.state == "LEADER"
        self.state = "FOLLOWER"
        self.current_term = term
        if was_leader:
            # Step down: tear down the scheduler so workers reconnect elsewhere
            threading.Thread(target=self.stop_scheduler, daemon=True).start()
        if not self._monitor_running:
            self._monitor_running = True
            threading.Thread(target=self.monitor_leader, daemon=True).start()

    def send_control_heartbeats(self):
        while True:
            with self.lock:
                if self.state != "LEADER":
                    self._ctrl_hb_running = False
                    break
                term = self.current_term
            print(f"[Node {self.node_id}] Sending control heartbeats "
                  f"for Term {term}")
            self.broadcast_control({"type": "HEARTBEAT", "term": term,
                                    "sender_id": self.node_id})
            time.sleep(CONTROL_HEARTBEAT_INTERVAL)

    def monitor_leader(self):
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        while True:
            fire = False
            with self.lock:
                if self.state != "FOLLOWER":
                    self._monitor_running = False
                    break
                if time.time() - self.last_heartbeat_time > timeout:
                    print(f"[Node {self.node_id}] Leader timeout! "
                          f"(last heard "
                          f"{time.time() - self.last_heartbeat_time:.2f}s ago)")
                    fire = True
            if fire:
                self._monitor_running = False
                self.start_election()
                break
            time.sleep(0.1)

    # ======================================================================= #
    # DATA PLANE  (scheduler master — only active while LEADER)
    # ======================================================================= #
    def _init_scheduler_state(self):
        """Restore from the durable snapshot, or seed the initial workload."""
        with self.sched_lock:
            self.task_queue = queue.Queue()
            self.workers = {}
            self.worker_list = []
            self.rr_index = 0
            self.task_attempts = {}
            self.active_snapshot = {}

            if self.latest_snapshot is not None:
                self._restore_from_snapshot(self.latest_snapshot)
            else:
                for i in range(NUM_TASKS):
                    self.task_queue.put({"id": i, "type": "sleep",
                                         "duration": TASK_DURATION})
                    self.task_attempts[i] = 0
                print(f"[Node {self.node_id}] No snapshot found — seeded "
                      f"{NUM_TASKS} fresh tasks.")

    def _restore_from_snapshot(self, snap):
        ms = snap["master_state"]
        restored = 0
        # tasks that were still queued at snapshot time
        for task in ms.get("task_queue", []):
            self.task_queue.put(task)
            self.task_attempts.setdefault(task["id"], 0)
            restored += 1
        # tasks that were in-flight (assigned) — reassign them
        for wid, winfo in ms.get("workers", {}).items():
            for tid in winfo.get("assigned_tasks", []):
                self.task_queue.put({"id": tid, "type": "sleep",
                                     "duration": TASK_DURATION})
                self.task_attempts.setdefault(tid, 0)
                restored += 1
        print(f"[Node {self.node_id}] RESTORED {restored} tasks from snapshot "
              f"{snap['id']} — resuming.")

    def start_scheduler(self):
        with self.sched_lock:
            if self.is_leader:
                return
            self.is_leader = True
        threading.Thread(target=self.run_data_server, daemon=True).start()
        threading.Thread(target=self.assign_tasks, daemon=True).start()
        threading.Thread(target=self.monitor_workers, daemon=True).start()
        threading.Thread(target=self.snapshot_controller, daemon=True).start()
        print(f"[Node {self.node_id}] Scheduler ONLINE on data port "
              f"{self.data_port}.")

    def stop_scheduler(self):
        with self.sched_lock:
            if not self.is_leader:
                return
            self.is_leader = False
            srv = self.data_server
            self.data_server = None
            wl = list(self.workers.values())
            self.workers = {}
            self.worker_list = []
        # close listening socket + all worker conns; their loops will exit
        if srv:
            try:
                srv.close()
            except Exception:
                pass
        for info in wl:
            try:
                info["conn"].close()
            except Exception:
                pass
        print(f"[Node {self.node_id}] Scheduler OFFLINE (stepped down).")

    def run_data_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind((self.host, self.data_port))
            srv.listen()
        except Exception as e:
            print(f"[Node {self.node_id}] Could not bind data port: {e}")
            return
        with self.sched_lock:
            self.data_server = srv
        while True:
            try:
                conn, addr = srv.accept()
            except Exception:
                break  # socket closed on step-down
            with self.sched_lock:
                if not self.is_leader:
                    conn.close()
                    break
            threading.Thread(target=self.handle_worker,
                             args=(conn, addr), daemon=True).start()

    def _worker_send(self, wid, message):
        """Serialize all sends to one worker (heartbeat/task/marker threads)."""
        with self.sched_lock:
            info = self.workers.get(wid)
        if not info:
            return False
        try:
            with info["send_lock"]:
                _send_framed(info["conn"], message)
            return True
        except Exception:
            return False

    def handle_worker(self, conn, addr):
        try:
            reg = _recv_framed(conn)
            worker_id = reg["worker_id"]
        except Exception:
            conn.close()
            return
        with self.sched_lock:
            self.workers[worker_id] = {
                "conn": conn,
                "last_heartbeat": time.time(),
                "assigned_tasks": [],
                "send_lock": threading.Lock(),
            }
            if worker_id not in self.worker_list:
                self.worker_list.append(worker_id)
        print(f"[Master {self.node_id}] Worker {worker_id} connected from {addr}")

        while True:
            with self.sched_lock:
                if not self.is_leader:
                    break
            try:
                msg = _recv_framed(conn)
                if msg is None:
                    break

                if msg.get("heartbeat"):
                    with self.sched_lock:
                        if worker_id in self.workers:
                            self.workers[worker_id]["last_heartbeat"] = time.time()
                    self._worker_send(worker_id, {"heartbeat_ack": True})

                elif msg.get("result") is not None:
                    task_id = msg.get("task_id")
                    print(f"[Master {self.node_id}] Result from {worker_id}: "
                          f"{msg['result']}")
                    with self.sched_lock:
                        if worker_id in self.workers and \
                                task_id in self.workers[worker_id]["assigned_tasks"]:
                            self.workers[worker_id]["assigned_tasks"].remove(task_id)

                elif msg.get("type") == "state_report":
                    self._collect_state_report(msg)

            except Exception:
                break

        print(f"[Master {self.node_id}] Worker {worker_id} disconnected")
        self.cleanup_worker(worker_id, reassign=True)

    def assign_tasks(self):
        while True:
            with self.sched_lock:
                if not self.is_leader:
                    break
                ready = (not self.task_queue.empty()) and bool(self.worker_list)
                if ready:
                    worker_id = self.worker_list[self.rr_index % len(self.worker_list)]
                    self.rr_index += 1
                    task = self.task_queue.get()
                    task_id = task["id"]
                    if worker_id in self.workers:
                        self.workers[worker_id]["assigned_tasks"].append(task_id)
                    else:
                        self.task_queue.put(task)
                        task = None
                else:
                    task = None
                    worker_id = None
            if task is not None:
                if self._worker_send(worker_id, {"task": task}):
                    print(f"[Master {self.node_id}] Assigned Task {task['id']} "
                          f"to {worker_id}")
                else:
                    with self.sched_lock:
                        self.task_queue.put(task)
            time.sleep(1)

    def monitor_workers(self):
        while True:
            with self.sched_lock:
                if not self.is_leader:
                    break
                now = time.time()
                dead = []
                for wid, info in list(self.workers.items()):
                    if now - info["last_heartbeat"] > WORKER_HEARTBEAT_TIMEOUT:
                        print(f"[Master {self.node_id}] Worker {wid} missed "
                              f"heartbeat -> reassigning its tasks")
                        dead.append(wid)
            for wid in dead:
                self.cleanup_worker(wid, reassign=True)
            time.sleep(2)

    def cleanup_worker(self, worker_id, reassign=False):
        with self.sched_lock:
            info = self.workers.pop(worker_id, None)
            if worker_id in self.worker_list:
                self.worker_list.remove(worker_id)
            if info and reassign:
                for t in info["assigned_tasks"]:
                    if self.task_attempts.get(t, 0) < MAX_RETRIES:
                        self.task_attempts[t] = self.task_attempts.get(t, 0) + 1
                        self.task_queue.put({"id": t, "type": "sleep",
                                             "duration": TASK_DURATION})
                        print(f"[Master {self.node_id}] Re-queued Task {t} from "
                              f"{worker_id} (Attempt {self.task_attempts[t]})")
                    else:
                        print(f"[Master {self.node_id}] Task {t} permanently "
                              f"failed after {MAX_RETRIES} retries")
        if info:
            print(f"[Master {self.node_id}] Cleaned up {worker_id}")

    # ----------------------- Chandy-Lamport snapshot ----------------------- #
    def snapshot_controller(self):
        while True:
            time.sleep(SNAPSHOT_INTERVAL)
            with self.sched_lock:
                if not self.is_leader:
                    break
            self.initiate_snapshot()

    def initiate_snapshot(self):
        with self.sched_lock:
            if not self.is_leader or not self.workers:
                return
            snapshot_id = f"snap_{uuid.uuid4()}"
            print(f"\n[Master {self.node_id}] Initiating snapshot {snapshot_id}")
            master_state = {
                "task_queue": list(self.task_queue.queue),
                "workers": {w: {"assigned_tasks": list(i["assigned_tasks"])}
                            for w, i in self.workers.items()},
            }
            self.active_snapshot = {
                "id": snapshot_id,
                "term": self.current_term,
                "master_state": master_state,
                "worker_states": {},
                "pending_workers": list(self.workers.keys()),
            }
            targets = list(self.workers.keys())

        # Marker-Sending Rule: one marker on each outgoing channel
        for wid in targets:
            self._worker_send(wid, {"type": "marker", "snapshot_id": snapshot_id})

    def _collect_state_report(self, msg):
        snapshot_id = msg.get("snapshot_id")
        rpt = msg.get("worker_id")
        completed = None
        with self.sched_lock:
            if self.active_snapshot and self.active_snapshot["id"] == snapshot_id:
                print(f"[Master {self.node_id}] Received state report from {rpt}")
                self.active_snapshot["worker_states"][rpt] = msg.get("state")
                if rpt in self.active_snapshot["pending_workers"]:
                    self.active_snapshot["pending_workers"].remove(rpt)
                if not self.active_snapshot["pending_workers"]:
                    completed = self.active_snapshot.copy()
                    self.snapshots[snapshot_id] = completed
                    self.latest_snapshot = completed  # durable, locally
                    self.active_snapshot = {}
        if completed:
            print(f"[Master {self.node_id}] Snapshot {snapshot_id} complete!")
            print("\n--- GLOBAL SNAPSHOT ---")
            print(json.dumps(completed, indent=2))
            print("-----------------------\n")
            # Replicate to all peers so a future leader can restore
            self.broadcast_control({"type": "SNAPSHOT_STORE",
                                    "term": self.current_term,
                                    "sender_id": self.node_id,
                                    "snapshot": completed})

    # ======================================================================= #
    # WORKER AGENT  (runs while this node is NOT the leader)
    # ======================================================================= #
    def worker_agent_loop(self):
        while True:
            with self.lock:
                leader = self.current_leader_id
                am_leader = self.state == "LEADER"
            if am_leader or leader is None or leader == self.node_id \
                    or leader not in ALL_NODES:
                time.sleep(0.5)
                continue

            cfg = ALL_NODES[leader]
            try:
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                conn.settimeout(3.0)
                conn.connect((cfg["host"], cfg["data_port"]))
                _send_framed(conn, {"worker_id": self.node_id})  # register
                conn.settimeout(1.0)
                print(f"[Worker {self.node_id}] Connected to leader {leader} "
                      f"@ data port {cfg['data_port']}")
                threading.Thread(target=self.send_worker_heartbeat,
                                 args=(conn, leader), daemon=True).start()
                self._listen_tasks(conn, leader)
            except Exception:
                pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            time.sleep(1)

    def send_worker_heartbeat(self, conn, leader):
        while True:
            with self.lock:
                if self.current_leader_id != leader or self.state == "LEADER":
                    break
            if not self._wk_send(conn, {"heartbeat": True,
                                        "worker_id": self.node_id}):
                break
            time.sleep(WORKER_HEARTBEAT_INTERVAL)

    def _listen_tasks(self, conn, leader):
        while True:
            with self.lock:
                if self.current_leader_id != leader or self.state == "LEADER":
                    break
            try:
                # timeout-aware framed read: header may time out (lets us
                # re-check leadership), body is then read blocking.
                hdr = _recv_exact(conn, 4)
                if hdr is None:
                    break
                n = int.from_bytes(hdr, "big")
                conn.settimeout(None)
                body = _recv_exact(conn, n)
                conn.settimeout(1.0)
                if body is None:
                    break
                msg = pickle.loads(body)

                if msg.get("task"):
                    task = msg["task"]
                    print(f"[Worker {self.node_id}] Received Task {task['id']}, "
                          f"running in background.")
                    threading.Thread(target=self._execute_task,
                                     args=(conn, task), daemon=True).start()

                elif msg.get("heartbeat_ack"):
                    pass  # (quietly accepted)

                elif msg.get("type") == "marker":
                    snapshot_id = msg.get("snapshot_id")
                    with self._wk_state_lock:
                        if self._wk_is_processing:
                            local_state = {"status": "Processing",
                                           "task": self._wk_current_task}
                        else:
                            local_state = {"status": "Idle", "task": None}
                    print(f"[Worker {self.node_id}] Marker received. "
                          f"Recorded state: {local_state['status']}")
                    report = {"type": "state_report", "snapshot_id": snapshot_id,
                              "worker_id": self.node_id, "state": local_state}
                    if not self._wk_send(conn, report):
                        break
            except socket.timeout:
                continue
            except Exception:
                break

    def _execute_task(self, conn, task):
        with self._wk_state_lock:
            self._wk_is_processing = True
            self._wk_current_task = task
        result = None
        try:
            if task.get("type") == "sleep":
                dur = task.get("duration", 5)
                print(f"[Worker {self.node_id}] [Task {task['id']}] "
                      f"sleeping {dur}s")
                time.sleep(dur)
                result = f"Task {task['id']} finished sleeping."
        except Exception as e:
            result = str(e)
        if self._wk_send(conn, {"result": result, "task_id": task["id"]}):
            print(f"[Worker {self.node_id}] Completed Task {task['id']}")
        with self._wk_state_lock:
            self._wk_is_processing = False
            self._wk_current_task = None

    # ======================================================================= #
    # Boot
    # ======================================================================= #
    def start(self):
        threading.Thread(target=self.run_control_server, daemon=True).start()
        threading.Thread(target=self.worker_agent_loop, daemon=True).start()
        with self.lock:
            self.become_follower(0)
        while True:
            time.sleep(60)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python hybrid_node.py <node_id>")
        print(f"       node_id must be one of {list(ALL_NODES.keys())}")
        sys.exit(1)
    try:
        nid = int(sys.argv[1])
        if nid not in ALL_NODES:
            raise ValueError
    except ValueError:
        print(f"Error: invalid node_id. Must be one of {list(ALL_NODES.keys())}")
        sys.exit(1)

    HybridNode(nid).start()
