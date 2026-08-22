import asyncio
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import requests
from influxdb_client import InfluxDBClient

# Windows requires SelectorEventLoopPolicy for aiomqtt/paho-mqtt compatibility
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

ROOT = Path(__file__).resolve().parents[1]

# Node 1 keeps Phase 1's port so every existing caller and config is unchanged.
RABBIT_MGMT_PORTS = {1: 15672, 2: 15673, 3: 15674}


def rabbit_api(node: int = 1) -> str:
    return f"http://localhost:{RABBIT_MGMT_PORTS[node]}/api"


RABBIT_API = rabbit_api(1)  # retained: Phase 1/2 modules import this name


def _load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env()

LOCK_PATH = ROOT / ".pytest-stack.lock"


def _pid_alive(pid: int) -> bool:
    """Is this PID a running process?

    os.kill(pid, 0) is the POSIX idiom and is NOT safe here: on Windows, os.kill
    is implemented with TerminateProcess, so probing with signal 0 would kill the
    very process we are asking about. Windows gets tasklist instead.
    """
    if sys.platform == "win32":
        proc = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True, check=False,
        )
        return str(pid) in proc.stdout
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    return True


def acquire_stack_lock(path: Path = LOCK_PATH) -> None:
    """Refuse to share a live stack with another pytest session.

    A lock whose PID is gone is reclaimed rather than reported, so a crashed run
    does not require manual cleanup. If reclamation is ever wrong, the refusal
    message names the file to delete.

    `path` is parameterised so the unit tests can exercise this against a
    throwaway file instead of the lock the live session is holding.
    """
    if path.exists():
        raw = path.read_text(encoding="utf-8").strip()
        try:
            holder = int(raw)
        except ValueError:
            holder = None
        if holder is not None and holder != os.getpid() and _pid_alive(holder):
            raise RuntimeError(
                f"another pytest session (PID {holder}) is using this stack. "
                f"Concurrent runs produced contaminated results in Phase 2 (see ADR-0012 "
                f"and HANDOFF). Wait for it to finish, or delete {path} if you are "
                f"certain it is stale."
            )
    path.write_text(str(os.getpid()), encoding="utf-8")


def release_stack_lock(path: Path = LOCK_PATH) -> None:
    if path.exists():
        try:
            path.unlink()
        except OSError:
            pass


def cluster_mode() -> bool:
    """Is this session running against the 3-node cluster overlay?

    Driven by an environment variable rather than by marker inspection because the
    stack fixture is session-scoped and must choose its compose files before any
    test runs. tests/experiments/test_cluster_preflight.py fails loudly if a
    cluster test runs without it.
    """
    return os.environ.get("IOT_CLUSTER") == "1"


def region_mode() -> bool:
    """Is this session running against the multi-region overlay?

    Same rationale as cluster_mode(): the stack fixture is session-scoped and must
    choose its compose files before any test runs, so this is an environment
    variable rather than marker inspection.
    """
    return os.environ.get("IOT_REGION") == "1"


def security_mode() -> bool:
    """Is this session running against the security overlay?

    Same rationale as cluster_mode() and region_mode(): the stack fixture is
    session-scoped and must choose its compose files before any test runs.
    """
    return os.environ.get("IOT_SECURITY") == "1"


def load_mode() -> bool:
    """Is this session running against the Phase 6 load overlay?

    Same rationale as cluster_mode(), region_mode() and security_mode(): the stack
    fixture is session-scoped and must choose its compose files before any test
    runs, so this is an environment variable rather than marker inspection.
    """
    return os.environ.get("IOT_LOAD") == "1"


def compose_files() -> tuple[str, ...]:
    """The compose file set for this session, in overlay-application order.

    compose.region.yml is always applied before the security files: it re-mounts
    /etc/rabbitmq/rabbitmq.conf and /etc/rabbitmq/definitions.json over whatever
    the cluster overlay put there, and compose resolves same-target mounts by
    last-one-wins. The security files touch neither of those targets, so their
    position is unconstrained; they go last so the -f order reads in phase order.

    compose.region-security.yml exists only for the both-axes-on combination. Its
    region TLS listeners cannot live in conf.d/10-security.conf (under
    base+security those addresses do not exist and the listeners fail to bind)
    nor in compose.region.yml (under region-alone there is no advanced.config, so
    a TLS listener would have no ssl_options and the node would not boot).

    compose.load.yml goes last of all. It re-declares the rabbitmq service purely
    to pin mem_limit/cpus and an absolute memory watermark, so it must win over
    anything an earlier overlay set; compose resolves competing scalar keys by
    last-one-wins. Phase 6 combines it only with the base and cluster profiles.
    """
    files = ("compose.yml",)
    if cluster_mode():
        files += ("compose.cluster.yml",)
    if region_mode():
        files += ("compose.region.yml",)
    if security_mode():
        files += ("compose.security.yml",)
        if region_mode():
            files += ("compose.region-security.yml",)
    if load_mode():
        files += ("compose.load.yml",)
    return files


def consumer_files() -> tuple[str, ...]:
    """The file set for the ack-after-write consumer overlay.

    Derived, never hardcoded. `compose.consumer.yml`'s `consumer` service carries
    `depends_on: rabbitmq (condition: service_healthy)`, so compose evaluates the
    `rabbitmq` service against whatever file set this returns. A hardcoded
    single-node set therefore recreates iot-rabbitmq onto the empty single-node
    volume in the middle of a cluster run — the exact reconcile ADR-0025 proved
    and ADR-0026 filed as bite #16.
    """
    return compose_files() + ("compose.consumer.yml",)


def compose(*args: str, files: tuple[str, ...] = ("compose.yml",),
            check: bool = True) -> subprocess.CompletedProcess:
    """Run `docker compose` against the project, raising with captured output on failure.

    subprocess.CalledProcessError's default __str__ hides stdout/stderr, which makes a
    failed `up --wait` almost undiagnosable from a bare pytest failure. Handoff finding #5.
    """
    file_args: list[str] = []
    for name in files:
        file_args += ["-f", name]
    proc = subprocess.run(
        ["docker", "compose", *file_args, *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"docker compose {' '.join(args)} failed with exit {proc.returncode}\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )
    return proc


_RECREATE = re.compile(r"^\s*Container\s+(\S+)\s+Recreate\b", re.MULTILINE)
_ORPHANS = re.compile(r"Found orphan containers \(\[(.*?)\]\)")
_VOLUME_CREATING = re.compile(r"^\s*Volume\s+(\S+)\s+Creating\b", re.MULTILINE)


def detect_profile_mismatch(dry_run_output: str) -> str | None:
    """Does this `up --dry-run` describe a reconcile rather than a no-op?

    Run against an already-running stack, `up --dry-run` reports what compose
    *would* change. Against the file set the stack was brought up with, that is
    nothing. Against a different set, compose announces exactly the damage
    ADR-0025 diagnosed: peers of the other profile become orphans, the shared
    service is recreated, and it is recreated onto a different, empty volume.

    `Recreate` is the reconcile; `Creating` on a container is a clean first
    bring-up and must never fire this.
    """
    recreated = _RECREATE.findall(dry_run_output)
    orphans = _ORPHANS.findall(dry_run_output)
    volumes = _VOLUME_CREATING.findall(dry_run_output) if recreated else []
    if not recreated and not orphans:
        return None
    parts = []
    if recreated:
        parts.append(f"would recreate {', '.join(recreated)}")
    if orphans:
        parts.append(f"sees orphan containers [{orphans[0]}]")
    if volumes:
        parts.append(f"would create volume(s) {', '.join(volumes)}")
    return "; ".join(parts)


def _fail_on_profile_mismatch() -> None:
    """Refuse to `up` a file set that would reconcile a running stack.

    Gated on `ps -q` so a clean session pays one cheap command and no dry-run.
    COMPOSE_PROFILES is inherited from the environment so profile-gated services
    (the region simulators) are not mistaken for orphans.
    """
    files = compose_files()
    running = compose("ps", "-q", files=files, check=False).stdout.strip()
    if not running:
        return
    # check=False: a mismatched dry-run may exit non-zero, and its output is
    # exactly what we are here to read.
    proc = compose("up", "-d", "--wait", "--dry-run", files=files, check=False)
    message = detect_profile_mismatch(proc.stdout + proc.stderr)
    if message:
        pytest.fail(
            f"compose file set {files} would reconcile the running stack: {message}.\n"
            "Tear the stack down with the file set it was brought up with, plus "
            "--remove-orphans, or re-run with the matching IOT_CLUSTER / IOT_REGION "
            "values. See ADR-0025."
        )


@pytest.fixture(scope="session")
def stack():
    """Bring the compose stack up once per test session.

    Set KEEP_STACK=1 to skip teardown while iterating locally.
    """
    if not (ROOT / ".env").exists():
        pytest.fail("main/.env is missing. Copy .env.example to .env first.")
    if security_mode():
        # certs/ is gitignored, so a clean clone has none and the security
        # overlay's bind mounts would resolve to an empty directory. make_certs
        # is idempotent, so a warm run reuses what is already there and never
        # invalidates a stack that is already running against it.
        from scripts.make_certs import CERTS_DIR, generate_all
        generate_all(CERTS_DIR)
    acquire_stack_lock()
    try:
        _fail_on_profile_mismatch()
        compose("up", "-d", "--wait", files=compose_files())
        yield
    finally:
        if os.environ.get("KEEP_STACK") != "1":
            compose("down", "-v", files=compose_files())
        release_stack_lock()


@pytest.fixture(scope="session")
def rabbit_get_node(stack):
    """Returns a factory: rabbit_get_node(2) is a GET bound to node 2's API."""
    auth = (
        os.environ.get("RABBITMQ_ADMIN_USER", "admin"),
        os.environ.get("RABBITMQ_ADMIN_PASSWORD", "adminpass"),
    )
    session = requests.Session()
    session.auth = auth

    def _for_node(node: int = 1):
        base = rabbit_api(node)

        def _get(path: str) -> requests.Response:
            return session.get(f"{base}{path}", timeout=10)

        return _get

    yield _for_node
    session.close()


@pytest.fixture(scope="session")
def rabbit_get(rabbit_get_node):
    """Node 1's getter. Unchanged contract for every Phase 1/2 caller."""
    return rabbit_get_node(1)


@pytest.fixture(scope="session")
def influx_query(stack):
    token = os.environ.get("INFLUXDB_TOKEN", "dev-token-0123456789abcdef")
    org = os.environ.get("INFLUXDB_ORG", "iot")
    client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)

    def _query(flux: str) -> list[dict]:
        rows: list[dict] = []
        for table in client.query_api().query(flux):
            for record in table.records:
                row = dict(record.values)
                row["_value"] = record.get_value()
                row["_field"] = record.get_field()
                row["_time"] = record.get_time()
                rows.append(row)
        return rows

    yield _query
    client.close()


BUCKET = "telemetry"


def flux_range_start(started_at: datetime) -> str:
    """RFC3339 UTC, one minute before started_at.

    Points carry device-side timestamps (telegraf.conf sets timestamp_path = "ts"),
    so after an outage the drained points land at outage-era times. Verification
    queries must anchor on the run's own start, never on a relative recent window.
    """
    return (started_at - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _field_query(run_id: str, field: str, start: str) -> str:
    return f'''
from(bucket: "{BUCKET}")
  |> range(start: {start})
  |> filter(fn: (r) => r._measurement == "telemetry")
  |> filter(fn: (r) => r.run_id == "{run_id}")
  |> filter(fn: (r) => r._field == "{field}")
'''


def fetch_seqs(influx_query, run_id: str, start: str = "-15m") -> list[dict]:
    return influx_query(_field_query(run_id, "seq", start))


def fetch_values(influx_query, run_id: str, start: str = "-15m") -> list[dict]:
    return influx_query(_field_query(run_id, "value", start))


def query_measurement(influx_query, measurement: str, start: str = "-5m") -> list[dict]:
    flux = f'''
from(bucket: "{BUCKET}")
  |> range(start: {start})
  |> filter(fn: (r) => r._measurement == "{measurement}")
'''
    return influx_query(flux)
