#!/usr/bin/env python3
"""Start the candidate with disposable PostgreSQL and tune a synthetic provider."""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
KEY = "ci-only-admin-key"


def run(*args, **kwargs):
    result = subprocess.run([str(x) for x in args], capture_output=True, text=True,
                            timeout=kwargs.pop("timeout", 60), **kwargs)
    if result.returncode:
        raise RuntimeError(f"{' '.join(map(str, args))}: {result.stdout}\n{result.stderr}")
    return result.stdout.strip()


def eventually(check, description, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            last = str(error)
        time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for {description}: {last}")


def qualify(image, evidence):
    image_id = json.loads(run("docker", "image", "inspect", image))[0]["Id"]
    project = "conductor-ci-" + uuid.uuid4().hex[:12]
    with tempfile.TemporaryDirectory(prefix="conductor-runtime-") as temporary:
        scratch = Path(temporary)
        environment = scratch / ".env"
        environment.write_text("POSTGRES_PASSWORD=ci-only-password\nCONDUCTOR_CRED_KEY=" + "01" * 32 +
                               "\nCONDUCTOR_ADMIN_API_KEY=" + KEY + "\n")
        model = json.loads(run("docker", "compose", "--env-file", environment,
                               "-f", ROOT / "docker-compose.example.yml", "config", "--format", "json"))
        model.pop("name", None)
        model["networks"] = {"default": {}}
        for volume in model.get("volumes", {}).values():
            volume.pop("name", None)
        for service in model["services"].values():
            service.pop("container_name", None)
            service["restart"] = "no"
        app = model["services"]["conductor"]
        app.pop("build", None)
        app["image"] = image
        app["ports"] = [{"target": 8409, "published": "0", "host_ip": "127.0.0.1"}]
        app["environment"]["CONDUCTOR_BASE_URL"] = "http://conductor:8409"
        app["environment"].update({"CONDUCTOR_UPSTREAM_PROXY": "", "CONDUCTOR_CRED_KEY": "01" * 32,
                                   "CONDUCTOR_ADMIN_API_KEY": KEY, "CONDUCTOR_AUTH_ENABLED": "true"})
        model["services"]["provider"] = {
            # Use the packaged FFmpeg to serve continuous, monotonic test
            # video/audio. Looping a short .ts file would reset timestamps and
            # correctly fail Conductor's startup continuity guard.
            "image": image, "entrypoint": ["ffmpeg"], "restart": "on-failure",
            "command": ["-v", "error", "-re", "-f", "lavfi", "-i", "testsrc=size=160x90:rate=25",
                        "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
                        "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency", "-pix_fmt", "yuv420p",
                        "-g", "25", "-c:a", "aac", "-b:a", "128k", "-muxrate", "2000000",
                        "-pcr_period", "20", "-f", "mpegts",
                        "-listen", "1", "http://0.0.0.0:8080/stream.ts"],
        }
        path = scratch / "compose.json"
        path.write_text(json.dumps(model))

        def compose(*args, **kwargs):
            return run("docker", "compose", "-p", project, "-f", path, *args, **kwargs)

        def alive():
            container = compose("ps", "--all", "-q", "conductor")
            details = json.loads(run("docker", "inspect", container))[0]
            if not details["State"]["Running"] or details["Image"] != image_id:
                raise AssertionError(f"Candidate is not running: {details['State']}")

        def request(route, payload=None, admin=False):
            headers = {"Content-Type": "application/json"}
            if admin:
                headers["Authorization"] = "Bearer " + KEY
            req = urllib.request.Request(base + route, None if payload is None else json.dumps(payload).encode(), headers)
            with urllib.request.urlopen(req, timeout=15) as response:
                return json.load(response)

        def ready():
            alive()
            return request("/readyz")

        def tune():
            lineup = request("/lineup.json")
            if len(lineup) != 1 or lineup[0]["GuideName"] != "Runtime Fixture":
                raise AssertionError(f"Persisted channel is absent from the tuner lineup: {lineup}")
            route = urllib.parse.urlsplit(lineup[0]["URL"]).path
            with urllib.request.urlopen(base + route, timeout=30) as response:
                sample = response.read(188 * 500)
            if len(sample) != 188 * 500 or any(sample[i] != 0x47 for i in range(0, len(sample), 188)):
                raise AssertionError("Tuning did not deliver aligned MPEG-TS packets")
            probe = subprocess.run(["docker", "run", "--rm", "-i", "--network", "none", "--entrypoint", "ffprobe", image,
                                    "-v", "error", "-show_entries", "stream=codec_type", "-of", "json", "pipe:0"],
                                   input=sample, capture_output=True, timeout=30, check=True)
            types = {s["codec_type"] for s in json.loads(probe.stdout)["streams"]}
            if not {"video", "audio"} <= types:
                raise AssertionError(f"The tuned stream has no decodable video/audio: {types}")
            eventually(lambda: not request("/admin/streams", admin=True), "stream lease release", timeout=45)
            return {"bytes": len(sample), "codec_types": sorted(types)}

        try:
            compose("up", "-d", timeout=180)
            base = "http://" + compose("port", "conductor", "8409")
            eventually(ready, "migrations and database-backed readiness")
            uid = compose("exec", "-T", "conductor", "grep", "^Uid:", "/proc/1/status").split()[1]
            if uid != "10001":
                raise AssertionError(f"The real entrypoint did not drop privileges: uid={uid}")
            discovery = request("/discover.json")
            if request("/admin/channels", admin=True):
                raise AssertionError("Runtime qualification requires a fresh catalogue")
            try:
                request("/admin/providers")
            except urllib.error.HTTPError as error:
                if error.code != 401:
                    raise
            else:
                raise AssertionError("The packaged server accepted an unauthenticated admin request")
            provider = request("/admin/providers", {"name": "Runtime Fixture", "kind": "m3u_xtream", "base_url": "http://provider:8080"}, True)
            request(f"/admin/providers/{provider['ID']}/credentials", {"username": "ci-user", "password": "ci-only", "max_streams": 2}, True)
            channel = request("/admin/channels", {"number": 1, "name": "Runtime Fixture"}, True)
            request(f"/admin/channels/{channel['ID']}/sources", {"provider_id": provider["ID"], "upstream_url": "http://provider:8080/stream.ts"}, True)
            first = tune()
            print("Fresh migrations, authenticated provisioning, real video/audio and lease release passed", flush=True)
            compose("restart", "conductor")
            base = "http://" + compose("port", "conductor", "8409")
            eventually(ready, "database-backed readiness after restart")
            if request("/discover.json")["DeviceID"] != discovery["DeviceID"]:
                raise AssertionError("The device identity changed across restart")
            second = tune()
            proof = {"image_id": image_id, "uid": uid, "checks": ["fresh migrations", "admin authentication",
                     "provider/credential/channel API writes", "tuner lineup", "real MPEG-TS audio/video",
                     "lease release", "restart identity and encrypted-credential persistence"],
                     "first_tune": first, "after_restart": second}
            (evidence / "runtime.json").write_text(json.dumps(proof, indent=2) + "\n")
            print(json.dumps(proof, indent=2), flush=True)
        finally:
            try:
                (evidence / "compose.log").write_text(compose("logs", "--no-color", timeout=20))
                (evidence / "containers.json").write_text(compose("ps", "--all", "--format", "json"))
            finally:
                compose("down", "--volumes", "--remove-orphans", "--timeout", "5", timeout=60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    qualify(args.image, args.evidence)
