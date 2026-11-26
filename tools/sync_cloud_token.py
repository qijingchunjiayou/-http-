"""Sync the server ingest token locally, or explicitly verify the local token."""
import argparse
import getpass
import hashlib
import json
from pathlib import Path
import shlex
import sys
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tmp" / "ssh_deps"))
import paramiko


class RememberHost(paramiko.MissingHostKeyPolicy):
    def missing_host_key(self, client, hostname, key):
        fingerprint = hashlib.sha256(key.asbytes()).hexdigest()
        print(f"SSH host key SHA256 (hex): {fingerprint}", flush=True)
        client.get_host_keys().add(hostname, key.get_name(), key)
        client.save_host_keys(str(ROOT / "tmp" / "ssh_known_hosts"))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def read_server_token(args):
    password = getpass.getpass("SSH password (not saved): ")
    known_hosts = ROOT / "tmp" / "ssh_known_hosts"
    known_hosts.parent.mkdir(parents=True, exist_ok=True)
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    if known_hosts.exists():
        client.load_host_keys(str(known_hosts))
    client.set_missing_host_key_policy(RememberHost())
    try:
        client.connect(args.host, port=args.port, username=args.user,
                       password=password, timeout=20, auth_timeout=20,
                       banner_timeout=20, look_for_keys=False, allow_agent=False)
        password = None
        with client.open_sftp() as sftp:
            with sftp.open("/etc/glove-imu.env", "r") as remote:
                env_text = remote.read(65536).decode("utf-8-sig")
    finally:
        client.close()
    tokens = []
    for line in env_text.splitlines():
        key, separator, value = line.strip().partition("=")
        if separator and key.strip() == "IMU_TOKEN":
            parts = shlex.split(value, comments=True)
            if len(parts) != 1 or not parts[0]:
                raise ValueError("Server IMU_TOKEN format is invalid")
            tokens.append(parts[0])
    if len(tokens) != 1:
        raise ValueError("Expected exactly one server IMU_TOKEN")
    return tokens[0]


def verify_local_token():
    definitions = {}
    for line in (ROOT / "main" / "cloud_config.h").read_text(encoding="utf-8").splitlines():
        parts = line.split(maxsplit=2)
        if len(parts) == 3 and parts[0] == "#define":
            definitions[parts[1]] = json.loads(parts[2])
    token = definitions["GLOVE_CLOUD_TOKEN"]
    url = "https://yeyouzzz.cn/imu/api/ingest"
    if definitions["GLOVE_CLOUD_URL"] != url:
        raise ValueError("Unexpected ingest URL")
    request = urllib.request.Request(url, data=b"", method="POST", headers={
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
    })
    # An empty body is rejected after authentication and cannot insert frames.
    opener = urllib.request.build_opener(NoRedirect())
    try:
        with opener.open(request, timeout=20) as response:
            status = response.status
    except urllib.error.HTTPError as error:
        status = error.code
        error.close()
    print(f"INGEST_HTTP_STATUS={status}", flush=True)
    if status != 413:
        raise ValueError("Ingest authentication check did not pass")
    print("Ingest authentication verified (token hidden).", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host")
    parser.add_argument("--user", default="root")
    parser.add_argument("--port", type=int, default=22)
    parser.add_argument("--verify-local", action="store_true")
    args = parser.parse_args()
    if args.verify_local:
        verify_local_token()
        return
    if not args.host:
        parser.error("--host is required for read-only server sync")
    token = read_server_token(args)
    url = "https://yeyouzzz.cn/imu/api/ingest"
    header = (
        "#pragma once\n\n"
        "/* Synced from server IMU_TOKEN; verify ingest before flashing. */\n"
        f"#define GLOVE_CLOUD_URL {json.dumps(url)}\n"
        f"#define GLOVE_CLOUD_TOKEN {json.dumps(token)}\n"
    )
    (ROOT / "main" / "cloud_config.h").write_text(header, encoding="utf-8")
    print("Cloud token synchronized locally (value hidden); HTTP verification pending.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Token sync failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
