"""
Punto de entrada del benchmark Pub/Sub local.
Los módulos originales de ejecución y análisis se conservan en ./benchmark/.
Este archivo no inventa ni reconstruye resultados; permite consultar y ejecutar
los subcomandos originales de campaña y recolección.
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODULES = ROOT / "benchmark"

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark experimental de ACK sobre el emulador local de Pub/Sub."
    )
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("campaign", help="Ejecutar campaña en el emulador local.")
    run.add_argument("--out", required=True)
    run.add_argument("--validation", action="store_true")
    run.add_argument("--resume", action="store_true")
    collect = sub.add_parser("collect", help="Normalizar y recolectar una campaña formal.")
    collect.add_argument("campaign_dir")
    collect.add_argument("output_dir")
    args, extra = parser.parse_known_args()

    env = os.environ.copy()
    env["PUBSUB_EMULATOR_HOST"] = "127.0.0.1:8085"
    if args.command == "campaign":
        cmd = [sys.executable, str(MODULES / "campaign.py"),
               "--out", args.out,
               "--host", "127.0.0.1:8085",
               "--project", "local-pubsub-u2"]
        if args.validation:
            cmd.append("--validation")
        if args.resume:
            cmd.append("--resume")
    else:
        cmd = [sys.executable, str(MODULES / "collect.py"),
               args.campaign_dir, args.output_dir]
    return subprocess.call(cmd, cwd=str(MODULES), env=env)

if __name__ == "__main__":
    raise SystemExit(main())
