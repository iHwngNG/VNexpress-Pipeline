import os
import subprocess
import sys
import argparse
import shutil


def start():
    print("===================================================")
    print("Starting VNExpress Pipeline...")
    print("===================================================")

    # Check .env
    if not os.path.exists(".env"):
        if os.path.exists(".env.example"):
            print("[INFO] .env file not found. Creating from .env.example...")
            shutil.copy(".env.example", ".env")
        else:
            print("[ERROR] .env.example not found! Please create a .env file.")
            sys.exit(1)

    # Create required directories
    for d in ["logs", "plugins", "dags"]:
        os.makedirs(d, exist_ok=True)

    print("[INFO] Building and starting Docker containers...")

    # Try different docker compose commands
    cmd = ["docker", "compose", "up", "-d", "--build"]
    try:
        subprocess.run(cmd, check=True)
    except FileNotFoundError:
        # Fallback to docker-compose if 'docker compose' isn't available
        try:
            cmd = ["docker-compose", "up", "-d", "--build"]
            subprocess.run(cmd, check=True)
        except Exception as e:
            print(f"[ERROR] Docker is not installed or not in PATH: {e}")
            sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to start Docker containers: {e}")
        sys.exit(1)

    print("===================================================")
    print("Pipeline is running!")
    print("Access Airflow UI: http://localhost:8080")
    print("Login: airflow / airflow")
    print("===================================================")


def stop():
    print("===================================================")
    print("Stopping VNExpress Pipeline...")
    print("===================================================")
    try:
        subprocess.run(["docker", "compose", "down"], check=True)
    except FileNotFoundError:
        subprocess.run(["docker-compose", "down"], check=True)
    except Exception as e:
        print(f"[ERROR] Failed to stop containers: {e}")

    print("===================================================")
    print("Pipeline stopped.")
    print("===================================================")


def logs():
    print("===================================================")
    print("Viewing realtime logs... (Ctrl+C to exit)")
    print("===================================================")
    try:
        subprocess.run(["docker", "compose", "logs", "-f"])
    except FileNotFoundError:
        subprocess.run(["docker-compose", "logs", "-f"])
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"[ERROR] Could not view logs: {e}")


def query(search_term):
    print(f"[INFO] Searching database for: '{search_term}'")
    try:
        subprocess.run(
            [
                "docker",
                "exec",
                "airflow-webserver",
                "python",
                "/opt/airflow/plugins/tools/query_db.py",
                search_term,
            ],
            check=True,
        )
    except Exception as e:
        print("\n[ERROR] Search failed.")
        print("Possible reasons:")
        print("1. Pipeline is not running (Run: python manage.py start)")
        print("2. Docker container is starting up (Wait 30s)")
        print(f"Details: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VNExpress Pipeline Manager")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser("start", help="Start the pipeline")
    subparsers.add_parser("stop", help="Stop the pipeline")
    subparsers.add_parser("logs", help="View pipeline logs")

    query_parser = subparsers.add_parser("query", help="Query the vector database")
    query_parser.add_argument(
        "term", type=str, help="Search term (in quotes if multiple words)"
    )

    args = parser.parse_args()

    if args.command == "start":
        start()
    elif args.command == "stop":
        stop()
    elif args.command == "logs":
        logs()
    elif args.command == "query":
        query(args.term)
    else:
        parser.print_help()
