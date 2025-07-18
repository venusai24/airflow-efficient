import os
import time
import threading
import subprocess

DAGS_FOLDER = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "./dags")
SCHEDULER_CMD = "airflow scheduler"  # Adjust if you use a custom entrypoint

def start_scheduler_instance(instance_id):
    # Start a scheduler shard in a subprocess
    env = os.environ.copy()
    env["SCHEDULER_SHARD_ID"] = f"shard-{instance_id}"
    return subprocess.Popen(SCHEDULER_CMD, shell=True, env=env)

def touch_dag_file(dag_filename):
    # Simulate a DAG file change
    dag_path = os.path.join(DAGS_FOLDER, dag_filename)
    with open(dag_path, "a") as f:
        f.write(f"# touched at {time.time()}\n")

def monitor_metrics(duration=60):
    # Monitor Airflow metrics/logs for scheduling latency and duplicates
    print("Monitoring metrics for", duration, "seconds...")
    time.sleep(duration)
    # You can parse Airflow logs or query the DB for task instance states here

def main():
    # Start 2 scheduler shards
    schedulers = [start_scheduler_instance(i) for i in range(2)]
    print("Started 2 scheduler shards.")

    # Simulate DAG changes
    for i in range(5):
        dag_file = f"test_dag_{i}.py"
        touch_dag_file(dag_file)
        print(f"Touched {dag_file}")
        time.sleep(2)  # Wait for event-driven scheduler to pick up changes

    # Monitor for 60 seconds
    monitor_metrics(duration=60)

    # Cleanup
    for proc in schedulers:
        proc.terminate()
    print("Schedulers terminated.")

if __name__ == "__main__":
    main()