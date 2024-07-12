#!/usr/bin/env python3
import os
import signal
import subprocess
import sys
import time
import asyncio
import argparse

import time
from datetime import datetime

from kubernetes import client, config
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

# constants
WRAPPER_CONTAINER_NAME = "wrapper"

# global vars
main_process = None

def get_pod_names(namespace: str = "default") -> list[str]:
    pods = client.CoreV1Api().list_namespaced_pod(namespace)
    return [pod.metadata.name for pod in pods.items]

def handle_restart_signal(signum, frame):
    """Signal handler for SIGUSR1 (restart)."""
    print("Restart signal received. Restarting main command...")
    os.kill(main_process.pid, signal.SIGKILL)
    start_main_process()

def start_main_process():
    """Starts the main command and returns the Popen object."""
    global main_process
    main_command_parts = sys.argv[1:]
    main_command = " ".join(main_command_parts)
    print(f"Running main command: {main_command}")
    main_process = subprocess.Popen(main_command_parts)
    return main_process

async def broadcast_restart_signal(namespace: str = "default"):
    # create coroutines
    tasks = [
        asyncio.create_task(exec_restart_command(pod_name, namespace))
        for pod_name in get_pod_names()
    ]
    # await concurrent execution of coroutines to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            print(f"Failed to broadcast signal to pod: {result}")
    print("Finished broadcasting restart signal")


async def exec_restart_command(pod_name: str, namespace: str = "default"):
    """Asynchronously use kubectl-exec to send a SIGUSR1 signal to PID 1 (wrapper process)
    in each pod in the given namespace in the cluster."""
    # command to send SIGUSR1 signal to PID 1 in a container
    exec_command = ["/bin/sh","-c","kill","-SIGUSR1","1"]

    # kubectl exec asynchronously so we broadcast to pods concurrently
    resp = await asyncio.to_thread(
            stream(client.CoreV1Api().connect_get_namespaced_pod_exec,
                  pod_name,
                  namespace,
                  container=WRAPPER_CONTAINER_NAME,
                  command=exec_command,
                  stderr=True, stdin=False,
                  stdout=True, tty=False)
            )

async def main(namespace: str):
    global main_process

    config.load_kube_config()

    # setup signal handler for SIGUSR1, which the sidecar will use to signal this wrapper process
    # that it must restart the main process.
    signal.signal(signal.SIGUSR1, handle_restart_signal)

    # start the main process
    main_process = start_main_process()

    # monitor the main process
    while True:
        main_process.poll()
        if main_process.returncode is not None:  # Check if process has finished
            print(f"Main command exited with code: {main_process.returncode}")
            if main_process.returncode == 0:
                break
            
            print("Main command failed. Broadcasting restart signal...")
            await broadcast_restart_signal(namespace)
            main_process = start_main_process()  # restart the main process
        
        await asyncio.sleep(1)  # sleep to avoid excessive polling


if __name__ == "__main__":
#    argparser = argparse.ArgumentParser()
#    argparser.add_argument("--namespace", type=str, default="default")
#    args = argparser.parse_args()
    asyncio.run(main("default"))