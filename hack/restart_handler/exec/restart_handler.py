#!/usr/bin/env python3
import os
import signal
import subprocess
import sys
import time
import asyncio
import time
import logging
import redis
from datetime import datetime

from kubernetes import client, config
from kubernetes.client import Configuration
from kubernetes.client.api import core_v1_api
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set the overall logging level

# Create file handler (for writing to a file)
file_handler = logging.FileHandler('restart_handler.log')  # Choose your log file name
file_handler.setLevel(logging.DEBUG)  # Set the level for this handler

# Create console handler (for writing to stdout)
console_handler = logging.StreamHandler(sys.stdout)  # Defaults to sys.stderr
console_handler.setLevel(logging.DEBUG)   # Set the level for this handler (e.g., only INFO and above)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# constants
WRAPPER_CONTAINER_NAME = "wrapper"
POD_NAME_ENV = "POD_NAME"
REDIS_HOST_ENV = "REDIS_HOST"
REDIS_PORT_ENV = "REDIS_PORT"
JOBSET_NAME_ENV = "JOBSET_NAME"
USER_COMMAND_ENV = "USER_COMMAND"

class RestartHandler:
    def __init__(self):
        self.main_process = None
        self.redis_lock_name = self._get_lock_name()
        self.redis_client = self._init_redis_client()

    def _get_lock_name(self):
        # use jobset name as lock name
        lock_name = os.getenv(JOBSET_NAME_ENV, None)
        if not lock_name:
            raise ValueError(f"environment variables {JOBSET_NAME_ENV} must be set.")
        return lock_name

    def _init_redis_client(self):
        # get Redis service details from env vars
        redis_host = os.environ.get(REDIS_HOST_ENV, None)
        redis_port = os.environ.get(REDIS_PORT_ENV, None)
        if not redis_host or not redis_port:
            raise ValueError(f"environment variables {REDIS_HOST_ENV} and {REDIS_PORT_ENV} must both be set.")

        # set up redis client
        client = redis.Redis(host=redis_host, port=redis_port)
        logger.debug(f"succesfully set up redis client: {client}")
        return client
     
    def acquire_lock(self) -> bool:
        """Attempts to acquire distributed lock for the JobSet. Returns boolean value indicating if
        lock was successfully acquired or not (i.e., it is already held by another process)."""
        success = self.redis_client.setnx(self.redis_lock_name, 1)
        return success

    def release_lock(self):
        """Release lock by deleting the lock key in Redis."""
        self.redis_client.delete(self.redis_lock_name)

    def get_pod_names(self, namespace: str = "default") -> list[str]:
        """Get all pods owned by the given JobSet, except the Redis pod and the
        pod the handler is currently running in."""
        self_pod_name = os.getenv(POD_NAME_ENV)
        pods = client.CoreV1Api().list_namespaced_pod(namespace)
        # filter out self pod name and redis pod
        return [pod.metadata.name for pod in pods.items if "redis" not in pod.metadata.name if pod.metadata.name != self_pod_name]

    def handle_restart_signal(self, signum, frame):
        """Signal handler for SIGUSR1 (restart)."""
        logger.debug("Restart signal received. Restarting main process...")
        os.kill(main_process.pid, signal.SIGKILL)
        self.start_main_process()
        logger.debug("Successfully restarted main process")

    def start_main_process(self):
        """Starts the main command and returns the Popen object."""
        global main_process
        main_command = os.getenv(USER_COMMAND_ENV, None)
        if not main_command:
            raise ValueError(f"environment variable {USER_COMMAND_ENV} must be set.")
        logger.debug(f"Running main command: {main_command}")
        main_process = subprocess.Popen(main_command, shell=True)
        return main_process

    async def broadcast_restart_signal(self, namespace: str = "default"):
        """Attemp to acquire a lock and concurrently broadcast restart signals to all worker pods
        in the JobSet. If this pod cannot acquire the lock, return early and do nothing, since
        this means another pod is already broadcasting the restart signal."""
        logger.debug(f"pod {os.getenv(POD_NAME_ENV)} acquired lock")

        # create coroutines
        tasks = [
            asyncio.create_task(self.exec_restart_command(pod_name, namespace))
            for pod_name in self.get_pod_names()
        ]
        # await concurrent execution of coroutines to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.debug(f"Failed to broadcast signal to pod: {result}")
            else:
                logger.debug(f"") 
        logger.debug("Finished broadcasting restart signal")
        logger.debug(f"pod {os.getenv(POD_NAME_ENV)} released lock")

    async def exec_restart_command(self, pod_name: str, namespace: str = "default"):
        """Asynchronously use kubectl-exec to send a SIGUSR1 signal to PID 1 (wrapper process)
        in each pod in the given namespace in the cluster."""
        # command to send SIGUSR1 signal to PID 1 in a container
        exec_command = ["kill","-SIGUSR1","1"]

        logger.debug(f"sending signal to pod: {pod_name}")
        
        # kubectl exec asynchronously so we broadcast to pods concurrently
        await asyncio.to_thread(
                        stream, # callable, followed by args
                        client.CoreV1Api().connect_get_namespaced_pod_exec,
                        pod_name,
                        namespace,
                        container=WRAPPER_CONTAINER_NAME,
                        command=exec_command,
                        stderr=True, stdin=False,
                        stdout=True, tty=False)

async def main(namespace: str):
    try: 
        # for in cluster testing
        config.load_incluster_config()
    except:
        # for local testing
        config.load_kube_config()

    # set up restart handler
    restart_handler = RestartHandler()

    # setup signal handler for SIGUSR1, which the sidecar will use to signal this wrapper process
    # that it must restart the main process.
    signal.signal(signal.SIGUSR1, restart_handler.handle_restart_signal)

    # start the main process
    main_process = restart_handler.start_main_process()

    # monitor the main process
    while True:
        main_process.poll()
        if main_process.returncode is not None:  # Check if process has finished
            logger.debug(f"Main command exited with code: {main_process.returncode}")
            if main_process.returncode == 0:
                break

            # acquire lock
            if not restart_handler.acquire_lock():
                return
    
            logger.debug("Main command failed. Broadcasting restart signal...")
            start = time.perf_counter()

            await restart_handler.broadcast_restart_signal(namespace)   # broadcast restart signal
            main_process = restart_handler.start_main_process()         # restart main process

            restart_latency = time.perf_counter() - start
            logger.debug(f"Broadcast complete. Duration: {restart_latency} seconds")

            time.sleep(120)
            restart_handler.release_lock()
        
        await asyncio.sleep(1)  # sleep to avoid excessive polling

if __name__ == "__main__":
    asyncio.run(main("default"))