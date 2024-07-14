#!/usr/bin/env python3
import os
import signal
import subprocess
import sys
import time
import time
import logging
import redis
import threading
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
    def __init__(self, restarts_channel: str = "restarts"):
        self.main_process = None
        self.restarts_channel = restarts_channel
        self.redis_lock_name = self._get_lock_name()
        self.redis_client = self._init_redis_client()
        self.redis_pubsub = self.redis_client.pubsub()
        self.redis_pubsub.subscribe(restarts_channel)
        self.pod_name = os.getenv(POD_NAME_ENV)

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
        pods = client.CoreV1Api().list_namespaced_pod(namespace)
        # filter out self pod name and redis pod
        return [pod.metadata.name for pod in pods.items if "redis" not in pod.metadata.name if pod.metadata.name != self.pod_name]

    def start_main_process(self):
        """Starts the main command and returns the Popen object."""
        global main_process
        main_command = os.getenv(USER_COMMAND_ENV, None)
        if not main_command:
            raise ValueError(f"environment variable {USER_COMMAND_ENV} must be set.")
        logger.debug(f"Running main command: {main_command}")
        main_process = subprocess.Popen(main_command, shell=True)
        self.main_process = main_process
        return main_process

    def broadcast_restart_signal(self, namespace: str = "default"):
        """Attemp to acquire a lock and concurrently publish restart signal to all worker pods
        in the JobSet. If this pod cannot acquire the lock, return early and do nothing, since
        this means another pod is already broadcasting the restart signal."""
        if self.acquire_lock():
            logger.debug("Acquired lock. Publishing restart signal")
            self.redis_client.publish(self.restarts_channel, self.pod_name)
            logger.debug("Finished broadcasting restart signal")
            time.sleep(10)
            logger.debug("Releasing lock.")
            self.release_lock()

    def signal_handler(self):
        for message in self.redis_pubsub.listen():
            if self._is_restart_signal(message):
                logger.debug(f"Received restart signal from {message}, restarting main process (PID: {self.main_process.pid})")
                os.kill(self.main_process.pid, signal.SIGKILL)
                self.start_main_process()
                logger.debug("Successfully restarted main process")

    def _is_restart_signal(self, msg: dict) -> bool:
        is_msg = msg.get("type", "") == 'message'
        from_self = str(msg.get("data", "")).decode() == self.pod_name
        return is_msg and not from_self


def main(namespace: str):
    try: 
        # for in cluster testing
        config.load_incluster_config()
    except:
        # for local testing
        config.load_kube_config()

    # set up restart handler
    restart_handler = RestartHandler()

    # start the main process
    main_process = restart_handler.start_main_process()

    # start subscriber / signal handler
    t = threading.Thread(target=restart_handler.signal_handler)
    t.start()

    # monitor the main process
    while True:
        main_process.poll()
        if main_process.returncode is not None:  # Check if process has finished
            logger.debug(f"Main command exited with code: {main_process.returncode}")
            if main_process.returncode == 0:
                break

            restart_handler.broadcast_restart_signal(namespace)   # broadcast restart signal
            main_process = restart_handler.start_main_process()   # restart main process

        time.sleep(1)  # sleep to avoid excessive polling

if __name__ == "__main__":
    main("default")