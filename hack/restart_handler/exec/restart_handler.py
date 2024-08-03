#!/usr/bin/env python3
import os
import signal
import subprocess
import sys
import time
import asyncio
import time
import logging
from datetime import datetime, timezone, timedelta

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
# console_handler = logging.StreamHandler(sys.stdout)  # Defaults to sys.stderr
# console_handler.setLevel(logging.DEBUG)   # Set the level for this handler (e.g., only INFO and above)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
#console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
#logger.addHandler(console_handler)

# constants
WRAPPER_CONTAINER_NAME = "wrapper"
POD_NAME_ENV = "POD_NAME"
JOBSET_NAME_ENV = "JOBSET_NAME"
USER_COMMAND_ENV = "USER_COMMAND"

class RestartHandler:
    def __init__(self, lease_name: str = "jobset-restart-lease", lease_ttl_seconds: int = 10, namespace: str = "default"):
        self.main_process = None
        self.lease_name = lease_name
        self.pod_name = os.getenv(POD_NAME_ENV)
        self.lease_name = lease_name or os.getenv(JOBSET_NAME_ENV)
        self.lease_ttl_seconds = lease_ttl_seconds
        self.namespace = namespace
        self.lease = self._create_or_fetch_lease()

        # setup signal handler for SIGUSR1, which the sidecar will use to signal this wrapper process
        # that it must restart the main process.
        signal.signal(signal.SIGUSR1, self.handle_restart_signal)

    def _get_lock_name(self):
        # use jobset name as lock name
        lock_name = os.getenv(JOBSET_NAME_ENV, None)
        if not lock_name:
            raise ValueError(f"environment variables {JOBSET_NAME_ENV} must be set.")
        return lock_name

    def _create_or_fetch_lease(self) -> client.models.v1_lease.V1Lease:
        """Create lease and return it, or fetch it if it already exists."""
        lease = client.V1Lease(
            metadata=client.V1ObjectMeta(
                name=self.lease_name,
                namespace=self.namespace,
            ),
            spec=client.V1LeaseSpec(
                holder_identity=self.pod_name,
                acquire_time=datetime.now(timezone.utc).isoformat(),
                renew_time=datetime.now(timezone.utc).isoformat(),
                lease_duration_seconds=self.lease_ttl_seconds,  # Duration of the lease in seconds
                lease_transitions=0,
            )
        )
        try:
            lease = client.CoordinationV1Api().create_namespaced_lease(namespace=self.namespace, body=lease)
            logger.debug(f"{self.pod_name} created lease with {self.lease_ttl_seconds} TTL.")
            return lease
        except client.rest.ApiException as e:
            if e.status == 409: # 409 is a conflict, lease already exists
                lease = client.CoordinationV1Api().read_namespaced_lease(name=self.lease_name, namespace=self.namespace)
                logger.debug(f"fetched lease from apiserver")
                return lease
            logger.debug(f"exception occured while acquiring lease: {e}")
            raise e

    def acquire_lease(self) -> bool:
        """Attempts to acquire restart lease for the JobSet. Returns boolean value indicating if
        lock was successfully acquired or not (i.e., it is already held by another process)."""
        try:
            lease = client.CoordinationV1Api().read_namespaced_lease(name=self.lease_name, namespace=self.namespace)
        except client.rest.ApiException as e:
            print(f"error fetching lease before acquiring it")
            raise e
        
        # we can acquire the lease if we already hold it or it has expired
        if lease.spec.holder_identity == self.pod_name or self._lease_has_expired(lease):
            try:
                lease.spec.holder_identity = self.pod_name
                lease.spec.renew_time = datetime.now(timezone.utc).isoformat()
                lease = client.CoordinationV1Api().replace_namespaced_lease(name=self.lease_name, namespace=self.namespace, body=lease)
                # if successful, persist lease locally for next time
                print(f"{self.pod_name} acquired lease: {lease}")
                return True
            except client.rest.ApiException as e:
                print(f"exception occured while acquiring lease: {e}")
                raise e
        else:
            print(f"lease still held by another pod {lease.spec.holder_identity}")
            return False
        
    def _lease_has_expired(self, lease: client.models.v1_lease.V1Lease) -> bool:
        return lease.spec.renew_time + timedelta(seconds=lease.spec.lease_duration_seconds) < datetime.now(lease.spec.renew_time.tzinfo)

    def get_pod_names(self, namespace: str = "default") -> list[str]:
        """Get all pods owned by the given JobSet, except self."""
        self_pod_name = os.getenv(POD_NAME_ENV)
        pods = client.CoreV1Api().list_namespaced_pod(namespace)
        # filter out self pod name
        return [pod.metadata.name for pod in pods.items if pod.metadata.name != self_pod_name]

    def handle_restart_signal(self, signum, frame):
        """Signal handler for SIGUSR1 (restart)."""
        logger.debug("Restart signal received. Restarting main process...")
        # kill existing user process
        os.kill(self.main_process.pid, signal.SIGKILL)
        # create new user process and store it for tracking
        self.start_main_process()
        logger.debug("Successfully restarted main process")

    def start_main_process(self) -> None:
        """Starts the main command as a subprocess, and stores the Popen object for tracking."""
        main_command = os.getenv(USER_COMMAND_ENV, None)
        if not main_command:
            raise ValueError(f"environment variable {USER_COMMAND_ENV} must be set.")
        # kill existing user process if necessary
        if self.main_process is not None:
            logger.debug(f"Killing existing user process PID: {self.main_process.pid}")
            try:
                os.kill(self.main_process.pid, signal.SIGKILL)
            except ProcessLookupError:
                logger.debug(f"process PID {self.main_process.pid} does not exist")
                pass

        logger.debug(f"Running main command: {main_command}")
        self.main_process = subprocess.Popen(main_command, shell=True)

    async def broadcast_restart_signal(self, namespace: str = "default"):
        """Attemp to acquire a lock and concurrently broadcast restart signals to all worker pods
        in the JobSet. If this pod cannot acquire the lock, return early and do nothing, since
        this means another pod is already broadcasting the restart signal."""
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
        
    async def run(self) -> None:
        '''Launch the user process and run monitoring loop.'''
        # launch user process
        self.start_main_process()

        # run monitoring loop
        while True:
            self.main_process.poll()
            if self.main_process.returncode is not None:  # Check if process has finished
                logger.debug(f"Main command exited with code: {self.main_process.returncode}")
                if self.main_process.returncode == 0:
                    break

                # attempt to acquire lease. if we successfully acquire it, broadcast restart signal.
                # otherwise, do nothing.
                if self.acquire_lease():
                    logger.debug("Main command failed. Broadcasting restart signal...")
                    start = time.perf_counter()

                    await self.broadcast_restart_signal(self.namespace) # broadcast restart signal
                    self.start_main_process()                           # restart local user process

                    restart_latency = time.perf_counter() - start
                    logger.debug(f"Broadcast complete. Duration: {restart_latency} seconds")

            await asyncio.sleep(1)  # sleep to avoid excessive polling

async def main(namespace: str):
    try: 
        # for in cluster testing
        config.load_incluster_config()
    except:
        # for local testing
        config.load_kube_config()

    # start the restart handler
    restart_handler = RestartHandler(namespace=namespace)
    await restart_handler.run()


if __name__ == "__main__":
    asyncio.run(main("default"))