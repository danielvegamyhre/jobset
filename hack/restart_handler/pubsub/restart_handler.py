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
POD_NAME_ENV = "POD_NAME"
REDIS_HOST_ENV = "REDIS_HOST"
REDIS_PORT_ENV = "REDIS_PORT"
JOBSET_NAME_ENV = "JOBSET_NAME"
USER_COMMAND_ENV = "USER_COMMAND"

class RestartHandler:
    def __init__(self, restarts_channel: str = "restarts", lock_name: str = None, lock_ttl_seconds: int = 10) -> None:
        """RestartHandler is a wrapper process which executes the command defined in $USER_COMMAND
        environment variable as a child process. It is meant to be ran in a k8s pod, injected by
        JobSet as a wrapper around the user defined main container process.

        The RestartHandler will monitor the process until
        it exits. If it exits with a non-zero exit code, it will broadcast a "restart signal" to
        all other RestartHandlers in the JobSet via Redis PubSub. RestartHanders subscribe to a
        restart channel, and delete + recreate the child process when a restart signal is received.

        :restarts_channel: name of Redis pubsub channel to broadcast restart signal on.
        :lock_name: name of Redis key to use as a lock.
        :lock_ttl_seconds: length of TTL set on lock key, in seconds. This is used to ensure if the
                           pod that acquired the lock dies before it can release the lock, the lock
                           will automatically be released (deleted) after TTL seconds to ensure we
                           don't deadlock.
        """
        self.main_process = None
        self.restarts_channel = restarts_channel
        self.redis_client = self._init_redis_client()
        self.redis_pubsub = self.redis_client.pubsub()
        self.pod_name = os.getenv(POD_NAME_ENV)
        self.redis_lock_name = lock_name or os.getenv(JOBSET_NAME_ENV)
        self.redis_lock_ttl = lock_ttl_seconds
        self._subscribe_and_confirm()

    def _init_redis_client(self) -> None:
        # get Redis service details from env vars
        redis_host = os.environ.get(REDIS_HOST_ENV, None)
        redis_port = os.environ.get(REDIS_PORT_ENV, None)
        if not redis_host or not redis_port:
            raise ValueError(f"environment variables {REDIS_HOST_ENV} and {REDIS_PORT_ENV} must both be set.")

        # set up redis client
        client = redis.Redis(host=redis_host, port=redis_port)
        if not client.ping():
            raise 
        logger.debug(f"succesfully set up redis client: {client}")
        return client
    
    def _subscribe_and_confirm(self) -> bool:
        """Subscribes to restarts pubsub channel and returns 'true' if successful.
        Raises exception if we timeout trying to subscribe."""
        self.redis_pubsub.subscribe(self.restarts_channel)
        msg: dict = self.redis_pubsub.get_message(timeout=5.0)
        if msg["type"] != "subscribe":
            raise Exception(f"failed to subscribe to channel: {self.restarts_channel}")
     
    def acquire_lock(self) -> bool:
        """Attempts to acquire distributed lock for the JobSet. Returns boolean value indicating if
        lock was successfully acquired or not (i.e., it is already held by another process)."""
        with self.redis_client.pipeline() as pipe:
            pipe.multi()       # Start the transaction
            pipe.setnx(self.redis_lock_name, 1)   
            pipe.expire(self.redis_lock_name, self.redis_lock_ttl) 
            result = pipe.execute()
        success: bool = result[0]        
        return success

    def release_lock(self) -> None:
        """Release lock by deleting the lock key in Redis."""
        self.redis_client.delete(self.redis_lock_name)

    def start_main_process(self) -> None:
        """Starts the main command and returns the Popen object."""
        main_command = os.getenv(USER_COMMAND_ENV, None)
        if not main_command:
            raise ValueError(f"environment variable {USER_COMMAND_ENV} must be set.")
        logger.debug(f"Running main command: {main_command}")
        self.main_process = subprocess.Popen(main_command, shell=True) # store open Popen object to monitor

    def broadcast_restart_signal(self) -> None:
        """Attempt to acquire a lock and concurrently publish restart signal to all worker pods
        in the JobSet. If this pod cannot acquire the lock, return early and do nothing, since
        this means another pod is already broadcasting the restart signal."""
        logger.debug(f"Pod {self.pod_name} acquired lock. Broadcasting restart signal.")
        num_receivers = self.redis_client.publish(self.restarts_channel, self.pod_name)
        logger.debug(f"Finished broadcasting restart signal. Message received by {num_receivers} subscribers.")

    def signal_handler(self) -> None:
        for message in self.redis_pubsub.listen():
            logger.debug(f"received message: {message}")
            if self._is_restart_signal(message):
                logger.debug(f"Received restart signal from {message}, restarting main process (PID: {self.main_process.pid})")
                os.kill(self.main_process.pid, signal.SIGKILL)
                self.start_main_process()
                logger.debug("Successfully restarted main process")
            else:
                logger.debug("not a restart signal")

    def _is_restart_signal(self, msg: dict) -> bool:
        # messages from self should return false
        return msg["type"] == "message" and not msg["data"].decode() == self.pod_name

    def run(self) -> None:
        # start main process
        self.start_main_process()

        # run subscriber / signal handler in a separate thread
        t = threading.Thread(target=self.signal_handler)
        t.start()

        # monitor the main process
        while True:
            self.main_process.poll()
            if self.main_process.returncode is not None:  # Check if process has finished
                logger.debug(f"Main command exited with code: {self.main_process.returncode}")
                if self.main_process.returncode == 0:
                    logger.debug("Main process completed successfully with exit code 0.")
                    break
                if self.acquire_lock():
                    self.broadcast_restart_signal()     # broadcast restart signal
                    self.start_main_process()           # restart main process
            time.sleep(1)  # sleep to avoid excessive polling


def main():
    # set up restart handler
    restart_handler = RestartHandler()
    restart_handler.run()

if __name__ == "__main__":
    main()