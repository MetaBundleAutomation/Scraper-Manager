import os
import json
import time
import uuid
import socket
import datetime
import subprocess
from typing import List, Dict, Optional, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
import threading
from typing import List
import asyncio
import logging
from queue import Queue
from threading import Semaphore

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("scraper-manager")

# Load environment variables
CONTAINER_PREFIX = os.environ.get("CONTAINER_PREFIX", "scraper-instance")
CONTAINER_NETWORK = os.environ.get("CONTAINER_NETWORK", "metabundle-scraper_scraper-network")
MAX_CONCURRENT_SCRAPERS = int(os.environ.get("MAX_CONCURRENT_SCRAPERS", "10"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "info")
MAX_RUNNING_CONTAINERS = int(os.environ.get("MAX_RUNNING_CONTAINERS", 10))

app = FastAPI(title="Scraper Manager API", description="API for managing scraper instances")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Get hostname for manager identification
MANAGER_ID = socket.gethostname()

# Track containers for listing
containers = []
# Map container IDs to container names and status
container_registry = {}

# Store messages for the dashboard
dashboard_messages = []

# Define request models
class ContainerCompleteRequest(BaseModel):
    container_id: str
    status: str
    result: dict

class TaskRequestModel(BaseModel):
    hostname: str

class HelloMessageRequest(BaseModel):
    container_id: str
    message: str

spawn_queue = Queue()
completion_queue = Queue()
request_task_queue = Queue()

spawn_semaphore = Semaphore(MAX_RUNNING_CONTAINERS)

def spawn_worker():
    while True:
        container_id, timestamp, task_data = spawn_queue.get()
        try:
            with spawn_semaphore:
                asyncio.run(run_container_async(container_id, timestamp, task_data))
        except Exception as e:
            logger.error(f"Worker failed to spawn container {container_id}: {e}")
        spawn_queue.task_done()
        time.sleep(0.2)  # Throttle to reduce CPU

def completion_worker():
    while True:
        request_dict = completion_queue.get()
        try:
            # Simulate the request object using a SimpleNamespace for compatibility
            from types import SimpleNamespace
            request = SimpleNamespace(**request_dict)
            # Process the payload (moved from /container/complete)
            if request.container_id in container_registry:
                container_registry[request.container_id]["status"] = "completed"
                container_info = container_registry[request.container_id]
                print(f"Container {container_info['name']} completed its task, sending confirmation...")
            dashboard_messages.append({
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "type": "complete",
                "container_id": request.container_id,
                "status": request.status,
                "result": request.result
            })
            if len(dashboard_messages) > 100:
                dashboard_messages.pop(0)
        except Exception as e:
            print(f"Error handling container completion in worker: {e}")
        completion_queue.task_done()
        time.sleep(0.1)  # Throttle to reduce CPU

def request_task_worker():
    while True:
        req_dict = request_task_queue.get()
        try:
            from types import SimpleNamespace
            request = SimpleNamespace(**req_dict)
            # Simulate real task assignment logic (move from /request_task here)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            container_id = str(uuid.uuid4())[:8]
            task_data = {
                "url": "https://example.com",
                "depth": 2,
                "max_pages": 10
            }
            container_info = {
                "id": container_id,
                "manager_id": MANAGER_ID,
                "spawn_time": timestamp,
                "status": "running",
                "task": task_data,
                "hostname": request.hostname
            }
            containers.append(container_info)
            container_registry[container_id] = {
                "name": request.hostname,
                "status": "running",
                "spawn_time": timestamp,
                "task_data": task_data
            }
            print(f"Task assigned to container {request.hostname} with ID {container_id}")
            # Optionally, notify somewhere that the task is ready/assigned
        except Exception as e:
            print(f"Error handling request_task in worker: {e}")
        request_task_queue.task_done()
        time.sleep(0.1)  # Throttle to reduce CPU

# Start worker threads on launch
threading.Thread(target=spawn_worker, daemon=True).start()
threading.Thread(target=completion_worker, daemon=True).start()
threading.Thread(target=request_task_worker, daemon=True).start()

@app.get("/")
async def root():
    return {"message": "Scraper Manager API is running"}

async def run_container_async(container_id, timestamp, task_data):
    """Run a container using the docker command line asynchronously"""
    async def _run():
        try:
            unique_id = str(uuid.uuid4())[:8]
            container_name = f"{CONTAINER_PREFIX}-{timestamp.replace(' ', '-').replace(':', '-')}-{unique_id}"
            container_registry[container_id] = {
                "name": container_name,
                "status": "starting",
                "spawn_time": timestamp,
                "task_data": task_data
            }
            cmd = [
                "docker", "run", "-d",
                "--name", container_name,
                "--network", CONTAINER_NETWORK,
                "-v", "/var/run/docker.sock:/var/run/docker.sock",
                "--cpus", os.environ.get("CONTAINER_CPU_LIMIT", "0.5"),
                "--memory", os.environ.get("CONTAINER_MEMORY_LIMIT", "256m"),
                "scraper-instance:latest"
            ]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                logger.error(f"Error running container: {stderr.decode()}")
                container_registry[container_id]["status"] = "error"
                return False
            logger.info(f"Container {container_name} started successfully with ID {container_id}")
            return True
        except Exception as e:
            logger.error(f"Exception running container: {e}")
            if container_id in container_registry:
                container_registry[container_id]["status"] = "error"
            return False
    return await _run()


@app.post("/spawn")
async def spawn_scraper():
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        container_id = str(uuid.uuid4())[:8]
        task_data = {
            "url": "https://example.com",
            "depth": 2,
            "max_pages": 10
        }
        container_info = {
            "id": container_id,
            "manager_id": MANAGER_ID,
            "spawn_time": timestamp,
            "status": "queued",
            "task": task_data
        }
        containers.append(container_info)

        # Queue the spawn task
        spawn_queue.put((container_id, timestamp, task_data))

        return {
            "container_id": container_id,
            "status": "queued"
        }
    except Exception as e:
        logger.error(f"Exception in spawn_scraper: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/request_task")
async def request_task(request: TaskRequestModel):
    try:
        # Immediately enqueue the request and ACK
        request_task_queue.put(request.dict())
        return {"status": "received", "message": "Task request enqueued for processing"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/messages")
async def get_messages():
    """Get all messages for the dashboard"""
    return {"messages": dashboard_messages}

@app.post("/container/hello")
async def container_hello(request: HelloMessageRequest):
    """Handle hello message from container"""
    try:
        message = f"Container {request.container_id} says: {request.message}"
        print(message)
        
        # Add message to the dashboard messages
        dashboard_messages.append({
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "type": "hello",
            "container_id": request.container_id,
            "message": request.message
        })
        
        # Limit the number of messages to keep
        if len(dashboard_messages) > 100:
            dashboard_messages.pop(0)
        
        return {"status": "success", "message": "Hello received"}
    except Exception as e:
        print(f"Error handling container hello: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/container/complete")
async def container_complete(request: ContainerCompleteRequest):
    try:
        # Immediately enqueue the payload and ACK
        completion_queue.put(request.dict())
        return {"status": "received", "message": "Payload enqueued for processing"}
    except Exception as e:
        print(f"Error enqueuing container completion: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/containers")
async def list_containers():
    try:
        # Get the list of containers using docker command line
        result = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.ID}}|{{.Names}}|{{.Status}}|{{.Image}}"],
            capture_output=True, text=True
        )
        
        if result.returncode != 0:
            return {"containers": containers}
        
        container_list = []
        for line in result.stdout.splitlines():
            if CONTAINER_PREFIX in line:
                parts = line.split("|")
                if len(parts) >= 4:
                    container_list.append({
                        "id": parts[0],
                        "name": parts[1],
                        "status": parts[2],
                        "image": parts[3]
                    })
        
        return {"containers": container_list}
    except Exception as e:
        print(f"Error listing containers: {e}")
        return {"containers": containers}

async def poll_scraper_status():
    while True:
        # ... polling logic ...
        await asyncio.sleep(1)  # Throttle polling to reduce CPU usage

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level=LOG_LEVEL)
