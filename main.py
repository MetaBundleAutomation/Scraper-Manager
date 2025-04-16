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
import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("scraper-manager")

# Load environment variables
CONTAINER_PREFIX = os.environ.get("CONTAINER_PREFIX", "scraper-instance")
CONTAINER_NETWORK = os.environ.get("CONTAINER_NETWORK", "metabundle-scraper_scraper-network")
MAX_CONCURRENT_SCRAPERS = int(os.environ.get("MAX_CONCURRENT_SCRAPERS", "10"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "info")
MAX_RUNNING_CONTAINERS = int(os.environ.get("MAX_RUNNING_CONTAINERS", 10))
MIN_WORKER_POOL = int(os.environ.get("MIN_WORKER_POOL", "2"))
MAX_WORKER_POOL = int(os.environ.get("MAX_WORKER_POOL", "5"))

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

# Worker pool management
class WorkerState:
    AVAILABLE = "available"
    BUSY = "busy"
    UNREACHABLE = "unreachable"

# Worker info storage
worker_registry = {}  # worker_id -> worker info

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
requests_queue = Queue()

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

def request_worker():
    while True:
        url, task_request = requests_queue.get()
        try:
            response = requests.post(url, json=task_request)
            if response.status_code != 200:
                logger.error(f"Error sending task to worker: {response.text}")
        except Exception as e:
            logger.error(f"Error sending task to worker: {e}")
        requests_queue.task_done()
        time.sleep(0.1)  # Throttle to reduce CPU

# Start worker threads on launch
threading.Thread(target=spawn_worker, daemon=True).start()
threading.Thread(target=completion_worker, daemon=True).start()
threading.Thread(target=request_task_worker, daemon=True).start()
threading.Thread(target=request_worker, daemon=True).start()

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

@app.post("/worker/register")
async def register_worker(request: dict):
    """Handle worker registration"""
    try:
        worker_id = request.get("worker_id")
        max_tasks = request.get("max_tasks", 5)
        
        if not worker_id:
            raise HTTPException(status_code=400, detail="Missing worker_id")
        
        # Store worker info
        worker_registry[worker_id] = {
            "id": worker_id,
            "status": WorkerState.AVAILABLE,
            "max_tasks": max_tasks,
            "current_tasks": 0,
            "register_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "last_heartbeat": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        
        logger.info(f"Worker {worker_id} registered with max_tasks={max_tasks}")
        return {"status": "success", "message": f"Worker {worker_id} registered successfully"}
    
    except Exception as e:
        logger.error(f"Error in worker registration: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/worker/heartbeat")
async def worker_heartbeat(request: dict):
    """Handle worker heartbeat"""
    try:
        worker_id = request.get("worker_id")
        tasks_count = request.get("tasks_count", 0)
        
        if not worker_id or worker_id not in worker_registry:
            raise HTTPException(status_code=404, detail="Worker not found")
        
        # Update worker info
        worker_registry[worker_id]["last_heartbeat"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        worker_registry[worker_id]["current_tasks"] = tasks_count
        
        # Update status based on tasks
        max_tasks = worker_registry[worker_id].get("max_tasks", 5)
        if tasks_count >= max_tasks:
            worker_registry[worker_id]["status"] = WorkerState.BUSY
        else:
            worker_registry[worker_id]["status"] = WorkerState.AVAILABLE
        
        return {"status": "success"}
    
    except Exception as e:
        logger.error(f"Error in worker heartbeat: {e}")
        return {"status": "error", "message": str(e)}

def get_available_worker():
    """Find the least loaded available worker"""
    available_workers = [
        worker_id for worker_id, info in worker_registry.items()
        if info["status"] == WorkerState.AVAILABLE
    ]
    
    if not available_workers:
        return None
    
    # Find worker with fewest tasks
    return min(
        available_workers,
        key=lambda worker_id: worker_registry[worker_id]["current_tasks"]
    )

async def spawn_worker_container():
    """Spawn a new worker container"""
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        unique_id = str(uuid.uuid4())[:8]
        container_name = f"{CONTAINER_PREFIX}-{timestamp.replace(' ', '-').replace(':', '-')}-{unique_id}"
        
        logger.info(f"Spawning new worker container: {container_name}")
        
        cmd = [
            "docker", "run", "-d",
            "--name", container_name,
            "--network", CONTAINER_NETWORK,
            "-p", "0:8000",  # Expose FastAPI port dynamically
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "--cpus", os.environ.get("CONTAINER_CPU_LIMIT", "0.5"),
            "--memory", os.environ.get("CONTAINER_MEMORY_LIMIT", "256m"),
            "scraper-instance:latest"
        ]
        
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            logger.error(f"Error running container: {stderr.decode()}")
            return None
        
        logger.info(f"Container {container_name} started successfully")
        return container_name
    
    except Exception as e:
        logger.error(f"Exception spawning worker: {e}")
        return None

async def ensure_minimum_workers():
    """Ensure we have at least MIN_WORKER_POOL workers"""
    available_count = len([
        w for w in worker_registry.values()
        if w["status"] in [WorkerState.AVAILABLE, WorkerState.BUSY]
    ])
    
    if available_count < MIN_WORKER_POOL:
        to_spawn = MIN_WORKER_POOL - available_count
        logger.info(f"Spawning {to_spawn} workers to maintain minimum pool size")
        
        for _ in range(to_spawn):
            with spawn_semaphore:
                await spawn_worker_container()
                # Allow time for the worker to start and register
                await asyncio.sleep(3)

@app.post("/spawn")
async def spawn_scraper():
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        task_id = str(uuid.uuid4())[:8]
        
        # Create task data
        task_data = {
            "url": "https://example.com",
            "depth": 2,
            "max_pages": 10
        }
        
        # Check for available worker
        worker_id = get_available_worker()
        
        # If no worker is available, check if we can spawn a new one
        if not worker_id:
            current_workers = len(worker_registry)
            
            if current_workers < MAX_WORKER_POOL:
                # Spawn a new worker asynchronously
                spawn_queue.put((None, timestamp, None))
                
                # Queue the task for later assignment
                logger.info(f"No available workers, queued task {task_id} and triggered worker spawn")
                task_info = {
                    "id": task_id,
                    "status": "queued",
                    "spawn_time": timestamp,
                    "task_data": task_data,
                    "assigned_to": None
                }
                containers.append(task_info)
                
                return {
                    "task_id": task_id,
                    "status": "queued",
                    "message": "No available workers. Task queued and new worker being spawned."
                }
            else:
                # We're at worker capacity but all are busy
                logger.info(f"All workers busy, queued task {task_id}")
                task_info = {
                    "id": task_id,
                    "status": "queued",
                    "spawn_time": timestamp,
                    "task_data": task_data,
                    "assigned_to": None
                }
                containers.append(task_info)
                
                return {
                    "task_id": task_id,
                    "status": "queued",
                    "message": "All workers busy. Task queued."
                }
        
        # Assign to an available worker
        try:
            # Prepare task request
            task_request = {
                "task_id": task_id,
                "url": task_data["url"],
                "depth": task_data["depth"],
                "max_pages": task_data["max_pages"]
            }
            
            # Track the task
            task_info = {
                "id": task_id,
                "status": "assigned",
                "spawn_time": timestamp,
                "task_data": task_data,
                "assigned_to": worker_id
            }
            containers.append(task_info)
            
            # Send to worker (background task to avoid blocking)
            worker_url = f"http://{worker_id}:8000/task"
            requests_queue.put((worker_url, task_request))
            
            # Update worker status preemptively
            worker_registry[worker_id]["current_tasks"] += 1
            if worker_registry[worker_id]["current_tasks"] >= worker_registry[worker_id]["max_tasks"]:
                worker_registry[worker_id]["status"] = WorkerState.BUSY
            
            logger.info(f"Task {task_id} assigned to worker {worker_id}")
            
            return {
                "task_id": task_id,
                "status": "assigned",
                "worker_id": worker_id
            }
        
        except Exception as e:
            logger.error(f"Error assigning task to worker: {e}")
            # Queue for retry
            task_info = {
                "id": task_id,
                "status": "queued",
                "spawn_time": timestamp,
                "task_data": task_data,
                "assigned_to": None,
                "error": str(e)
            }
            containers.append(task_info)
            
            return {
                "task_id": task_id,
                "status": "queued",
                "message": f"Error assigning to worker: {str(e)}. Task queued for retry."
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
