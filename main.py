import os
import json
import time
import uuid
import subprocess
from typing import List, Dict, Optional, Any
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
import threading
from typing import List

# Load environment variables
CONTAINER_PREFIX = os.environ.get("CONTAINER_PREFIX", "scraper-instance")
CONTAINER_NETWORK = os.environ.get("CONTAINER_NETWORK", "metabundle-scraper_scraper-network")
MAX_CONCURRENT_SCRAPERS = int(os.environ.get("MAX_CONCURRENT_SCRAPERS", "10"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "info")

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

@app.get("/")
async def root():
    return {"message": "Scraper Manager API is running"}

def run_container(container_id, timestamp, task_data):
    """Run a container using the docker command line"""
    try:
        # Generate a unique container name using timestamp and a UUID
        unique_id = str(uuid.uuid4())[:8]
        container_name = f"{CONTAINER_PREFIX}-{timestamp.replace(' ', '-').replace(':', '-')}-{unique_id}"
        
        # Store container info in registry
        container_registry[container_id] = {
            "name": container_name,
            "status": "starting",
            "spawn_time": timestamp,
            "task_data": task_data
        }
        
        # Build the docker run command - just start a basic container
        cmd = [
            "docker", "run", "-d",
            "--name", container_name,
            "--network", CONTAINER_NETWORK,
            # Mount the Docker socket so the container can remove itself
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "scraper-instance:latest"
        ]
        
        # Execute the command
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error running container: {result.stderr}")
            container_registry[container_id]["status"] = "error"
            return False
        
        print(f"Container {container_name} started successfully with ID {container_id}")
        return True
        
    except Exception as e:
        print(f"Exception running container: {e}")
        if container_id in container_registry:
            container_registry[container_id]["status"] = "error"
        return False

@app.post("/spawn")
async def spawn_scraper(background_tasks: BackgroundTasks):
    try:
        # Get current timestamp
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Generate a container ID
        container_id = str(uuid.uuid4())[:8]
        
        # Create mock task data (in a real system, this would come from the request)
        task_data = {
            "url": "https://example.com",
            "depth": 2,
            "max_pages": 10
        }
        
        # Track the container
        container_info = {
            "id": container_id,
            "manager_id": MANAGER_ID,
            "spawn_time": timestamp,
            "status": "starting",
            "task": task_data
        }
        containers.append(container_info)
        
        # Run the container in the background
        background_tasks.add_task(run_container, container_id, timestamp, task_data)
        
        # Return response with basic information (Hello World will come from the container)
        return {
            "status": "success",
            "container_id": container_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error spawning scraper: {str(e)}")

@app.post("/request_task")
async def request_task(request: TaskRequestModel):
    """Handle task request from a container"""
    try:
        # Get current timestamp
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Generate a container ID
        container_id = str(uuid.uuid4())[:8]
        
        # Create mock task data (in a real system, this would come from the request)
        task_data = {
            "url": "https://example.com",
            "depth": 2,
            "max_pages": 10
        }
        
        # Track the container
        container_info = {
            "id": container_id,
            "manager_id": MANAGER_ID,
            "spawn_time": timestamp,
            "status": "running",
            "task": task_data,
            "hostname": request.hostname
        }
        containers.append(container_info)
        
        # Store container info in registry
        container_registry[container_id] = {
            "name": request.hostname,
            "status": "running",
            "spawn_time": timestamp,
            "task_data": task_data
        }
        
        print(f"Task assigned to container {request.hostname} with ID {container_id}")
        
        # Return the task information
        return {
            "container_id": container_id,
            "manager_id": MANAGER_ID,
            "spawn_time": timestamp,
            "task": task_data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error assigning task: {str(e)}")

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
async def container_complete(request: ContainerCompleteRequest, background_tasks: BackgroundTasks):
    """Handle callback from container when it completes its task"""
    try:
        print(f"Container {request.container_id} completed with status: {request.status}")
        print(f"Result: {json.dumps(request.result)}")
        
        # Update container status in our tracking list
        for container in containers:
            if container.get("id") == request.container_id:
                container["status"] = "completed"
        
        # Update container status in registry
        if request.container_id in container_registry:
            container_registry[request.container_id]["status"] = "completed"
            
            # Get the container info from our registry
            container_info = container_registry[request.container_id]
            
            # Note: We don't need to remove the container anymore
            # The container will self-terminate after receiving this confirmation
            print(f"Container {container_info['name']} completed its task, sending confirmation...")
        
        # Add completion message to the dashboard messages
        dashboard_messages.append({
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "type": "complete",
            "container_id": request.container_id,
            "status": request.status,
            "result": request.result
        })
        
        # Limit the number of messages to keep
        if len(dashboard_messages) > 100:
            dashboard_messages.pop(0)
        
        # Return confirmation to the container - this will trigger self-termination
        return {"status": "success", "message": "Container completion acknowledged"}
    except Exception as e:
        print(f"Error handling container completion: {e}")
        return {"status": "error", "message": str(e)}

def remove_container(container_id):
    """Remove a container by its ID or name"""
    try:
        # Find all scraper-instance containers
        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", "name=scraper-instance", "--format", "{{.Names}}"],
            capture_output=True, text=True
        )
        
        if result.returncode != 0:
            print(f"Error finding containers: {result.stderr}")
            return
        
        # Get the list of container names
        container_names = result.stdout.strip().split('\n')
        
        # Remove each container
        for container_name in container_names:
            if container_name:  # Skip empty lines
                print(f"Removing container {container_name}")
                remove_result = subprocess.run(
                    ["docker", "rm", "-f", container_name],
                    capture_output=True, text=True
                )
                
                if remove_result.returncode != 0:
                    print(f"Error removing container {container_name}: {remove_result.stderr}")
                else:
                    print(f"Container {container_name} removed successfully")
        
        if not container_names or all(not name for name in container_names):
            print("No scraper-instance containers found to remove")
    except Exception as e:
        print(f"Exception removing containers: {e}")

def remove_specific_container(container_name):
    """Remove a specific container by name"""
    try:
        # Give the container a moment to finish any ongoing processes
        time.sleep(1)
        
        print(f"Removing container {container_name}")
        
        # First try to stop the container gracefully
        stop_result = subprocess.run(
            ["docker", "stop", container_name],
            capture_output=True, text=True
        )
        
        if stop_result.returncode != 0:
            print(f"Warning: Could not stop container {container_name}: {stop_result.stderr}")
        
        # Then remove the container
        remove_result = subprocess.run(
            ["docker", "rm", "-f", container_name],
            capture_output=True, text=True
        )
        
        if remove_result.returncode != 0:
            print(f"Error removing container {container_name}: {remove_result.stderr}")
        else:
            print(f"Container {container_name} removed successfully")
            
        # Verify the container was removed
        verify_result = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            capture_output=True, text=True
        )
        
        if verify_result.stdout.strip():
            print(f"Warning: Container {container_name} still exists after removal attempt")
        else:
            print(f"Verified: Container {container_name} no longer exists")
            
    except Exception as e:
        print(f"Exception removing container: {e}")

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

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level=LOG_LEVEL)
