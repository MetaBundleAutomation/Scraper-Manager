from fastapi import FastAPI, HTTPException
import datetime
import socket
import os
import uuid
from pydantic import BaseModel
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

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

# For demo purposes, maintain a list of mock containers
mock_containers = []

@app.get("/")
async def root():
    return {"message": "Scraper Manager API is running"}

@app.post("/spawn")
async def spawn_scraper():
    try:
        # Get current timestamp
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Generate a short ID (simulating Docker container ID)
        container_id = str(uuid.uuid4())[:8]
        
        # For demo purposes, we'll create a mock container object
        mock_container = {
            "id": container_id,
            "manager_id": MANAGER_ID,
            "spawn_time": timestamp,
            "status": "running"
        }
        
        # Add to our list of mock containers
        mock_containers.append(mock_container)
        
        # Return response with container information
        return {
            "message": f"Hello World! I'm container_{container_id}, son of {MANAGER_ID}, spawned at {timestamp}."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error spawning scraper: {str(e)}")

@app.get("/containers")
async def list_containers():
    return {"containers": mock_containers}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
