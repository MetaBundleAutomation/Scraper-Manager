# Scraper-Manager

FastAPI backend that receives frontend requests and spins up new scraper instances.

## Features

- Exposes endpoint `/spawn` to launch new scraper instances
- Tracks container metadata
- Returns messages with container information:
  - Container ID
  - Parent manager ID
  - Spawn timestamp

## Development

### Requirements

- Python 3.9+
- Docker (for production deployment)

### Setup

1. Install dependencies:
```
pip install -r requirements.txt
```

2. Run the development server:
```
uvicorn main:app --reload
```

The API will be available at http://localhost:8000

## API Endpoints

- `GET /` - Check if the API is running
- `POST /spawn` - Spawn a new scraper instance
- `GET /containers` - List all spawned containers

## Docker Deployment

```
docker build -t scraper-manager:latest .
docker run -p 8000:8000 scraper-manager:latest
```
