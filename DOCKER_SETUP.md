# Agentic Router - Docker Setup Guide

Complete guide for running the Agentic Router API as a containerized service.

The Agentic Router provides intelligent query routing with three main pipelines:
- **Extract Pipeline**: Table extraction from PDFs
- **Chatbot Pipeline**: RAG-based Q&A using Qdrant vector database
- **EDGAR Pipeline**: SEC financial data fetching

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start](#quick-start)
3. [Configuration](#configuration)
4. [Running the Service](#running-the-service)
5. [Production Deployment](#production-deployment)
6. [Troubleshooting](#troubleshooting)
7. [Architecture](#architecture)

---

## Prerequisites

- **Docker** >= 20.10
- **Docker Compose** >= 2.0
- **API Keys:**
  - OpenAI API key (required)
  - LlamaCloud API key (required for LlamaParse PDF parsing)

---

## Quick Start

### 1. Clone and Navigate

```bash
cd agentic_router
```

### 2. Create Environment File

```bash
cp env.example .env
```

Edit `.env` with your API keys:

```bash
# Required API Keys
OPENAI_API_KEY=sk-your-openai-key-here
LLAMA_CLOUD_API_KEY=llx-your-llamacloud-key-here

# Optional: API authentication (leave empty for dev mode)
API_KEY=

# Qdrant Configuration (defaults work with docker-compose)
QDRANT_URL=http://qdrant:6333
QDRANT_HOST=qdrant
QDRANT_PORT=6333
QDRANT_API_KEY=
```

### 3. Start Services

```bash
# Build and start all services
docker-compose up -d --build

# View logs
docker-compose logs -f
```

### 4. Verify

```bash
# Check health
curl http://localhost:8000/health

# Expected response:
# {"status":"healthy","timestamp":"...","router_initialized":true}
```

### 5. Access API

- **API:** http://localhost:8000
- **Swagger Docs:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc
- **Qdrant Dashboard:** http://localhost:6333/dashboard

---

## Configuration

### Environment Variables

Create a `.env` file in the `agentic_router/` directory:

```bash
# ==============================================================================
# REQUIRED - API Keys
# ==============================================================================
OPENAI_API_KEY=sk-...
LLAMA_CLOUD_API_KEY=llx-...

# ==============================================================================
# OPTIONAL - API Authentication
# ==============================================================================
# If set, all requests must include X-API-Key header
# Leave empty to disable authentication (development mode)
API_KEY=

# ==============================================================================
# QDRANT (defaults work with docker-compose)
# ==============================================================================
QDRANT_URL=http://qdrant:6333
QDRANT_HOST=qdrant
QDRANT_PORT=6333
QDRANT_API_KEY=
COLLECTION_NAME=policy_documents

# ==============================================================================
# MODEL CONFIGURATION
# ==============================================================================
EMBEDDING_MODEL=text-embedding-ada-002
LLM_MODEL=gpt-4o
EXTRACTOR_MODEL=gpt-4o-mini
TOP_K=5

# ==============================================================================
# PROCESSING
# ==============================================================================
USE_MULTIMODAL=true
```

### .env.example Template

```bash
# Copy this to .env and fill in your values

# Required
OPENAI_API_KEY=sk-your-key-here
LLAMA_CLOUD_API_KEY=llx-your-key-here

# Optional
API_KEY=
QDRANT_URL=http://qdrant:6333
QDRANT_HOST=qdrant
QDRANT_PORT=6333
COLLECTION_NAME=policy_documents
```

---

## Running the Service

### Development Mode

```bash
# Start with hot-reload
docker-compose up --build

# Or run API only (requires external Qdrant)
docker run -p 8000:8000 \
  -e OPENAI_API_KEY=$OPENAI_API_KEY \
  -e LLAMA_CLOUD_API_KEY=$LLAMA_CLOUD_API_KEY \
  -e QDRANT_URL=http://host.docker.internal:6333 \
  -e QDRANT_HOST=host.docker.internal \
  -e QDRANT_PORT=6333 \
  agentic-router:latest
```

### View Logs

```bash
# All services
docker-compose logs -f

# API only
docker-compose logs -f api

# Qdrant only
docker-compose logs -f qdrant
```

### Stop Services

```bash
# Stop (keep data)
docker-compose down

# Stop and delete volumes (fresh start)
docker-compose down -v
```

### Rebuild After Code Changes

```bash
docker-compose up -d --build
```

---

## Production Deployment

### 1. Build Production Image

```bash
docker build -t agentic-router:v1.0.0 .
docker tag agentic-router:v1.0.0 your-registry/agentic-router:v1.0.0
docker push your-registry/agentic-router:v1.0.0
```

### 2. Production docker-compose.prod.yml

```yaml
version: '3.8'

services:
  api:
    image: your-registry/agentic-router:v1.0.0
    deploy:
      replicas: 2
      resources:
        limits:
          cpus: '2'
          memory: 4G
    environment:
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - LLAMA_CLOUD_API_KEY=${LLAMA_CLOUD_API_KEY}
      - API_KEY=${API_KEY}  # REQUIRED in production
      - QDRANT_URL=http://qdrant:6333
      - QDRANT_HOST=qdrant
      - QDRANT_PORT=6333
    ports:
      - "8000:8000"
    depends_on:
      - qdrant
    restart: always

  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - qdrant_data:/qdrant/storage
    restart: always

volumes:
  qdrant_data:
```

### 3. Security Checklist

- [ ] Set `API_KEY` environment variable
- [ ] Set `QDRANT_API_KEY` for Qdrant authentication (optional but recommended)
- [ ] Use HTTPS (put behind nginx/traefik with TLS)
- [ ] Set resource limits
- [ ] Configure log rotation
- [ ] Set up monitoring (Prometheus/Grafana)
- [ ] Restrict Qdrant port exposure if not needed externally

### 4. Production Run

```bash
docker-compose -f docker-compose.prod.yml up -d
```

---

## Troubleshooting

### Common Issues

#### 1. "Router not initialized"

**Cause:** Missing or invalid API keys.

**Fix:**
```bash
# Check environment variables are set
docker-compose exec api env | grep API_KEY

# Verify keys in .env file
cat .env | grep -E "(OPENAI|LLAMA)"
```

#### 2. Qdrant Connection Failed

**Cause:** Qdrant not ready yet.

**Fix:**
```bash
# Check Qdrant health
curl http://localhost:6333/health

# Wait for Qdrant to be ready
docker-compose logs -f qdrant
# Look for: "Qdrant is ready"
```

#### 3. PDF Upload Fails

**Cause:** LlamaParse API key issue.

**Fix:**
```bash
# Verify LlamaParse key
curl -H "Authorization: Bearer $LLAMA_CLOUD_API_KEY" \
  https://api.cloud.llamaindex.ai/api/v1/health
```

### Viewing Container Logs

```bash
# Real-time logs
docker-compose logs -f api

# Last 100 lines
docker-compose logs --tail=100 api

# Specific time range
docker-compose logs --since="2024-01-15T10:00:00" api
```

### Accessing Container Shell

```bash
docker-compose exec api /bin/bash
```

### Checking Service Status

```bash
# All services
docker-compose ps

# Health status
curl http://localhost:8000/health
curl http://localhost:6333/health
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Docker Network                          │
│                    (agentic_router_network)                     │
│                                                                 │
│  ┌──────────────────────┐      ┌───────────────────────────┐   │
│  │   agentic_router_api │      │ agentic_router_qdrant     │   │
│  │                      │      │                            │   │
│  │   FastAPI + Uvicorn  │─────▶│   Qdrant Vector DB        │   │
│  │                      │      │                            │   │
│  │   Port: 8000         │      │   Port: 6333 (HTTP)       │   │
│  │                      │      │   Port: 6334 (gRPC)       │   │
│  └──────────────────────┘      └───────────────────────────┘   │
│           │                              │                      │
└───────────┼──────────────────────────────┼──────────────────────┘
            │                              │
        ┌───┴───┐                    ┌─────┴─────┐
        │ :8000 │                    │   :6333   │
        └───────┘                    └───────────┘
            │                              │
   ┌────────┴────────┐            ┌────────┴────────┐
   │ External Clients │            │ (Internal only) │
   │ (API calls, etc.)│            └─────────────────┘
   └──────────────────┘
```

### Container Details

| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| `agentic_router_api` | `agentic-router:latest` | 8000 | FastAPI backend with RAG pipeline |
| `agentic_router_qdrant` | `qdrant/qdrant:latest` | 6333, 6334 | Vector database for embeddings |

### Volumes

| Volume | Container Path | Purpose |
|--------|----------------|---------|
| `qdrant_data` | `/qdrant/storage` | Qdrant collections and vectors |
| `uploads_data` | `/app/uploads` | Uploaded PDFs |

---

## Health Checks

### API Health

```bash
curl http://localhost:8000/health
```

Expected:
```json
{
  "status": "healthy",
  "timestamp": "2025-01-15T10:30:00.000000",
  "router_initialized": true
}
```

### Qdrant Health

```bash
curl http://localhost:6333/health
```

Expected:
```json
{
  "title": "qdrant - vector search engine",
  "version": "...",
  "status": "ok"
}
```

---

## Quick Reference

```bash
# Start everything
docker-compose up -d --build

# Stop everything
docker-compose down

# View logs
docker-compose logs -f

# Rebuild API only
docker-compose up -d --build api

# Shell into API container
docker-compose exec api /bin/bash

# Fresh start (delete all data)
docker-compose down -v && docker-compose up -d --build
```

---

## RAG Integration

The Agentic Router uses an integrated RAG (Retrieval-Augmented Generation) pipeline:

- **Vector Database:** Qdrant stores document embeddings for semantic search
- **Document Isolation:** Documents are isolated per `user_id` for multi-tenant support
- **Query Pipeline:** Uses LlamaIndex workflows with SentenceTransformerRerank for improved retrieval
- **Node Parsing:** MarkdownElementNodeParser extracts both text and table elements
- **Embeddings:** OpenAI text-embedding-3-small for vector representations

**Key Features:**
- Documents uploaded with the same `user_id` are queryable together
- Each user's documents are stored in separate Qdrant collections
- Supports back-to-back queries without re-ingestion
- Automatic sub-question decomposition for complex queries

For more details, see [API_REFERENCE.md](API_REFERENCE.md) for the `/query` endpoint documentation.

---
