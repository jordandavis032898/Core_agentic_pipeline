# ============================================================================
# Agentic Router - Production Dockerfile
# ============================================================================
# Multi-stage build for optimized container size
# 
# Build: docker build -t agentic-router:latest .
# Run:   docker run -p 8000:8000 --env-file .env agentic-router:latest
# ============================================================================

# Stage 1: Builder
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .

# Create virtual environment and install dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ============================================================================
# Stage 2: Production
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2 \
    libxslt1.1 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set Python environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Create directories for uploads, temp files, and cache
RUN mkdir -p /app/uploads /app/temp /home/appuser/.cache && \
    chown -R appuser:appuser /app /home/appuser/.cache

# Copy application code (including pipeline_v1_final for RAG integration)
COPY --chown=appuser:appuser . .

# Ensure pipeline_v1_final is accessible
RUN chmod -R 755 pipeline_v1_final/

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
# Use --loop asyncio to avoid uvloop/nest_asyncio incompatibility
# Add timeout settings to prevent connection exhaustion
# --timeout-keep-alive: Keep connections alive for 5 seconds
# --timeout-graceful-shutdown: Graceful shutdown timeout
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "asyncio", "--timeout-keep-alive", "5", "--timeout-graceful-shutdown", "10"]

