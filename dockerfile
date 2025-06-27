FROM python:3.11-slim-bookworm

# Create non-root user for security
RUN groupadd -r apiuser && useradd -r -g apiuser apiuser

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy API service code
COPY app.py .
COPY shared/ ./shared/

# Change ownership to non-root user
RUN chown -R apiuser:apiuser /app
USER apiuser

# Expose port
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/api/test || exit 1

# Run the application
CMD ["python", "app.py"]