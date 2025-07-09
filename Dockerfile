FROM python:3.12-alpine

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy local code
WORKDIR /app
COPY . .

# Install the local package
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Set the entrypoint
ENTRYPOINT ["mysql2sqlite"]