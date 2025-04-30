#!/bin/bash

set -e  # Exit on any error

echo "🚀 Starting GPS Server Setup..."

# Environment Variables Configuration (Avoid storing secrets here)
REDIS_HOST="localhost"
REDIS_PORT="6379"
export KAFKA_TOPIC="amnex_direct_live"
export KAFKA_SERVER="localhost:9096"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
export REDIS_NODES="localhost:6379"
export IS_CLUSTER_REDIS="false"
export STANDALONE_REDIS_DATABASE="1"

# Database config should be injected securely, e.g., via AWS Secrets Manager
export DB_USER="atlas_rw"
export DB_PASS="password"
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="atlas_app_pilot"

# Install required packages
install_package() {
    if command -v yum &> /dev/null; then
        sudo yum install -y "$1"
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y "$1"
    else
        echo "❌ Error: No package manager found (yum/dnf)" >&2
        exit 1
    fi
}
mkdir -p ~/.ssh
ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null

# Install Redis CLI if missing
install_redis_cli() {
    if ! command -v redis6-cli &> /dev/null; then
        echo "📦 Installing Redis CLI..."
        install_package redis6
    fi
}

# Install Python dependencies
install_python_deps() {
    if ! command -v python3 &> /dev/null; then
        echo "📦 Installing Python3..."
        install_package python3
        install_package python3-pip
    fi

    if ! command -v virtualenv &> /dev/null; then
        echo "📦 Installing virtualenv..."
        python3 -m pip install --upgrade virtualenv
    fi

    echo "📦 Installing development tools..."
    install_package python3-devel
}

# Get Redis version key
get_version_from_redis() {
    local redis_host="${REDIS_HOST:-localhost}"
    local redis_port="${REDIS_PORT:-6379}"
    
    # Ensure Redis connection before attempting GET
    if ! redis6-cli -h "$redis_host" -p "$redis_port" PING &> /dev/null; then
        echo "⚠️ Redis is unreachable, falling back to latest commit."
        git fetch origin main
        echo "$(git rev-parse origin/main)"
        return
    fi

    local version
    version=$(redis6-cli -h "$redis_host" -p "$redis_port" GET GPS_SERVER_VERSION || echo "")

    if [[ -z "$version" ]]; then
        echo "⚠️ No version found in Redis, using latest commit."
        git fetch origin main
        version=$(git rev-parse origin/main)
    fi

    echo "$version"
}

# Install system dependencies
echo "🔧 Installing dependencies..."
install_redis_cli
install_python_deps
install_package git  # Ensure git is available

# Define working directory
WORK_DIR="/home/ec2-user/python-stuff"
REPO_URL="git@github.com:nammayatri/python-proxy.git"

# Set up working directory
sudo mkdir -p "$WORK_DIR"
sudo chown -R ec2-user:ec2-user "$WORK_DIR"
cd "$WORK_DIR"

# Clone or update repository
if [ -d "$WORK_DIR/.git" ]; then
    echo "🔄 Updating existing repository..."
    git fetch --all
    git reset --hard origin/main  # Ensure clean state
else
    echo "📥 Cloning repository..."
    git clone "$REPO_URL" .
fi

# Get version from Redis and checkout
VERSION=$(get_version_from_redis)
echo "⚡ Checking out version: $VERSION"
echo " upper is version"
git checkout "$VERSION"

# Set up Python virtual environment
echo "🐍 Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m virtualenv venv
fi
source venv/bin/activate

# Install requirements
echo "📦 Installing Python dependencies..."
python3 -m pip install --upgrade pip wheel
python3 -m pip install -r requirements.txt

# Start the server
echo "🚀 Starting GPS server..."
# Start the server in the background and save its PID
sudo $WORK_DIR/venv/bin/python3 amnex-live-data-server.py &
SERVER_PID=$!
echo "Server started with PID: $SERVER_PID"

# Monitor for version changes every 10 seconds
while true; do
    sleep 10
    NEW_VERSION=$(get_version_from_redis)
    CURRENT_VERSION="$VERSION"
    
    if [ "$NEW_VERSION" != "$CURRENT_VERSION" ]; then
        echo "🔄 Version changed from $CURRENT_VERSION to $NEW_VERSION. Restarting server..."
        # Kill the current server process
        sudo kill -9 $SERVER_PID
        
        # Update the current version
        VERSION="$NEW_VERSION"
        
        git checkout "$VERSION"
        
        $WORK_DIR/venv/bin/python3 -m pip install -r requirements.txt
        
        sudo $WORK_DIR/venv/bin/python3 amnex-live-data-server.py &
        SERVER_PID=$!
        echo "Server restarted with PID: $SERVER_PID"
    fi
done

# Cleanup
trap 'deactivate' EXIT

