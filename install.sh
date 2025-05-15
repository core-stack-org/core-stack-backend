#!/bin/bash

# Exit on error
set -e

echo "Starting installation process..."

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to print section headers
print_section() {
    echo "======================================"
    echo "$1"
    echo "======================================"
}

# Function to detect shell type
detect_shell() {
    if [ -n "$ZSH_VERSION" ]; then
        echo "zsh"
    elif [ -n "$BASH_VERSION" ]; then
        echo "bash"
    else
        # Default to bash if can't determine
        echo "bash"
    fi
}

# Function to refresh shell configuration
refresh_shell_config() {
    local shell_type=$(detect_shell)
    if [ "$shell_type" = "zsh" ]; then
        if [ -f ~/.zshrc ]; then
            source ~/.zshrc
            echo "Refreshed ~/.zshrc"
        fi
    else
        if [ -f ~/.bashrc ]; then
            source ~/.bashrc
            echo "Refreshed ~/.bashrc"
        fi
    fi
}

# Create necessary directories
print_section "Creating directories"
mkdir -p ~/miniconda

# Download and install Miniconda
print_section "Installing Miniconda"
if command_exists conda; then
    echo "Conda is already installed and available in PATH"
elif [ -f ~/miniconda/bin/conda ]; then
    echo "Miniconda is installed but not in PATH. Adding to PATH..."
    export PATH="$HOME/miniconda/bin:$PATH"
else
    echo "Downloading Miniconda..."
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda/miniconda.sh
    bash ~/miniconda/miniconda.sh -b -u -p ~/miniconda
    rm -rf ~/miniconda/miniconda.sh

    # Initialize conda for the detected shell
    shell_type=$(detect_shell)
    if [ "$shell_type" = "zsh" ]; then
        ~/miniconda/bin/conda init zsh
    else
        ~/miniconda/bin/conda init bash
    fi

    # Disable auto activation of base environment
    ~/miniconda/bin/conda config --set auto_activate_base false

    echo "Miniconda installed successfully"
fi

# Refresh shell configuration
refresh_shell_config

# Get environment name and location from user
print_section "Environment Configuration"
read -p "Enter the name for your conda environment (press Enter for default 'corestack'): " ENV_NAME
if [ -z "$ENV_NAME" ]; then
    ENV_NAME="corestack"
    echo "No name provided, using default: $ENV_NAME"
fi

read -p "Enter the location for your conda environment (press Enter for default location): " ENV_PATH
if [ -z "$ENV_PATH" ]; then
    ENV_PATH="$HOME/miniconda/envs/$ENV_NAME"
    echo "Using default location: $ENV_PATH"
fi

# Create the full path if it doesn't exist
mkdir -p "$(dirname "$ENV_PATH")"
echo "Created directory: $(dirname "$ENV_PATH")"

# Create and configure virtual environment using environment.yml
print_section "Creating virtual environment from environment.yml"
echo "Using environment.yml file to create conda environment..."

# Check if environment.yml exists
if [ ! -f "environment.yml" ]; then
    echo "Error: environment.yml file not found in the current directory."
    exit 1
fi

# Create environment from yml file with custom prefix
echo "Creating conda environment from environment.yml with prefix: $ENV_PATH"
echo "This may take some time as it installs all dependencies including pip packages..."
~/miniconda/bin/conda env create -f environment.yml --prefix "$ENV_PATH"

# Activate the environment
source ~/miniconda/bin/activate "$ENV_PATH"

# Install system dependencies
print_section "Installing system dependencies"
echo "Updating package lists and installing required system packages..."

# Check if running on KDE Neon
if [ -f /etc/os-release ] && grep -q "KDE neon" /etc/os-release; then
    echo "Detected KDE Neon, using pkcon..."
    sudo pkcon refresh -y
    sudo pkcon install -y pkg-config python3-dev default-libmysqlclient-dev build-essential
else
    echo "Using apt-get..."
    sudo apt-get update
    sudo apt-get install -y pkg-config python3-dev default-libmysqlclient-dev build-essential
fi

# Check and install RabbitMQ if not present
print_section "Setting up RabbitMQ"
if ! command_exists rabbitmq-server; then
    echo "Installing RabbitMQ..."
    if command_exists apt-get; then
        sudo apt-get update
        sudo apt-get install -y rabbitmq-server
    else
        echo "Error: Package manager 'apt-get' not found. Please install RabbitMQ manually."
        exit 1
    fi
fi

# Start and enable RabbitMQ
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
sudo systemctl status rabbitmq-server

# Print GDAL version
print_section "Checking GDAL installation"
gdalinfo --version

print_section "Installation Complete"
echo "Please run 'source ~/.zshrc' or 'source ~/.bashrc' to complete the setup"
echo "To activate the environment, use: conda activate $ENV_NAME"
