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
read -p "Enter the name for your conda environment: " ENV_NAME
if [ -z "$ENV_NAME" ]; then
    ENV_NAME="nrm-backend"
    echo "No name provided, using default: $ENV_NAME"
fi

read -p "Enter the location for your conda environment (press Enter for default location): " ENV_PATH
if [ -z "$ENV_PATH" ]; then
    ENV_PATH="$HOME/miniconda/envs/$ENV_NAME"
    echo "Using default location: $ENV_PATH"
fi

# Create the full path if it doesn't exist
mkdir -p "$ENV_PATH"
echo "Created directory: $ENV_PATH"

# Create and configure virtual environment
print_section "Creating virtual environment"
~/miniconda/bin/conda create --prefix "$ENV_PATH" python=3.10 -y

# Configure conda to show only environment name
~/miniconda/bin/conda config --set env_prompt '(${name}) '

# Install conda packages
print_section "Installing conda packages"
CONDA_PACKAGES=(
    "conda-forge::Django"
    "conda-forge::django-cors-headers"
    "conda-forge::djangorestframework"
    "conda-forge::drf-yasg==1.21.7"
    "conda-forge::earthengine-api"
    "conda-forge::Fiona"
    "conda-forge::geojson"
    "conda-forge::geopandas"
    "conda-forge::matplotlib"
    "conda-forge::pandas"
    "conda-forge::python-dotenv"
    "conda-forge::requests"
    "conda-forge::seaborn"
    "conda-forge::xmltodict"
    "conda-forge::folium"
    "conda-forge::pcraster"
    "conda-forge::geetools"
    "conda-forge::sqlite"
    "conda-forge::celery"
    "conda-forge::unidecode"
    "conda-forge::rasterio"
    "conda-forge::unidecode"
    "conda-forge::shapely"
    "conda-forge::pyshp"
    "conda-forge::pyproj"
    "conda-forge::gdal"
)

# Activate virtual environment and install packages
source ~/miniconda/bin/activate "$ENV_PATH"

for package in "${CONDA_PACKAGES[@]}"; do
    echo "Installing $package..."
    conda install -y $package
done

# Install pip packages
print_section "Installing pip packages"
PIP_PACKAGES=(
    "django-environ"
    "mysql-connector-python"
    "mysqlclient"
    "python-docx"
    "pymannkendall"
    "pydantic"
    "fastapi"
)

for package in "${PIP_PACKAGES[@]}"; do
    echo "Installing $package..."
    pip install --upgrade $package
done

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