#!/bin/bash

set -e

GITHUB_REPO="https://github.com/Fang4321/xfuse.git"
CLONE_DIR="xfuse"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' 

echo -e "${GREEN}[Step 1] Checking base tools...${NC}"

install_package() {
    PACKAGE="$1"
    if command -v apt &> /dev/null; then
        sudo apt update
        sudo apt install -y "$PACKAGE"
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y "$PACKAGE"
    elif command -v yum &> /dev/null; then
        sudo yum install -y "$PACKAGE"
    elif command -v pacman &> /dev/null; then
        sudo pacman -Sy --noconfirm "$PACKAGE"
    else
        echo -e "${RED}Unsupported package manager. Please install $PACKAGE manually.${NC}"
        exit 1
    fi
}

check_or_install() {
    CMD="$1"
    PACKAGE="$2"

    if ! command -v "$CMD" &> /dev/null; then
        echo -e "${RED}Missing: $CMD${NC}. Attempting to install..."
        install_package "$PACKAGE"
    else
        echo -e "${GREEN}Found: $CMD${NC}"
    fi
}

check_or_install python3 python3
check_or_install pip3 python3-pip
check_or_install git git

echo -e "${GREEN}[Step 2] Cloning project from GitHub...${NC}"

if [ -d "$CLONE_DIR" ]; then
    echo "Removing existing $CLONE_DIR directory..."
    rm -rf "$CLONE_DIR"
fi

git clone "$GITHUB_REPO"
cd "$CLONE_DIR"

echo -e "${GREEN}[Step 3] Installing Python requirements...${NC}"

pip3 install --upgrade pip

if [ -f requirements.txt ]; then
    pip3 install -r requirements.txt
else
    echo -e "${RED}Warning: requirements.txt not found.${NC}"
fi

echo -e "${GREEN}[Step 4] Installing the project...${NC}"

pip3 install .

echo -e "${GREEN}[Installation Complete!]${NC}"

echo ""
echo -e "${GREEN}Example usage:${NC}"
echo ""
echo -e "    xfuse /mnt/your_mount_dir"
echo ""
echo -e "${GREEN}All done.${NC}"
