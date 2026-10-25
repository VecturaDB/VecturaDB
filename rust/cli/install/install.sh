#!/usr/bin/env bash
# Changed from the original Chroma file; see the git history for the changes and their authors.
# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
set -e

# ----------------------------------------------
# VecturaDB CLI Installer Script
# Usage:
#   curl -sSL https://raw.githubusercontent.com/VecturaDB/VecturaDB/main/rust/cli/install/install.sh | bash
# The command is vecturadb. To have it also as chroma, run the script with VECTURADB_CHROMA_ALIAS=1:
#   curl -sSL https://raw.githubusercontent.com/VecturaDB/VecturaDB/main/rust/cli/install/install.sh | VECTURADB_CHROMA_ALIAS=1 bash
# ----------------------------------------------
REPO="VecturaDB/VecturaDB"

OS=$(uname -s)
ARCH=$(uname -m)
PLATFORM=""

case "$OS" in
  Linux*)
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
      PLATFORM="linux-aarch64"
    else
      PLATFORM="linux-x86_64"
    fi
    ;;
  Darwin*)
    if [ "$ARCH" = "arm64" ]; then
      PLATFORM="macos-arm64"
    else
      PLATFORM="macos-x64"
    fi
    ;;
  MINGW*|MSYS*|CYGWIN*)
    PLATFORM="windows-x64.exe"
    ;;
  *)
    echo "Unsupported OS: $OS"
    exit 1
    ;;
esac

RELEASE=$(curl -sSL "https://api.github.com/repos/${REPO}/releases?per_page=1")
TAG=$(echo "$RELEASE" | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')
if [ -z "$TAG" ]; then
  echo "No release found in ${REPO}"
  exit 1
fi
ASSET="vecturadb-${PLATFORM}"
# Older releases have the binary as chroma-<platform>: then the script also installs it as chroma, the name those
# releases document.
if ! echo "$RELEASE" | grep -q "\"name\": *\"${ASSET}\""; then
  ASSET="chroma-${PLATFORM}"
  VECTURADB_CHROMA_ALIAS=1
fi
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${ASSET}"
echo "Downloading ${ASSET} from ${DOWNLOAD_URL}..."
curl -L "$DOWNLOAD_URL" -o vecturadb

chmod +x vecturadb

if [ -w /usr/local/bin ]; then
  BIN_DIR=/usr/local/bin
else
  BIN_DIR="$HOME/.local/bin"
  mkdir -p "$BIN_DIR"
fi
mv vecturadb "$BIN_DIR/vecturadb"
if [ "${VECTURADB_CHROMA_ALIAS:-}" = "1" ]; then
  ln -sf vecturadb "$BIN_DIR/chroma"
  echo "Installed $BIN_DIR/vecturadb, also as $BIN_DIR/chroma"
else
  echo "Installed $BIN_DIR/vecturadb"
fi

if [ "$BIN_DIR" = "$HOME/.local/bin" ]; then
  case ":$PATH:" in
    *:"$HOME/.local/bin":*)
      ;;
    *)
      echo "====================="
      echo "Warning ⚠️: $HOME/.local/bin is not in your PATH."
      echo "To add it, you can run:"
      echo '  export PATH="$HOME/.local/bin:$PATH"'
      echo "====================="
      ;;
  esac
fi
