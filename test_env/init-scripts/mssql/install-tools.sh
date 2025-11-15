#!/bin/bash
set -e

echo "Installing MSSQL command-line tools..."

# Install dependencies
apt-get update
apt-get install -y curl gnupg2 apt-transport-https

# Add Microsoft repository for SQL Server tools
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl -sSL https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Update and install tools
apt-get update
ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev

# Add to PATH
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
export PATH="$PATH:/opt/mssql-tools18/bin"

echo "✓ MSSQL tools installed successfully"

# Now run the initialization script
exec /bin/bash /init.sh
