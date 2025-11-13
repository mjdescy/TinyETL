#!/bin/bash
set -e

echo "Starting SQL Server..."
/opt/mssql/bin/sqlservr &
SQL_PID=$!

# Wait for SQL Server to start
echo "Waiting for SQL Server to start..."
sleep 30

# Function to run SQL commands with retry
run_sql() {
    local max_attempts=10
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        echo "Attempt $attempt: Running SQL initialization..."
        if /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "TestPass123!" -C "$@"; then
            echo "✓ SQL command succeeded"
            return 0
        fi
        echo "SQL command failed, retrying in 3 seconds..."
        sleep 3
        attempt=$((attempt + 1))
    done
    
    echo "Failed to execute SQL after $max_attempts attempts"
    return 1
}

# Create database
echo "Creating testdb database..."
run_sql -Q "
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'testdb')
BEGIN
    CREATE DATABASE testdb;
    PRINT 'Database testdb created';
END
ELSE
BEGIN
    PRINT 'Database testdb already exists';
END
"

# Configure database and create user
echo "Configuring database and creating user..."
run_sql -d testdb -Q "
-- Set database options for better performance
ALTER DATABASE testdb SET RECOVERY SIMPLE;
ALTER DATABASE testdb SET AUTO_CREATE_STATISTICS ON;
ALTER DATABASE testdb SET AUTO_UPDATE_STATISTICS ON;

-- Create login if not exists
IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = 'testuser')
BEGIN
    CREATE LOGIN testuser WITH PASSWORD = 'testpass';
    PRINT 'Login testuser created';
END
ELSE
BEGIN
    PRINT 'Login testuser already exists';
END

-- Create user if not exists
IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = 'testuser')
BEGIN
    CREATE USER testuser FOR LOGIN testuser;
    ALTER ROLE db_owner ADD MEMBER testuser;
    PRINT 'User testuser created and granted permissions';
END
ELSE
BEGIN
    PRINT 'User testuser already exists';
END
"

echo "✓ SQL Server initialization complete!"
echo "Database: testdb"
echo "Users: SA (password: TestPass123!), testuser (password: testpass)"
echo ""

# Keep SQL Server running
wait $SQL_PID
