#!/bin/bash
# filepath: cleanup_old_files.sh

# Script to clean up old files after restructuring
# This script will move code from dags/weather/ to include/ and clean up old files

set -e  # Exit on any error

echo "🧹 Starting cleanup process..."

# Function to check if directory exists and is not empty
check_directory() {
    if [ -d "$1" ] && [ "$(ls -A $1)" ]; then
        return 0
    else
        return 1
    fi
}

# 1. Move remaining code from dags/weather/ to include/weather/ if needed
echo "📦 Checking if dags/weather/ needs to be migrated..."
if check_directory "dags/weather"; then
    echo "⚠️  Found files in dags/weather/. Moving to include/weather/..."
    
    # Create include/weather if it doesn't exist
    mkdir -p include/weather
    
    # Move files (excluding __pycache__ and .pyc files)
    find dags/weather/ -name "*.py" -not -path "*/__pycache__/*" -exec cp {} include/weather/ \;
    
    # Move tests directory if exists
    if [ -d "dags/weather/tests" ]; then
        mkdir -p include/weather/tests
        find dags/weather/tests/ -name "*.py" -not -path "*/__pycache__/*" -exec cp {} include/weather/tests/ \;
    fi
    
    echo "✅ Files moved to include/weather/"
fi

# 2. Remove old weather directory from dags/
echo "🗑️  Removing old dags/weather/ directory..."
if [ -d "dags/weather" ]; then
    rm -rf dags/weather/
    echo "✅ Removed dags/weather/"
fi

# 3. Remove Makefile (using uv instead)
echo "🗑️  Removing Makefile (using uv instead)..."
if [ -f "Makefile" ]; then
    rm Makefile
    echo "✅ Removed Makefile"
fi

# 4. Remove backup files
echo "🗑️  Removing backup files..."
if [ -f "pyproject.toml.backup" ]; then
    rm pyproject.toml.backup
    echo "✅ Removed pyproject.toml.backup"
fi

# 5. Remove migration script (if not needed anymore)
echo "🗑️  Removing migration script..."
if [ -f "migrate_structure.sh" ]; then
    rm migrate_structure.sh
    echo "✅ Removed migrate_structure.sh"
fi

# 6. Clean up any __pycache__ directories
echo "🧹 Cleaning up __pycache__ directories..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
echo "✅ Cleaned up __pycache__ directories"

# 7. Clean up any .pyc files
echo "🧹 Cleaning up .pyc files..."
find . -name "*.pyc" -delete 2>/dev/null || true
echo "✅ Cleaned up .pyc files"

echo ""
echo "🎉 Cleanup completed successfully!"
echo ""
echo "📁 Current structure should now be:"
echo "   ├── dags/           (only DAG files)"
echo "   ├── include/        (all reusable code)"
echo "   ├── kubernetes/     (k8s configurations)"
echo "   └── pyproject.toml  (uv configuration)"
echo ""
echo "ℹ️  Next steps:"
echo "   1. Update import statements in DAGs to use 'from include.weather import ...'"
echo "   2. Test your DAGs to ensure imports work correctly"
echo "   3. Run: uv sync"