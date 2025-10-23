import sys
import os
import ast
import importlib.util
from typing import List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_dag_syntax(file_path: str) -> Tuple[bool, str]:
    """Validate Python syntax of DAG file"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Parse AST to check syntax
        ast.parse(content)
        return True, "Syntax OK"
    
    except SyntaxError as e:
        return False, f"Syntax error: {e}"
    except Exception as e:
        return False, f"Error reading file: {e}"

def validate_dag_structure(file_path: str) -> Tuple[bool, str]:
    """Validate DAG structure and required elements"""
    try:
        # Add the directory to Python path
        dag_dir = os.path.dirname(file_path)
        if dag_dir not in sys.path:
            sys.path.insert(0, dag_dir)
        
        # Import the module
        spec = importlib.util.spec_from_file_location("dag_module", file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Check for DAG objects
        dag_found = False
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if hasattr(attr, '__class__') and attr.__class__.__name__ == 'DAG':
                dag_found = True
                
                # Validate DAG properties
                if not hasattr(attr, 'dag_id') or not attr.dag_id:
                    return False, "DAG must have a valid dag_id"
                
                if not hasattr(attr, 'schedule_interval'):
                    return False, "DAG must have schedule_interval defined"
                
                # Check for tasks
                if not attr.tasks:
                    return False, "DAG must contain at least one task"
        
        if not dag_found:
            return False, "No DAG object found in file"
        
        return True, "DAG structure OK"
    
    except Exception as e:
        return False, f"Error validating DAG structure: {e}"

def main():
    """Main validation function"""
    if len(sys.argv) < 2:
        logger.error("No files provided for validation")
        sys.exit(1)
    
    files_to_validate = sys.argv[1:]
    all_valid = True
    
    for file_path in files_to_validate:
        if not file_path.endswith('.py'):
            continue
            
        logger.info(f"Validating {file_path}")
        
        # Run all validations
        validations = [
            validate_dag_syntax,
            validate_dag_structure
        ]
        
        for validation_func in validations:
            is_valid, message = validation_func(file_path)
            if not is_valid:
                logger.error(f"❌ {file_path}: {message}")
                all_valid = False
            else:
                logger.info(f"✅ {file_path}: {message}")
    
    if not all_valid:
        logger.error("Some DAG files failed validation")
        sys.exit(1)
    
    logger.info("All DAG files passed validation")
    sys.exit(0)

if __name__ == "__main__":
    main()