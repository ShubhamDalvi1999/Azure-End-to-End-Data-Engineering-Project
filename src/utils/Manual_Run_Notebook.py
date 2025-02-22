"""
Manual Notebook Runner Utility
============================

This notebook provides a flexible way to execute other notebooks programmatically
with optional parameter passing. It serves as a utility for manual triggering and
workflow automation of data pipeline components.

Features:
- Dynamic notebook execution
- Optional parameter passing
- Flexible tablename configuration
- Workflow and manual execution support

Dependencies:
    - dbutils: For Databricks utilities and widget handling

Workflow Configuration:
    This notebook can be run as part of a Databricks workflow with the following parameters:
    - notebook_path: Path to the notebook to execute
    - tablename: (Optional) Table name to pass to the target notebook
    - timeout_seconds: (Optional) Maximum execution time in seconds

Author: Shubham Dalvi
Last Modified: 2024
"""

# COMMAND ----------
from typing import Dict, Optional, Tuple
import json

def get_workflow_parameters() -> Dict[str, str]:
    """
    Get parameters from workflow context if available.
    
    Returns:
        Dict[str, str]: Dictionary of workflow parameters
    """
    try:
        # Check if running in a workflow
        workflow_params = json.loads(dbutils.widgets.get("job_parameters"))
        return {
            "notebook_path": workflow_params.get("notebook_path", ""),
            "tablename": workflow_params.get("tablename", ""),
            "timeout_seconds": workflow_params.get("timeout_seconds", "0")
        }
    except:
        return {}

def setup_widgets() -> Tuple[str, Optional[str], int]:
    """
    Configure and initialize notebook widgets.
    Supports both workflow and manual execution.
    
    Returns:
        Tuple[str, Optional[str], int]: (notebook_path, tablename, timeout_seconds)
    """
    # Setup widgets for manual execution
    dbutils.widgets.text("notebook_path", "")
    dbutils.widgets.text("tablename", "")
    dbutils.widgets.text("timeout_seconds", "0")
    
    # Try to get workflow parameters first
    workflow_params = get_workflow_parameters()
    if workflow_params:
        return (
            workflow_params["notebook_path"],
            workflow_params.get("tablename"),
            int(workflow_params.get("timeout_seconds", 0))
        )
    
    # Fall back to widget values for manual execution
    return (
        dbutils.widgets.get("notebook_path"),
        dbutils.widgets.get("tablename"),
        int(dbutils.widgets.get("timeout_seconds"))
    )

def run_notebook(notebook_path: str, tablename: str = None, timeout_seconds: int = 0) -> None:
    """
    Execute a notebook with optional parameters.
    
    Args:
        notebook_path (str): Path to the notebook to execute
        tablename (str, optional): Table name to pass as parameter
        timeout_seconds (int, optional): Maximum execution time in seconds
        
    Returns:
        None
        
    Raises:
        ValueError: If notebook_path is not provided
        TimeoutError: If notebook execution exceeds timeout
    """
    if not notebook_path:
        raise ValueError("notebook_path is required")
        
    params = {"tablename": tablename} if tablename else {}
    print(f"Executing notebook {notebook_path} with parameters: {params}")
    
    try:
        result = dbutils.notebook.run(notebook_path, timeout_seconds, params)
        print(f"Notebook execution result: {result}")
    except Exception as e:
        print(f"Error executing notebook: {str(e)}")
        raise

# COMMAND ----------
def main():
    """
    Main execution function that orchestrates the notebook running process.
    Supports both workflow and manual execution modes.
    """
    try:
        # Setup widgets and get values
        notebook_path, tablename, timeout_seconds = setup_widgets()
        
        # Execute notebook
        run_notebook(
            notebook_path=notebook_path,
            tablename=tablename if tablename else None,
            timeout_seconds=timeout_seconds
        )
        
        print("Notebook execution completed successfully")
    except Exception as e:
        print(f"Error executing notebook: {str(e)}")
        raise

# COMMAND ----------
# Execute main function
if __name__ == "__main__":
    main()

# Usage Examples:
# 1. Workflow Execution:
#    Configure job with parameters:
#    {
#        "notebook_path": "/Workspace/Bronze/ingest_races",
#        "tablename": "formula1",
#        "timeout_seconds": 3600
#    }
#
# 2. Manual Execution:
#    - Set notebook_path widget to "/Workspace/Bronze/ingest_races"
#    - Set tablename widget to "formula1" (optional)
#    - Set timeout_seconds widget to "3600" (optional)