"""
Formula 1 Results Email Distribution Utility
=========================================

This notebook implements automated email distribution of Formula 1 race results
to end users. It processes data from the Gold layer and sends formatted CSV
reports via email.

Features:
- Automated email composition
- Secure credential management
- CSV report generation
- SSL-enabled email transmission
- Configurable recipient list
- Workflow and manual execution support

Dependencies:
    - email: For email composition and MIME handling
    - smtplib: For SMTP server communication
    - ssl: For secure email transmission
    - dbutils: For secret management
    - spark: For SQL queries
    - pandas: For CSV generation

Workflow Configuration:
    This notebook can be run as part of a Databricks workflow with the following parameters:
    - report_year: Year for the report data
    - report_month: Month for the report data
    - receiver_email: Email address to send report to
    - report_limit: (Optional) Number of top drivers to include

Author: Shubham Dalvi
Last Modified: 2024
"""

# COMMAND ----------
# Import required libraries
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import logging
from typing import Tuple, Dict, Optional
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------
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
            "report_year": workflow_params.get("report_year", str(datetime.today().year)),
            "report_month": workflow_params.get("report_month", str(datetime.today().month)),
            "receiver_email": workflow_params.get("receiver_email", ""),
            "report_limit": workflow_params.get("report_limit", "10")
        }
    except:
        return {}

def setup_widgets() -> Dict[str, any]:
    """
    Configure and initialize notebook widgets.
    Supports both workflow and manual execution.
    
    Returns:
        Dict[str, any]: Configuration parameters
    """
    # Setup widgets for manual execution
    dbutils.widgets.text("report_year", str(datetime.today().year))
    dbutils.widgets.text("report_month", str(datetime.today().month))
    dbutils.widgets.text("receiver_email", "")
    dbutils.widgets.text("report_limit", "10")
    
    # Try to get workflow parameters first
    workflow_params = get_workflow_parameters()
    if workflow_params:
        return {
            "report_year": int(workflow_params["report_year"]),
            "report_month": int(workflow_params["report_month"]),
            "receiver_email": workflow_params["receiver_email"],
            "report_limit": int(workflow_params["report_limit"])
        }
    
    # Fall back to widget values for manual execution
    return {
        "report_year": int(dbutils.widgets.get("report_year")),
        "report_month": int(dbutils.widgets.get("report_month")),
        "receiver_email": dbutils.widgets.get("receiver_email"),
        "report_limit": int(dbutils.widgets.get("report_limit"))
    }

def get_email_config(receiver_email: str = None) -> Dict[str, str]:
    """
    Get email configuration including credentials from secrets.
    
    Args:
        receiver_email (str, optional): Override default receiver email
    
    Returns:
        Dict[str, str]: Email configuration parameters
    """
    return {
        "sender_email": "layer.admin@gmail.com",
        "password": dbutils.secrets.get(scope="layer-deltalake", key="gmail-password"),
        "receiver_email": receiver_email or "ssagarprajapati2212@gmail.com",
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 465
    }

def create_email_message(config: Dict[str, str], month: int, year: int) -> MIMEMultipart:
    """
    Create the email message with headers.
    
    Args:
        config (Dict[str, str]): Email configuration
        month (int): Report month
        year (int): Report year
        
    Returns:
        MIMEMultipart: Configured email message object
    """
    logger.info("Creating email message...")
    message = MIMEMultipart()
    message["From"] = config["sender_email"]
    message["To"] = config["receiver_email"]
    message["Subject"] = f"Formula 1 result data {month} {year}"
    message["Bcc"] = config["receiver_email"]
    
    body = "Please find out formula 1 result data in CSV file attached in the email"
    message.attach(MIMEText(body, 'plain'))
    
    return message

def generate_report_data(year: int, month: int, limit: int = 10) -> str:
    """
    Generate the Formula 1 results report and save to CSV.
    
    Args:
        year (int): Year to filter results
        month (int): Month to filter results
        limit (int): Number of top drivers to include
        
    Returns:
        str: Path to the generated CSV file
    """
    logger.info(f"Generating Formula 1 results report for {month}/{year}...")
    query = f"""
    SELECT b.full_name, total_points 
    FROM gold.fact_results a 
    INNER JOIN gold.dim_driver b ON a.driverId = b.driverId  
    WHERE year(date) = {year} AND month(date) = {month:02d}
    ORDER BY total_points DESC 
    LIMIT {limit}
    """
    df = spark.sql(query)
    
    output_path = '/dbfs/FileStore/tables/race.csv'
    logger.info(f"Saving report to {output_path}")
    df.toPandas().to_csv(output_path, index=False)
    
    return output_path

def attach_csv_to_message(message: MIMEMultipart, file_path: str, file_name: str = 'race.csv') -> None:
    """
    Attach a CSV file to the email message.
    
    Args:
        message (MIMEMultipart): Email message to attach to
        file_path (str): Path to the CSV file
        file_name (str): Name for the attached file
    """
    logger.info("Attaching CSV to email...")
    with open(file_path, 'rb') as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename={file_name}")
    message.attach(part)

def send_email(config: Dict[str, str], message: MIMEMultipart) -> None:
    """
    Send the email using SSL connection.
    
    Args:
        config (Dict[str, str]): Email configuration
        message (MIMEMultipart): Prepared email message
    """
    logger.info("Sending email...")
    context = ssl.create_default_context()
    
    with smtplib.SMTP_SSL(config["smtp_server"], config["smtp_port"], context=context) as server:
        server.login(config["sender_email"], config["password"])
        server.sendmail(
            config["sender_email"],
            config["receiver_email"],
            message.as_string()
        )
    logger.info("Email sent successfully!")

# COMMAND ----------
def main():
    """
    Main execution function that orchestrates the email distribution process.
    Supports both workflow and manual execution modes.
    """
    try:
        # Get configurations
        config = setup_widgets()
        email_config = get_email_config(config["receiver_email"])
        
        # Create message
        message = create_email_message(
            email_config,
            config["report_month"],
            config["report_year"]
        )
        
        # Generate and attach report
        report_path = generate_report_data(
            year=config["report_year"],
            month=config["report_month"],
            limit=config["report_limit"]
        )
        attach_csv_to_message(message, report_path)
        
        # Send email
        send_email(email_config, message)
        
    except Exception as e:
        logger.error(f"Error in email distribution process: {str(e)}")
        raise

# COMMAND ----------
# Execute main function
if __name__ == "__main__":
    main()

# Usage Examples:
# 1. Workflow Execution:
#    Configure job with parameters:
#    {
#        "report_year": "2024",
#        "report_month": "3",
#        "receiver_email": "user@example.com",
#        "report_limit": "15"
#    }
#
# 2. Manual Execution:
#    - Set report_year widget to "2024"
#    - Set report_month widget to "3"
#    - Set receiver_email widget to "user@example.com"
#    - Set report_limit widget to "15" (optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  b.full_name,total_points from gold.fact_results a inner join gold.dim_driver b on a.driverId=b.driverId  where year(date)=2022 and month(date)=08 order by total_points desc limit 10