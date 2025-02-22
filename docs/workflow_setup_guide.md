# Formula 1 Pipeline Workflow Setup Guide

This guide provides step-by-step instructions for setting up and configuring the Formula 1 data pipeline workflow in Azure Databricks.

## Table of Contents
1. [Prerequisites](#1-prerequisites)
2. [Repository Setup](#2-repository-setup)
3. [Cluster Configuration](#3-cluster-configuration)
4. [Workflow Setup](#4-workflow-setup)
5. [Monitoring and Alerting](#5-monitoring-and-alerting)
6. [Best Practices](#6-best-practices)
7. [Quick Start](#quick-start)
8. [Troubleshooting](#troubleshooting)

## 1. Prerequisites

Before setting up the workflow, ensure you have:
- Azure Databricks workspace with admin or contributor access
- Git repository access with the Formula 1 pipeline code
- Appropriate Azure storage account access
- Required service principals and secrets configured

## 2. Repository Setup

1. In Azure Databricks workspace, go to **Repos**
2. Click **Add Repo**
3. Enter the Git repository URL
4. Configure Git credentials if needed
5. The repository structure should look like:
   ```
   azure-databricks-formula1/
   ├── notebooks/
   │   ├── main_pipeline.py
   │   └── workflow_setup_guide.py
   ├── src/
   │   ├── pipelines/
   │   ├── config/
   │   └── utils/
   └── requirements.txt
   ```

## 3. Cluster Configuration

### Development Cluster
```json
{
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": "2-8",
    "autoscale": true,
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true"
    }
}
```

### Production Cluster
```json
{
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS4_v2",
    "num_workers": "4-16",
    "autoscale": true,
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true",
        "spark.databricks.cluster.profile": "serverless"
    }
}
```

## 4. Workflow Setup

### 4.1 Create New Workflow
1. Navigate to **Workflows** in Azure Databricks
2. Click **Create Job**
3. Configure basic settings:
   - Name: Formula1_Pipeline
   - Type: Multi-task job
   - Job Clusters: Use production cluster configuration

### 4.2 Configure Tasks

#### Option 1: Single Task Pipeline
```json
{
    "name": "formula1_full_pipeline",
    "task_key": "full_pipeline",
    "notebook_task": {
        "notebook_path": "/Repos/azure-databricks-formula1/notebooks/main_pipeline",
        "base_parameters": {
            "environment": "prod",
            "layer_to_process": "all"
        }
    },
    "email_notifications": {
        "on_failure": ["your-team@company.com"]
    }
}
```

#### Option 2: Multi-Task Pipeline (Recommended)
```json
{
    "tasks": [
        {
            "task_key": "bronze_layer",
            "notebook_task": {
                "notebook_path": "/Repos/azure-databricks-formula1/notebooks/main_pipeline",
                "base_parameters": {
                    "environment": "prod",
                    "layer_to_process": "bronze"
                }
            }
        },
        {
            "task_key": "silver_layer",
            "depends_on": [{"task_key": "bronze_layer"}],
            "notebook_task": {
                "notebook_path": "/Repos/azure-databricks-formula1/notebooks/main_pipeline",
                "base_parameters": {
                    "environment": "prod",
                    "layer_to_process": "silver"
                }
            }
        },
        {
            "task_key": "gold_layer",
            "depends_on": [{"task_key": "silver_layer"}],
            "notebook_task": {
                "notebook_path": "/Repos/azure-databricks-formula1/notebooks/main_pipeline",
                "base_parameters": {
                    "environment": "prod",
                    "layer_to_process": "gold"
                }
            }
        }
    ]
}
```

### 4.3 Schedule Configuration
1. Click on **Schedule**
2. Set up schedule based on requirements:
```json
{
    "quartz_cron_expression": "0 0 2 * * ?",  # Run daily at 2 AM
    "timezone_id": "UTC"
}
```

## 5. Monitoring and Alerting

### 5.1 Email Notifications
```json
{
    "email_notifications": {
        "on_start": [],
        "on_success": ["data-team@company.com"],
        "on_failure": ["data-team@company.com", "oncall@company.com"]
    }
}
```

### 5.2 Webhook Notifications (Optional)
```json
{
    "webhook_notifications": {
        "on_failure": [{
            "url": "https://api.pagerduty.com/webhooks/..."
        }]
    }
}
```

### 5.3 Azure Monitor Integration
1. Enable Azure Monitor integration in workspace settings
2. Configure metrics and alerts:
   - Job failure rate
   - Task duration
   - Data quality metrics

## 6. Best Practices

### 6.1 Version Control
- Always work in feature branches
- Use pull requests for code review
- Tag releases for production deployments

### 6.2 Testing
- Test pipeline in dev environment first
- Use small data samples for testing
- Validate data quality between layers

### 6.3 Monitoring
- Set up alerts for critical failures
- Monitor pipeline duration trends
- Track data quality metrics

### 6.4 Documentation
- Keep this guide updated
- Document any configuration changes
- Maintain runbook for common issues

### 6.5 Security
- Use service principals for authentication
- Rotate secrets regularly
- Follow least privilege principle

## Quick Start

1. Clone the repository to Databricks Repos
2. Create clusters using provided configurations
3. Create workflow using Option 2 (Multi-Task Pipeline)
4. Configure notifications
5. Set up schedule
6. Run test execution

## Troubleshooting

### Common Issues

1. **Pipeline Timeout**
   - Check cluster configuration
   - Optimize data processing
   - Adjust timeout settings

2. **Data Quality Failures**
   - Check source data
   - Validate transformations
   - Review error logs

3. **Permission Issues**
   - Verify service principal permissions
   - Check storage access
   - Review workspace access

---

For additional support, contact: data-platform-team@company.com 