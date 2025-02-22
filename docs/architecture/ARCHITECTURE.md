# Formula 1 Data Engineering Architecture

## Overview
This project implements a medallion architecture (Bronze → Silver → Gold) for processing Formula 1 racing data using Azure Databricks.

## Data Architecture

```
Source Data → Bronze → Silver → Gold → Analytics
   (Raw)     (Landing) (Clean)  (Agg)   (Reports)
```

### Storage Layers
- **Source Layer**: Raw files (CSV, JSON, API)
  - Mount: `/mnt/source_layer_gen2/`
  - Purpose: Original data preservation

- **Bronze Layer**: Raw ingestion
  - Mount: `/mnt/bronze_layer_gen2/`
  - Tables: circuits, constructors, drivers, races, results
  - Purpose: Data ingestion and history tracking

- **Silver Layer**: Cleansed data
  - Mount: `/mnt/silver_layer_gen2/`
  - Features: Validated, deduplicated, standardized
  - Purpose: Data quality and consistency

- **Gold Layer**: Business aggregations
  - Mount: `/mnt/gold_layer_gen2/`
  - Tables: fact_results, dim_drivers, dim_circuits, dim_race
  - Purpose: Analytics-ready data models

## Key Components

### Data Pipeline
1. **Ingestion (Bronze)**
   - Raw data capture
   - Partition by ingestion date
   - Audit columns tracking

2. **Processing (Silver)**
   - Data validation
   - Schema enforcement
   - Business rule application

3. **Aggregation (Gold)**
   - Dimensional modeling
   - Performance metrics
   - Reporting views

### Infrastructure
- Storage: Azure Data Lake Gen2
- Compute: Azure Databricks
- Security: Azure Key Vault
- Orchestration: Databricks Workflows

### Data Flow
```
[Source Files] → [Bronze Tables] → [Silver Tables] → [Gold Tables]
     ↓              ↓                   ↓                ↓
   Raw Data     Partitioned      Quality Checked    Aggregated
                  Data             Clean Data        Metrics
```

## Security
- OAuth2 authentication
- Mounted storage with secure credentials
- Secret management via Key Vault
- Role-based access control

## Monitoring
- Pipeline logging
- Data quality metrics
- Job execution tracking
- Email notifications 