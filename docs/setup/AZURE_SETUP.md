# Azure Services Setup Guide

## Azure Databricks
### Workspace Configuration
- **Workspace Type**: Premium Tier (required for RBAC)
- **Region**: Choose based on data residency requirements
- **Pricing Tier**: Premium for production workloads
- **Runtime Version**: DBR 13.0 or later (for Delta Lake support)
- **Unity Catalog**: Enabled
- **Git Integration**: Enabled for Azure Repos

### Cluster Configuration
- **Node Type**: Standard_DS4_v2 or similar
- **Autoscaling**: 2-8 workers recommended
- **Runtime**: Include ML Runtime for advanced analytics
- **Advanced Options**:
  - Enable autoscaling
  - Set spark.databricks.delta.preview.enabled=true
  - Enable credential passthrough
  - Enable serverless compute

### Unity Catalog Setup
- **Metastore**: Create new metastore
- **Storage Account**: Dedicated for metadata storage
- **Catalogs**:
  - `formula1_raw`: Source data
  - `formula1_bronze`: Bronze layer
  - `formula1_silver`: Silver layer
  - `formula1_gold`: Gold layer
- **External Locations**: Configure for each storage container
- **Security Model**: 
  - Row-level security enabled
  - Column-level security for sensitive data

### Workflows Configuration
- **Service Principal**: Create dedicated for automation
- **Schedule**: Configure based on data refresh needs
- **Notifications**: Email alerts for failures
- **Dependencies**:
  - Bronze jobs → Silver jobs → Gold jobs
- **Retry Policy**: 
  - Max retries: 3
  - Timeout: 1 hour
- **Job Clusters**: Auto-terminating clusters

### Git Integration
- **Repository**: Azure Repos connection
- **Branch Strategy**:
  - main: Production
  - development: Development
  - feature/*: Feature branches
- **Continuous Integration**: 
  - Pull request validation
  - Automated testing
- **Deployment**: 
  - Environment-specific configurations
  - Release branches

## Azure Data Lake Storage Gen2
### Storage Account
- **Performance Tier**: Standard
- **Replication**: RA-GRS recommended
- **Access Tier**: Hot for active data

### Containers Required
- `source-layer-gen2`: Raw data landing
- `bronze-layer-gen2`: Bronze layer storage
- `silver-layer-gen2`: Silver layer storage
- `gold-layer-gen2`: Gold layer storage

### Hierarchical Namespace
- Enable hierarchical namespace
- Configure POSIX permissions

### Required Permissions
- **Storage Blob Data Owner**: Unity Catalog managed identity
- **Storage Blob Data Contributor**: Data pipeline service principal
- **Storage Blob Data Reader**: Read-only users
- **Storage Account Contributor**: Infrastructure team

## Azure Key Vault
### Basic Setup
- **SKU**: Standard
- **Retention Period**: 90 days recommended
- **Purge Protection**: Enabled

### Required Permissions
- **Secret Officer**: Infrastructure team
- **Secret Reader**: Databricks service principals
- **Crypto Officer**: Security team
- **Certificate Officer**: Security team

### Secrets Required
- `client-id`: Azure AD application ID
- `client-secret`: Azure AD application secret
- `tenantid`: Azure AD tenant ID
- `source-layer-gen2`: ADLS connection string
- `bronze-layer-gen2`: ADLS connection string
- `silver-layer-gen2`: ADLS connection string
- `gold-layer-gen2`: ADLS connection string

## Azure Active Directory
### Application Registration
- Create service principal for Databricks
- Required Permissions:
  - Storage Blob Data Contributor
  - Key Vault Secrets User

### Security Groups
- Data Engineers: Full access
- Data Scientists: Read access to Gold layer
- Analysts: Read access to specific views

### Service Principals Required
1. **Databricks Workspace**
   - Purpose: Main workspace operations
   - Permissions: Storage and Key Vault access
2. **Unity Catalog**
   - Purpose: Metadata management
   - Permissions: Storage management
3. **Data Pipeline**
   - Purpose: Automated workflows
   - Permissions: Data access and processing
4. **CI/CD**
   - Purpose: Deployment automation
   - Permissions: Resource management

## IAM Roles and Permissions Matrix

### Azure Roles
1. **Subscription Level**
   - Owner: Infrastructure team
   - Contributor: DevOps team
   - Reader: Auditors

2. **Resource Group Level**
   - Contributor: DevOps team
   - Custom Role - Data Platform: Data Engineering team
   - Reader: Data Science team

3. **Storage Account Level**
   - Storage Blob Data Owner: Data Platform service principals
   - Storage Blob Data Contributor: Data Engineering team
   - Storage Blob Data Reader: Data Science team

4. **Key Vault Level**
   - Key Vault Administrator: Security team
   - Key Vault Secrets Officer: DevOps team
   - Key Vault Secrets User: Service principals

### Databricks Workspace Roles
1. **Workspace Level**
   - Admin: Platform team
   - User: Data teams
   - Token User: Service accounts

2. **Cluster Level**
   - Cluster Creator: Data Engineering
   - Cluster User: Data Science

3. **Unity Catalog Level**
   - Metastore Admin: Data Platform team
   - Catalog Admin: Data Engineering leads
   - Schema Owner: Data Engineers
   - Table Owner: Data Engineers

### Security Groups
1. **Platform Management**
   - Members: Platform team
   - Roles: Workspace Admin, Metastore Admin

2. **Data Engineering**
   - Members: DE team
   - Roles: Catalog Admin, Cluster Creator

3. **Data Science**
   - Members: DS team
   - Roles: Table Reader, Cluster User

4. **Business Analysts**
   - Members: Analytics team
   - Roles: View Reader, SQL Warehouse User

## Azure Monitor
### Diagnostic Settings
- Enable for all services
- Log Analytics workspace configuration
- Retention period: 30 days minimum

### Alert Rules
- Cluster performance
- Job failures
- Data quality issues
- Storage capacity

## Networking (Optional)
### Virtual Network
- Subnet for Databricks workspace
- Network Security Groups
- Private Endpoints for:
  - Key Vault
  - Storage Account
  - Databricks workspace

## Cost Management
### Budget Setup
- Set monthly budgets
- Configure alerts at 80% threshold
- Track by resource groups

## Deployment Checklist
1. Create Resource Group
2. Deploy Key Vault
3. Create Storage Account
4. Setup Azure AD Application
5. Deploy Databricks Workspace
6. Configure Networking
7. Set up Monitoring
8. Create Alert Rules

## Security Checklist
- [ ] Enable Azure AD authentication
- [ ] Configure RBAC
- [ ] Enable encryption at rest
- [ ] Setup network security
- [ ] Configure audit logging
- [ ] Enable diagnostic settings
- [ ] Review access policies 

## Unity Catalog Deployment Steps
1. Create metastore
2. Configure external locations
3. Create catalogs and schemas
4. Set up access control
5. Migrate existing data
6. Configure audit logging
7. Test permissions

## CI/CD Pipeline Setup
1. Configure Azure Repos integration
2. Set up build pipelines
3. Configure release pipelines
4. Set up environment-specific variables
5. Configure automated testing
6. Set up deployment approval gates
7. Configure rollback procedures 