<h2 align="center">
  Welcome to My Formula 1 Data Processing Project!
  <img src="https://media.giphy.com/media/hvRJCLFzcasrR4ia7z/giphy.gif" width="28">
</h2>

<!-- Intro  -->
<h3 align="center">
        <samp>&gt; Hey There!, I am
                <b><a target="_blank" href="https://yourwebsite.com">Shubham Dalvi</a></b>
        </samp>
</h3>

<p align="center"> 
  <samp>
    <br>
    「 I am a data engineer with a passion for big data, distributed computing, cloud solutions, and data visualization 」
    <br>
    <br>
  </samp>
</p>

<div align="center">
<a href="https://git.io/typing-svg"><img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&pause=1000&random=false&width=435&lines=Azure+Databricks+%7C+Delta+Lake+%7C+Key+Vault+;ADLS+Gen2+%7C+SQL+Database+%7C+Power+BI;3+yrs+of+Professional+Experience+%7C+Data+Engineer+%40+Accenture;Passionate+Data+Engineer+" alt="Typing SVG" /></a>
</div>


<p align="center">
 <a href="https://www.linkedin.com/in/shubham-dalvi-21603316b" target="_blank">
  <img src="https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" alt="yourprofile"/>
 </a>
</p>
<br />

<!-- About Section -->
# About Me

<p>
 <img align="right" width="350" src="/assets/programmer.gif" alt="Coding gif" />
  
 ✌️ &emsp; Enjoy solving data problems <br/><br/>
 ❤️ &emsp; Passionate about big data technologies, cloud platforms, and data visualizations<br/><br/>
 📧 &emsp; Reach me: shubhamdworkmail@gmail.com<br/><br/>
</p>

<br/>

![Formula 1 Data Pipeline](https://github.com/user-attachments/assets/f1-datapipeline-overview)


## Skills and Technologies

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-013243?style=for-the-badge&logo=matplotlib&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-0078D7?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-01172F?style=for-the-badge&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-EA4C89?style=for-the-badge&logo=databricks&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=power-bi&logoColor=black)

<br/>

## Project Overview

This project demonstrates the implementation of an **end-to-end data processing pipeline for Formula 1 data** using Azure Databricks and other Azure services. By following the modern **data lakehouse architecture** with Bronze, Silver, and Gold layers, the solution handles raw data ingestion, cleansing, transformation, and analytics-ready dataset creation for insights.

The architecture integrates **Azure Data Lake Storage Gen2**, **Azure Key Vault**, and **Azure SQL Database**, alongside **Delta Lake** for scalable, secure, and reliable data processing.

## Table of Contents
- [Technologies Used](#technologies-used)
- [Skills Demonstrated](#skills-demonstrated)
- [Azure Architecture](#azure-architecture)
- [Data Flow](#data-flow)
- [Usage Instructions](#usage-instructions)

## Technologies Used
- **Azure Blob Storage**: For scalable and secure object storage.
- **Azure Data Lake Storage Gen2 (ADLS)**: For raw and processed data storage.
- **Azure Databricks**: For data processing and orchestration.
- **Databricks Workflows**: For orchestrating and scheduling Databricks jobs.
- **Delta Lake**: To ensure ACID transactions, schema enforcement, and time travel.
- **Azure DevOps**: For CI/CD and pipeline automation.
- **Azure Key Vault**: For secure secrets management.
- **Azure SQL Database**: For structured data storage.
- **Power BI**: For data visualization and insights.
- **Python**: For scripting and processing logic.

## Skills Demonstrated
- **Cloud Integration**: Using Azure services to build a robust data pipeline.
- **ETL Pipeline Design**: Automating data ingestion, cleansing, and transformation.
- **Data Lakehouse Implementation**: Adopting the Bronze, Silver, and Gold layers for processing.
- **Delta Lake Features**: Leveraging schema validation, time travel, and ACID compliance.
- **Data Visualization**: Creating reports and dashboards in Power BI.
- **Secure Data Handling**: Using Azure Key Vault for managing credentials and secrets.

## Azure Architecture

The architecture follows the modern data lakehouse approach, integrating technologies such as Azure Blob Storage, Azure DevOps, and Databricks Workflows alongside the core services:

![Architecture Diagram](https://github.com/user-attachments/assets/f1-architecture-diagram.png)

1. **Bronze Layer** (Raw Data):
   - Data ingestion from multiple sources: CSV, Excel, Parquet, SQL, APIs, and streaming data.
   - Stored in ADLS Gen2 under `/mnt/bronze_layer_gen2/`.

2. **Silver Layer** (Cleansed Data):
   - Data cleansing, schema validation, and type standardization.
   - Delta Lake ensures ACID compliance.
   - Stored in ADLS Gen2 under `/mnt/silver_layer_gen2/`.

3. **Gold Layer** (Analytics-Ready Data):
   - Fact and dimension tables for business reporting.
   - Stored in ADLS Gen2 under `/mnt/gold_layer_gen2/`.

4. **Visualization and Analytics**:
   - Power BI dashboards and reports for insights.

## Data Flow

## Orchestration and CI/CD Automation
- **Azure DevOps**: Enables CI/CD pipelines, ensuring seamless deployment and integration of data workflows.
- **Databricks Workflows**: Manages job scheduling, task orchestration, and dependency handling within the data pipeline.

### Detailed Data Ingestion Process:
   - Data is ingested into the Bronze layer using Azure Blob Storage and Azure Databricks.
   - Databricks Workflows handle automated scheduling and orchestration, ensuring tasks are executed in the correct sequence.
   - Multiple sources ingested into the Bronze layer.
   - Azure Databricks notebooks handle ingestion for APIs, CSVs, and SQL tables.

2. **Data Cleansing and Transformation**:
   - Bronze-to-Silver processing cleanses raw data and applies schema.
   - Silver-to-Gold processing aggregates and implements business logic.

3. **Analytics and Reporting**:
   - Data is queried from the Gold layer for creating dashboards and insights using Power BI.
   - Power BI connects to Azure Databricks and the Gold layer datasets for interactive reporting.

## Usage Instructions
1. **Setup Azure Resources**:
   - Configure Azure Databricks workspace and clusters.
   - Set up ADLS Gen2 for data storage.
   - Configure Azure Key Vault for secret management.

2. **Implement Data Processing**:
   - Create notebooks for data ingestion and cleansing.
   - Implement Delta Lake tables for data storage.
   - Develop transformation logic for Silver and Gold layers.

3. **Visualization and Reporting**:
   - Use Power BI to connect to Gold layer datasets.
   - Build interactive dashboards for business insights.

4. **Monitor Pipeline**:
   - Use Azure Monitor and Databricks logging for monitoring.

---

Feel free to contribute or reach out if you have any suggestions or improvements!
