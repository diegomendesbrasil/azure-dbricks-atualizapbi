## Updating Power BI Datasets with Databricks

This repository contains a Databricks script designed to automate the updating of datasets in Power BI. This intelligent pipeline takes into account a "freezing" period and ensures dataset updates only occur when appropriate.

### **Highlighted Code**

#### Prerequisites

- Configured Databricks environment.
- Installed `adal` library. You can do this with `!pip install adal`.
- Credentials and secrets for SQL Server and Power BI stored securely in Databricks secrets.

### **Script Overview**

This script performs several important steps:

1. **Connection to SQL Server:** The script connects to the SQL Server database using securely stored credentials in Databricks.

2. **SQL Query for "Freezing" Period:** It executes an SQL query to fetch details about the "freezing" period, ensuring that the update is appropriate.

3. **Validation of the "Freezing" Period:** The code checks if the current time is within the "freezing" period. If it is, the pipeline is halted until after 12:00.

4. **Authentication with Azure AD for Power BI:** The script acquires an Azure Active Directory token for the Power BI service.

5. **Updating the Dataset in Power BI:** With the token in hand, it triggers the dataset update in Power BI.

6. **Monitoring the Update:** The code monitors the status of the update to ensure it completes successfully.

### **Usage Instructions**

This script is intended to be executed from Azure Data Factory, where the values for `datesetId` and `workspaceId` will be passed as parameters. Therefore, when setting up your Data Factory workflow, be sure to provide these appropriate values.

### **Troubleshooting**

- **Credentials and Secrets:** Ensure that the credentials and secrets in your Databricks environment are up-to-date and correct.

- **Permissions:** Ensure that the Databricks environment has the necessary permissions to access the SQL Server database and the Power BI service.

- **Timezone:** Verify that the timezone settings for the SQL Server and the Databricks environment are consistent.

- **Power BI Errors:** In case of errors during the Power BI dataset update, consult the Power BI service for more details about the issue.


Feel free to reach out to Diego Mendes Brasil at m.diegobrasil@outlook.com if you encounter any issues or have any questions. We wish you success in your data processing!
