# car_sales-databricks-asset-bundles

### Project Description :
This project showcases an end-to-end real-time data engineering solution built on Databricks for streaming car sales data from 2015 to 2025. The pipeline is designed as a star-schema model, with the Car Sales fact table enriched by three dimension tables—Dealers, Branches, and Customers.

Using Delta Live Tables (DLT), the system processes incoming data through bronze, silver, and gold layers, ensuring reliable ingestion, cleansing, and enrichment. Auto Loader enables seamless real-time streaming as new files land in the raw zone. The gold tables serve as the analytics layer, powering interactive dashboards that visualize trends, performance metrics, manufacturer insights, and regional sales behavior.

The entire setup is orchestrated and deployed through a CI/CD workflow using Databricks Asset Bundles, enabling consistent, versioned, and automated promotion across environments. This project demonstrates production-grade practices in streaming ETL, DLT framework adoption, dimensional modeling, and automated deployment pipelines within the Databricks ecosystem.
