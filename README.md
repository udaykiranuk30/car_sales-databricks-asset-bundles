# car_sales-databricks-asset-bundles

### Project Description :
This project showcases an end-to-end real-time data engineering solution built on Databricks for streaming car sales data from 2015 to 2025. The pipeline is designed as a star-schema model, with the Car Sales fact table enriched by three dimension tables—Dealers, Branches, and Customers.

Using Delta Live Tables (DLT), the system processes incoming data through bronze, silver, and gold layers, ensuring reliable ingestion, cleansing, and enrichment. Auto Loader enables seamless real-time streaming as new files land in the raw zone. The gold tables serve as the analytics layer, powering interactive dashboards that visualize trends, performance metrics, manufacturer insights, and regional sales behavior.

The entire setup is orchestrated and deployed through a CI/CD workflow using Databricks Asset Bundles, enabling consistent, versioned, and automated promotion across environments. This project demonstrates production-grade practices in streaming ETL, DLT framework adoption, dimensional modeling, and automated deployment pipelines within the Databricks ecosystem.

<img width="453" height="29" alt="image" src="https://github.com/user-attachments/assets/8f5764d9-c0c9-4fee-a940-4fe8f1965b4d" />
<img width="438" height="26" alt="image" src="https://github.com/user-attachments/assets/2953691d-4f43-44d2-b5c8-7d3305f5cc51" />
