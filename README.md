# LocateSmartAI
Relocation Decision Support System

<img width="553" alt="Screenshot 2024-02-20 at 2 34 20 PM" src="https://github.com/shrutimundargi/LocateSmartAI/assets/48567754/8b52fa59-f52d-43ed-b428-0e722342656a">

## Objective
The primary aim of this project is to develop an advanced, intuitive solution to assist individuals and families in identifying optimal relocation destinations. This decision-support system will empower users to make well-informed choices by analyzing key factors that influence relocation decisions.

## Key Factors for Analysis
Crime and Unemployment Rate: Evaluating local job market stability and safety levels to ensure economic security and personal well-being.

Real Estate Prices: Assessing housing affordability to facilitate financial planning and determine cost of living in potential relocation areas.

Quality of Life: Analyzing factors such as school availability, healthcare facilities, dining options, and employment trends to gauge overall quality of life.

## Snowflake Dataset

<img width="601" alt="Screenshot 2024-02-20 at 2 38 58 PM" src="https://github.com/shrutimundargi/LocateSmartAI/assets/48567754/4ce6e001-f8d7-440c-8305-9493afd94761">

All our datasets have been taken from the company Cybersyn, Inc.
Cybersyn, a trailblazing Data-as-a-Service (DaaS) company, is dedicated to making the world's economic data transparent and accessible. The company's mission focuses on serving a diverse clientele that includes governments, businesses, and entrepreneurs, aiming to empower a new generation of decision-makers with comprehensive economic insights.
Datasets we have used:

1. Financial and Economic Analysis:
Cybersyn's Financial & Economic Essentials is a comprehensive product that provides users with an in-depth view of the financial industry's current state. It includes a variety of macroeconomic indicators and banking sector data. The product covers essential topics such as GDP, unemployment rates, housing starts, inflation, interest rates, default rates, and foreign exchange currency pairs.￼
We are using the tables above four tables to create our schema from this dataset.

2. US Housing & Real Estate Essentials:
￼
It serves as a central source of housing and real estate data covering the United States. This product provides housing valuation and financing, migration, addresses, points of interest (POI), and income statistics across the US.

3. Crime Statistics:
￼
Cybersyn's Crime Statistics includes police department crime data from New York, Los Angeles, San Francisco, Houston, Chicago, and Seattle. This dataset has been reformatted to cover the date of occurrence, offense classification/description, and estimated zip code location for crimes.

4. Point of Interest Addresses:
￼
This product serves as a master points of interest (POI), address, and geographic reference dataset. The points of interest data contains the name, location, and category of 11M points of interest ranging from restaurants and commercial brands to hospitals and parks, The address data includes 145M US residential and commercial addresses covering the United States and Puerto Rico. The geographic data contains Cybersyn's standardized geographic entities (e.g. cities, counties), relationships between these geographies (e.g. cities contained within counties) and the characteristics of these geographies (e.g. geospatial boundaries, coordinates, abbreviations).

## Our Dataset

Since we used 4 datasets and a huge amount of tables in total we have created views that acts as our table based on which we proceeded to create the SQL processes, User-defined function, and stored procedure.
￼
### Views:

1. BLS_EMPLOYMENT_V:
This view has information on unemployment rates, new hires, Job Openings and Labor Turnover Surveys of a particular area fetched on an annual basis.

2. CPI_RENT_VIEW:
View for creating a table to get the consumer price index for getting average rent of a particular area on an annual basis.

3. GEO_VIEW:
The ids of each location has been mapped with the name of the place.

4. HOME_MORTGAGE_VIEW:
A view to get an average of total loan amounts, property values, and interest rates for each location on an annual basis

5. URBAN_CRIME_STATISTICS:
Details of criminal incidents occurred at each location on an annual basis.

6. US_POI_VIEW:
Information about the total number of schools, healthcare, cafes in a particular area on an annual basis.

### SQL Processes:

1. CRIME_UNEMPLOYMENT_RATIO:
Finding the crime-to-unemployment ratio of an area to understand the safety of the area, using tables URBAN_CRIME_STATISTICS and BLS_EMPLOYMENT_V

2. EMPLOYMENT_POI_ANALYSIS:
Finding the average employment information and the average of schools, hospitals, food places etc. using tables US_POI_VIEW and BLS_EMPLOYMENT_V

3. HOME_PURCHASE_DATA:
Finding the average of how expensive it would be to live in a location based on average rent and average property value in that area, using tables CPI_RENT_VIEW, HOME_MORTGAGE_VIEW and GEO_VIEW.

### User Defined Functions:
1. BUYING_VS_RENTING:
Function to find if smart to buy or rent a house or a property in a particular area.
￼
2. CRIME_TO_UNEMPLOYMENT_RATIO:
Function to get a ratio between crime rates and unemployment rates to find any correlation.
￼
3. LIFE_QUALITY_INDEX:
Function to calculate the quality of life index based on the total number of services and facilities, job opening, opportunities available in that area.

## Walkthrough Model

<img width="690" alt="Screenshot 2024-02-20 at 2 36 20 PM" src="https://github.com/shrutimundargi/LocateSmartAI/assets/48567754/37efe625-f0d6-47be-8cca-b4993aac89ec">

