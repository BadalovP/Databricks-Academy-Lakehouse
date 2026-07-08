# Lab 1 — Databricks Fundamentals

This folder contains my Lab 1 work for the Databricks Academy Lakehouse project.

The goal of this lab is to practice the basic Databricks development workflow using Unity Catalog, Delta Lake, PySpark, SQL, Git, and Databricks Dashboards.

## Folder Structure

```text
lab_01_fundamentals/
├── Dashboards/
│   └── Parvin - Lab 1 FIFA Dashboard.lvdash.json
├── notebooks/
│   └── lab_01_fundamentals.ipynb
└── README.md
```

## Main Notebook

```text
notebooks/lab_01_fundamentals.ipynb
```

The notebook contains the full Lab 1 implementation:

- Create and use Unity Catalog schema
- Create raw file folders in a Unity Catalog volume
- Load CSV data into Spark
- Create Bronze, Silver, and Gold Delta tables
- Practice PySpark transformations
- Load data from an external API
- Create dashboard-ready tables
- Build a Databricks dashboard

## Dataset

Main dataset:

```text
fifa_world_cup_2026_player_performance.csv
```

The dataset contains FIFA World Cup 2026 player performance data, including:

- Player name, team, nationality, and position
- Match information
- Goals and assists
- Shooting and passing metrics
- Defensive metrics
- Physical performance metrics
- Player ratings and performance scores

## Unity Catalog Setup

Catalog:

```text
dbr_dev
```

Schema:

```text
parvinbadalov
```

Raw files are stored under:

```text
/Volumes/dbr_dev/parvinbadalov/raw_files/lab_01/
```

Example folders:

```text
/Volumes/dbr_dev/parvinbadalov/raw_files/lab_01/csv/
/Volumes/dbr_dev/parvinbadalov/raw_files/lab_01/json/
/Volumes/dbr_dev/parvinbadalov/raw_files/lab_01/api/
```

## Medallion Architecture

The notebook follows a simple medallion architecture.

### Bronze Layer

Bronze table:

```text
dbr_dev.parvinbadalov.lab01_fifa_players_bronze
```

Purpose:

- Load raw CSV data
- Keep original data structure
- Add ingestion metadata

### Silver Layer

Silver table:

```text
dbr_dev.parvinbadalov.lab01_fifa_players_silver
```

Purpose:

- Clean and enrich the data
- Remove duplicate player-match records
- Handle missing values
- Create derived columns

Example derived columns:

```text
goal_contribution
shots_accuracy_pct
xg_overperformance
minutes_category
performance_band
```

### Gold Layer

Gold tables are created for analytics and dashboard reporting.

Example tables:

```text
dbr_dev.parvinbadalov.lab01_fifa_team_summary_gold
dbr_dev.parvinbadalov.lab01_fifa_position_summary_gold
dbr_dev.parvinbadalov.lab01_fifa_top_players_gold
dbr_dev.parvinbadalov.lab01_fifa_stage_summary_gold
dbr_dev.parvinbadalov.lab01_fifa_value_summary_gold
```

Purpose:

- Prepare aggregated data for dashboard visuals
- Analyze team performance
- Analyze player performance
- Analyze position-level performance
- Analyze tournament stage performance
- Analyze player value and performance

## PySpark Transformations Practiced

This lab includes examples of:

```text
select
filter
join
groupBy
agg
withColumn
dropDuplicates
fillna
```

The notebook also includes aggregations such as:

```text
count
countDistinct
sum
avg
max
round
```

## External API

The lab includes a simple external API ingestion example using Open-Meteo.

Example API tables:

```text
dbr_dev.parvinbadalov.lab01_api_weather_bronze
dbr_dev.parvinbadalov.lab01_api_weather_daily_gold
```

Purpose:

- Practice loading JSON data from an external API
- Save API data into Delta tables
- Create a small daily weather summary

## Dashboard

Dashboard name:

```text
Parvin - Lab 1 FIFA Dashboard
```

Dashboard file:

```text
Dashboards/Parvin - Lab 1 FIFA Dashboard.lvdash.json
```

The dashboard includes visuals such as:

- Total goals by team
- Total assists by team
- Total goal contributions by team
- Average player rating by team
- Average performance score by team
- Top 10 scorers
- Top 10 assisters
- Top 10 players by goal contributions
- Weather analysis

## Dashboard Data Sources

The dashboard mainly reads from Gold tables, especially:

```text
dbr_dev.parvinbadalov.lab01_fifa_team_summary_gold
dbr_dev.parvinbadalov.lab01_fifa_top_players_gold
dbr_dev.parvinbadalov.lab01_api_weather_daily_gold
```

Example dashboard query:

```sql
SELECT
  player_name,
  team,
  total_goals
FROM dbr_dev.parvinbadalov.lab01_fifa_top_players_gold
ORDER BY total_goals DESC
LIMIT 10;
```

This query returns the top 10 players by total goals and is used for a dashboard bar chart.

## Lab 1 Output Tables

The notebook creates the following main tables:

```text
dbr_dev.parvinbadalov.lab01_fifa_players_bronze
dbr_dev.parvinbadalov.lab01_fifa_players_silver
dbr_dev.parvinbadalov.lab01_fifa_team_summary_gold
dbr_dev.parvinbadalov.lab01_fifa_position_summary_gold
dbr_dev.parvinbadalov.lab01_fifa_top_players_gold
dbr_dev.parvinbadalov.lab01_fifa_stage_summary_gold
dbr_dev.parvinbadalov.lab01_fifa_value_summary_gold
dbr_dev.parvinbadalov.lab01_api_weather_bronze
dbr_dev.parvinbadalov.lab01_api_weather_daily_gold
```

## Key Learning Outcomes

After completing this lab, I practiced:

- Using Databricks Git folders with GitHub
- Organizing a Databricks project
- Loading files from Unity Catalog volumes
- Creating Delta tables
- Building Bronze, Silver, and Gold layers
- Using PySpark for data transformation
- Creating analytics-ready Gold tables
- Loading data from an external API
- Creating a Databricks dashboard

## Notes

This lab is for Databricks Academy learning practice.  
Some transformations are simplified for educational purposes and are not intended to represent a full production pipeline.
