# Lab 3 — Streaming & Incremental Ingestion

This folder contains Lab 3 work for the Databricks Academy Lakehouse project.

The lab covers incremental file ingestion with Auto Loader, schema evolution, stream monitoring, checkpoint recovery, and Azure Event Hubs streaming into Bronze and Silver layers.

## Folder Structure

```text
lab_03_streaming_incremental/
├── dbc_notebooks/                                   # Databricks CLI / bundle notebook exports
├── images/                                          # screenshots and diagrams
├── notebooks/
│   ├── lab03_00_setup.ipynb                         # environment and parameter setup
│   ├── lab03_01_file_generation.ipynb               # generate incremental source files
│   ├── lab03_02_autoloader_initial_load.ipynb       # Auto Loader initial load
│   ├── lab03_03_autoloader_schema_evolution.ipynb   # schema evolution handling
│   ├── lab03_04_autoloader_monitoring.ipynb         # stream monitoring and metrics
│   ├── lab03_05_checkpoint_recovery.ipynb           # checkpoint recovery scenarios
│   ├── lab03_06_eventhub_producer.ipynb             # Event Hubs producer setup
│   ├── lab03_07_eventhub_consumer_bronze.ipynb      # Event Hubs → Bronze ingestion
│   └── lab03_08_eventhub_silver.ipynb                 # Bronze → Silver streaming
├── src/                                             # reusable Python modules
├── tests/                                           # unit and integration tests
├── requirements.txt
└── README.md
```

## Notebook Flow

```text
lab03_00_setup
      │
      ▼
lab03_01_file_generation
      │
      ▼
lab03_02_autoloader_initial_load
      │
      ├──► lab03_03_autoloader_schema_evolution
      ├──► lab03_04_autoloader_monitoring
      └──► lab03_05_checkpoint_recovery
      │
      ▼
lab03_06_eventhub_producer
      │
      ▼
lab03_07_eventhub_consumer_bronze
      │
      ▼
lab03_08_eventhub_silver
```

## Status

Not started.
