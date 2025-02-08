Weather Data ETL Pipeline
Project Overview :
This project implements an automated ETL (Extract, Transform, Load) pipeline for weather data processing. It fetches data from the Open-Meteo API, transforms it using Python, and loads it into a PostgreSQL database. The pipeline is orchestrated using Apache Airflow and containerized with Docker.

The pipeline consists of three main components:

Extract: Fetches weather data from Open-Meteo API
Transform: Processes and formats the raw weather data using Python
Load: Stores the formatted data in PostgreSQL database

Technologies Used

Apache Airflow: Workflow orchestration and scheduling
Astronomer: Airflow deployment and management platform
Docker: Containerization and environment management
Python: Core programming language for DAG implementation and data processing
PostgreSQL: Data warehouse for processed weather data
Open-Meteo API: Source for weather data
