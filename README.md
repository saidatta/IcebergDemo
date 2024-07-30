# IcebergDemo Project

## Overview

Welcome to the IcebergDemo Project, a Scala application designed to showcase the capabilities of [Apache Iceberg](https://iceberg.apache.org/). This demo includes creating and managing tables, performing upserts, and executing time travel queries using a Harry Potter character dataset from [Kaggle](https://www.kaggle.com/datasets/electroclashh/harry-potter-dataset/data). Data is stored locally within the resource folder. This project is for demonstration purposes only.

## Prerequisites

Ensure you have the following installed:

- JVM (Version 21)
- Scala (Version 3.4)

## Usage

Launch the application using an IDE or via the console with the following command:

```bash
./gradlew run
```

## Features

- **Database and Table Creation:** Set up a local database and table.
- **Data Insertion:** Insert initial data from the Kaggle dataset into the table.
- **Upsert Operations:** Update existing data in the table with upsert functionality.
- **Table Management and Time Travel:** Manage tables and perform time travel queries to view historical data versions.

Enjoy exploring the capabilities of Apache Iceberg!