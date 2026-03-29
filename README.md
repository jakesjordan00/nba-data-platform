# NBA Data Platform

<!-- Brief description of what this project does -->

## Table of Contents

- [NBA Data Platform](#nba-data-platform)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Setup](#setup)
  - [Configuration](#configuration)
  - [Usage](#usage)
  - [Project Structure](#project-structure)
  - [Pipelines](#pipelines)
    - [scoreboard.py](#scoreboardpy)
  - [ScoreboardPipeline](#scoreboardpipeline)
  - [Functions](#functions)

## Installation

### Prerequisites

- Python 3.11+
- SQL Server (with ODBC Driver 17+)

### Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/<your-username>/nba-data-platform.git
   cd nba-data-platform
   ```

2. **Create a virtual environment**

   ```bash
   python -m venv venv
   source venv/bin/activate    # Linux/Mac
   venv\Scripts\activate       # Windows
   ```

3. **Install the package**

   ```bash
   pip install -e .
   ```

   This installs the following dependencies:
   - `polars` / `pandas` - Data processing
   - `sqlalchemy` / `pyodbc` - SQL Server connectivity
   - `requests` - NBA API calls
   - `colorlog` - Logging
   - `python-dotenv` - Environment variable management

4. **Install ODBC Driver** (if not already installed)

   Download and install [Microsoft ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

## Configuration

1. **Create a `.env` file** in the project root:

   ```env
   ServerIP=<your-sql-server-ip>
   AdminPass=<your-sql-server-password>
   ```

2. Database connection details are managed in [`config/settings.py`](config/settings.py).

## Usage

<!-- How to run pipelines, example commands, etc. -->

## Project Structure

```
nba-data-platform/
├── config/          # Settings, API mappings, data mappings
├── connectors/      # API, SQL, and static data connectors
├── dags/            # Pipeline DAGs and run scripts
├── pipelines/       # Core pipeline logic
├── sql/             # SQL queries and table definitions
├── transforms/      # Data transformations
├── docs/            # Documentation
└── scripts/         # Utility scripts
```

## Pipelines

<!-- List and describe each pipeline -->

### scoreboard.py

ScoreboardPipeline
---
Fetches Games taking place today from NBA static data feed.
___
* https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json
* Scoreboard resets each day at 12pm EST (Noon). If pulled before then, will show games from last night

## Functions
    ### ```def __init__```(self, environment: str):
        - Initializes Schedule pipeline
        - Inherits logger, destination and run_timestamp from superclass
        - Sets url, tag, source, transformer, environment and file_source

    ### ```extract```(self):
        https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json
        - Fetches Scoreboard data from NBA's static data feed
            - Returns **data_extract**: Dict containing 'meta' and **'scoreboard'** dicts

    ### ```transform```(self, data_extract):
        - Given data_extract, returns a list of formatted Scoreboard dictionaries
            - Parameter: **data_extract**: Output of fetch()/extract(). Contains game information for today's games
            - Returns **data_transformed**: List of games taking place today that are **In progress** or **Completed**

    ### ```load```(self, data_transformed):
        - Does nothing and returns untouched data_transformed parameter
