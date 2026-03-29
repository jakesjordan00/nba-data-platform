# **NBA Data Platform**



## Table of Contents

- [**NBA Data Platform**](#nba-data-platform)
  - [Table of Contents](#table-of-contents)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Airflow](#airflow)
  - [Local](#local)
- [Project Structure](#project-structure)
- [Pipelines](#pipelines)
  - [ScoreboardPipeline - `scoreboard.py`](#scoreboardpipeline---scoreboardpy)
    - [Execution Behavior](#execution-behavior)
    - [Functions](#functions)
      - [```def __init__```(self, environment: str):](#def-__init__self-environment-str)
      - [```extract```(self):](#extractself)
      - [```transform```(self, data\_extract: *dict*):](#transformself-data_extract-dict)
      - [```load```(self, data\_transformed: *list*):](#loadself-data_transformed-list)
  - [BoxscorePipeline - `boxscore.py`](#boxscorepipeline---boxscorepy)
    - [Execution Behavior](#execution-behavior-1)
    - [Functions](#functions-1)
      - [```def __init__```(self, pipeline\_name: *str*, sc\_data: *dict*, environment: *str*):](#def-__init__self-pipeline_name-str-sc_data-dict-environment-str)
      - [```extract```(self):](#extractself-1)
      - [```transform```(self, data\_extract: *dict*):](#transformself-data_extract-dict-1)
      - [```load```(self, data\_transformed: *list*):](#loadself-data_transformed-list-1)
  - [PlayByPlayPipeline - `playbyplay.py`](#playbyplaypipeline---playbyplaypy)
    - [Execution Behavior](#execution-behavior-2)
    - [Functions](#functions-2)
      - [```def __init__```(self, pipeline\_name: *str*, boxscore\_data: *dict*, db\_actions: *int*, db\_last\_action\_number: *int*, home\_stats: *dict* | *None*, away\_stats: *dict* | *None*, stint\_status: *str*, environment: *str*):](#def-__init__self-pipeline_name-str-boxscore_data-dict-db_actions-int-db_last_action_number-int-home_stats-dict--none-away_stats-dict--none-stint_status-str-environment-str)
      - [```extract```(self):](#extractself-2)
      - [```transform```(self, data\_extract: *dict*):](#transformself-data_extract-dict-2)
      - [```load```(self, data\_transformed: *list*):](#loadself-data_transformed-list-2)

# Installation

## Prerequisites

- Python 3.11+
- SQL Server (with ODBC Driver 17+)

## Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/<your-username>/nba-data-platform.git
   cd nba-data-platform
   ```

2. **Create a virtual environment**

   ```bash
   python -m venv venv
   source venv/bin/activate    #Linux/Mac
   venv\Scripts\activate       #Windows
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

<br>

# Configuration

1. **Create a `.env` file** in the project root:

   ```env
   ServerIP=<your-sql-server-ip>
   AdminPass=<your-sql-server-password>
   ```

2. Database connection details are managed in [`config/settings.py`](config/settings.py).

# Usage

## Airflow
DAGs are located in the [`dags/`](dags/) folder and require **Apache Airflow 3.x**.

Configure Airflow to point to the `dags/` directory via symlink, then trigger or schedule DAGs via the UI or CLI


## Local
Run any of the files in [scripts](/scripts) folder


# Project Structure

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

# Pipelines 

## ScoreboardPipeline - [`scoreboard.py`](pipelines/scoreboard.py)

### Execution Behavior
1. Fetches Games taking place today from NBA static data feed, [todaysScoreboard_00](https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json).
   - Scoreboard resets each day at 12pm EST (Noon). If pulled before then, will show games from last night
  
2. Transforms the result from [todaysScoreboard_00](https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json) to a list of dictionaries containing data for any games *in progress*
   - For each game in our list of game dicts, we'll initialize an instance of the downstream pipelines ([BoxscorePipeline](pipelines/boxscore.py) and [PlayByPlayPipeline](pipelines/playbyplay.py)) with the data transformed in this Pipeline.

### Functions

#### ```def __init__```(self, environment: str):
   - Initializes Schedule pipeline
   - Inherits logger, destination and run_timestamp from superclass
   - Sets url, tag, source, transformer, environment and file_source

#### ```extract```(self):   
   - Fetches Scoreboard data from NBA static data feed, [todaysScoreboard_00](https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json).
   - Returns **data_extract**: Dict containing 'meta' and **'scoreboard'** dicts

#### ```transform```(self, data_extract: *dict*):
   - Given data_extract, returns a list of formatted Scoreboard dictionaries
   - Parameter: **data_extract**
     - Output of fetch()/extract(). Contains game information for today's games
   - Returns **data_transformed**: List of games taking place today that are **In progress** or **Completed**

#### ```load```(self, data_transformed: *list*): 
   - **Does nothing** and returns *untouched* **data_transformed** parameter

---
---
---


## BoxscorePipeline - [`boxscore.py`](pipelines/boxscore.py)

### Execution Behavior

### Functions

#### ```def __init__```(self, pipeline_name: *str*, sc_data: *dict*, environment: *str*):
   - Initializes Boxscore pipeline
   - Inherits logger, destination and run_timestamp from superclass
   - sets GameID, GameIDStr, url, transformer
   - Parameter: **sc_data**
     - From ScoreboardPipeline, a dict of a game in the data_transformed list

#### ```extract```(self):  

#### ```transform```(self, data_extract: *dict*):

#### ```load```(self, data_transformed: *list*): 

---
---
---


## PlayByPlayPipeline - [`playbyplay.py`](pipelines/playbyplay.py)

### Execution Behavior

### Functions

#### ```def __init__```(self, pipeline_name: *str*, boxscore_data: *dict*, db_actions: *int*, db_last_action_number: *int*, home_stats: *dict* | *None*, away_stats: *dict* | *None*, stint_status: *str*, environment: *str*):
   - Initializes PlayByPlay pipeline
   - Inherits logger, destination and run_timestamp from superclass

#### ```extract```(self):  

#### ```transform```(self, data_extract: *dict*):

#### ```load```(self, data_transformed: *list*): 


---
---
---