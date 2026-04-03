from config.settings import DATABASES, TABLES 
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text, Numeric
from transforms.stint_processor import StintResult
import pyodbc
import logging
import datetime
import polars as pl
from typing import TypedDict, ClassVar
import types
from dataclasses import dataclass
from sql import query
@dataclass(frozen=True)
class Query:
    name: str
    query: str

class SQLConnector:
    class Queries:
        schedule_api_player_check: ClassVar[Query] = Query(
            name= 'schedule_api_player_check',
            query= query('schedule_api_player_check')
        )
        schedule_api_player_backfill: ClassVar[Query] = Query(
            name = 'schedule_api_player_backfill',
            query = query('schedule_api_player_backfill')
        )
        schedule_api_team_check: ClassVar[Query] = Query(
            name= 'schedule_api_team_check',
            query= query('schedule_api_team_check')
        )
        schedule_api_team_backfill: ClassVar[Query] = Query(
            name= 'schedule_api_team_backfill',
            query= query('schedule_api_team_backfill')
        )
        schedule_backfill: ClassVar[Query] = Query(
            name = 'schedule_backfill',
            query = query('schedule_backfill')
        )

        placeholder: ClassVar[Query] = Query(
            name = '',
            query = ''''''
        )

    def __init__(self, pipeline_name, database_name: str):
        if database_name not in DATABASES:
            raise ValueError(f'Unknown db!')
        self.pipeline_name = pipeline_name
        self.database_name = database_name
        self.config = DATABASES[database_name]
        self.tables = TABLES
        self.engine = self._create_engine()
        self.pyodbc_connection = pyodbc.connect(self._get_pyodbc_connection())
        self.logger = logging.getLogger(f'{pipeline_name}.sql')
        self.tag = 'sql'
        self.queries = self.Queries()

    def _create_engine(self):
        password = quote_plus(self.config['password'])
        connection_string = (
            f"mssql+pyodbc://{self.config['username']}:{password}"
            f"@{self.config['server']}/{self.config['database']}"
            f"?driver=ODBC+Driver+17+for+SQL+Server"
        )
        return create_engine(connection_string)
    
    def _get_pyodbc_connection(self) -> str:
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.config['server']};"
            f"DATABASE={self.config['database']};"
            f"UID={self.config['username']};"
            f"PWD={self.config['password']}"
        )
    
    def initiate_insert(self, data_transformed: dict):
        transformed_dicts = []
        
        for table_name, data in data_transformed.items():
            if type(data) == dict:
                data = [data]
            
            if table_name in['Team', 'Arena', 'Official', 'Player', 'StartingLineups']:
                self.checked_insert(table_name, data)
            elif table_name in['Game', 'GameExt', 'TeamBox', 'PlayerBox']:
                self.checked_upsert(table_name, data)
            elif type(data) == StintResult:
                if data.Stint:
                    self.checked_upsert('Stint', data.Stint)
                else:
                    self.logger.warning('No Stint records to upsert.')
                if data.StintPlayer:
                    self.checked_upsert('StintPlayer', data.StintPlayer)
                else:
                    self.logger.warning('No StintPlayer records to upsert.')
                if data.status:
                    self.checked_upsert('StintStatus', [data.status])
            elif table_name == 'PlayByPlay':
                if len(data) == 0:
                    self.logger.info(f'No new PlayByPlay actions to insert. Skipping...')
                else:
                    self.unchecked_insert(table_name, data)
        return data_transformed
    


    def checked_insert(self, table_name: str, data: list):
        '''`checked_insert`(self, table_name: *str*, data: *list*)
    ---
    <hr>
    
    Given a table name and a list of rows to insert, performs a "checked" insert to the database


    :param str `table_name`: The name of the table to update

    :param list `data`: list of dictionaries that correspond to the values in config/settings.py

        * Each dictionary must match the format of the table's keys (or lookup values), table's columns, the columns to be updated (if necc), and then the keys again
            * This results fills out the *upsert_string* value
            >>> upsert_string = f"""
                if not exists(
                select 1 
                from {table_name}
                where {' = ? and '.join(sql_table['keys'])} = ?
                )
                begin
                insert into {table_name}({', '.join(sql_table['columns'])})
                values({', '.join(['?'] * len(sql_table['columns']))})
                end
                else
                begin
                update {table_name} set {' = ?, '.join(col for col in sql_table['update_columns'])} = ?
                where {' = ? and '.join(sql_table['keys'])} = ?
                end
            """
        '''
        sql_table = self.tables[table_name]
        insert_string = f'''
if not exists(
select 1 
from {table_name}
where {' = ? and '.join(sql_table['keys'])} = ?
)
begin
insert into {table_name}({', '.join(sql_table['columns'])})
values({', '.join(['?'] * len(sql_table['columns']))})
end
'''
        try:
            params = [self._dict_to_params(data_dict, sql_table['keys'] + sql_table['columns']) for data_dict in data]
            cursor = self.pyodbc_connection.cursor()
            cursor.fast_executemany = True
            cursor.executemany(insert_string, params)
            cursor.commit()
            self.logger.info(f'{table_name} ╍ Ignored/inserted {len(data)} rows')
        except Exception as e:
            self.logger.error({
                'Table': table_name,
                'err_msg': e
            })
    

    def unchecked_insert(self, table_name: str, data: list):
        sql_table = self.tables[table_name]
        insert_string = f'''
insert into {table_name}({', '.join(sql_table['columns'])})
values({', '.join(['?'] * len(sql_table['columns']))})
        '''
        try:
            params = [self._dict_to_params(data_dict, sql_table['columns']) for data_dict in data]
            cursor = self.pyodbc_connection.cursor()
            cursor.fast_executemany = True
            cursor.executemany(insert_string, params)
            cursor.commit()
            self.logger.info(f'{table_name} ╍ Inserted {len(data)} rows')
        except Exception as e:
            self.logger.error({
                'Table': table_name,
                'err_msg': e
            })




    
    def checked_upsert(self, table_name: str, data: list):
        '''`checked_upsert`(self, table_name: *str*, data: *list*)
    ---
    <hr>
    
    Given a table name and a list of rows to insert, performs an upsert to database.


    :param str `table_name`: The name of the table to update

    :param list `data`: list of dictionaries that correspond to the values in config/settings.py

        * Each dictionary must match the format of the table's keys (or lookup values), table's columns, the columns to be updated (if necc), and then the keys again
            * This results fills out the *upsert_string* value
            >>> upsert_string = f"""
                if not exists(
                select 1 
                from {table_name}
                where {' = ? and '.join(sql_table['keys'])} = ?
                )
                begin
                insert into {table_name}({', '.join(sql_table['columns'])})
                values({', '.join(['?'] * len(sql_table['columns']))})
                end
                else
                begin
                update {table_name} set {' = ?, '.join(col for col in sql_table['update_columns'])} = ?
                where {' = ? and '.join(sql_table['keys'])} = ?
                end
            """
        '''
        sql_table = self.tables[table_name]
        upsert_string = f'''
if not exists(
select 1 
from {table_name}
where {' = ? and '.join(sql_table['keys'])} = ?
)
begin
insert into {table_name}({', '.join(sql_table['columns'])})
values({', '.join(['?'] * len(sql_table['columns']))})
end
else
begin
update {table_name} set {' = ?, '.join(col for col in sql_table['update_columns'])} = ?
where {' = ? and '.join(sql_table['keys'])} = ?
end
'''
        try:
            params = [self._dict_to_params(data_dict, sql_table['keys'] + sql_table['columns'] + sql_table['update_columns'] + sql_table['keys']) for data_dict in data]
            cursor = self.pyodbc_connection.cursor()
            cursor.fast_executemany = True
            # self._parse_pyodbc_query(upsert_string, params)
            cursor.executemany(upsert_string, params)
            self.logger.info(f'{table_name} ╍ Upserted {len(data)} rows')
            cursor.commit()
        except Exception as e:
            self.logger.error({
                'Table': table_name,
                'err_msg': e
            })
            bp ='here'
        if table_name == 'DailyLineups':
            return data

    def cursor_query(self, table_name: str, keys: dict) -> dict :
        '''cursor_query
    ===
    Given a a SQL table name and a dictionary of keys, this function will execute that table's check_query value and output the results.    

    :param str `table_name`: Name of table to lookup in config/settings.py
    :param dict `keys`: These values will replace the placeholder key values in the table's check_query
    
    <hr>

    Returns
    ===
    
    >>> table_name = 'PlayByPlay'

     *   **When PlayByPlay is passed as table_name, a dictionary containing the *count of rows*, *last_action_number*, and the *stint_status* are returned.**
    * returns **{'actions': actions, 'last_action_number': last_action_number, 'stint_status': stint_status}**

    <br>

    >>> table_name = 'Schedule'
    
    *   **When Schedule is passed, a dictionary containing a list of all *Schedule* games is returned**
    * returns **{'schedule': schedule_list}**

    <br>

    >>> table_name not in ['PlayByPlay', 'Schedule']
    
    *   **Similar to Schedule, a dictionary containg a list of all rows fetched by the query is returned**
    * returns **{'data': data_list}**'''
        sql_table = self.tables[table_name]
        query = sql_table['check_query'].replace('season_id', keys['season_id']).replace('game_id', keys['game_id'])
        cursor = self.pyodbc_connection.cursor()
        if table_name == 'PlayByPlay':
            try:
                cursor.execute(query)
                row = cursor.fetchone()
                actions = row[0] if row else 0
                last_action_number = row[1] if row and row[1] != None else 0
                stint_status = row[2] if row and row[2] not in['', None] else 'failure'
                bp = 'here'
            except Exception as e:
                actions = 0
                test = 1
            return {
                'actions': actions,
                'last_action_number': last_action_number,
                'stint_status': stint_status
            }
        elif table_name == 'Schedule':
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            schedule_list = []
            for row in results:
                schedule_list.append(dict(zip(columns, row)))
            schedule = {'schedule': schedule_list}
            return schedule
        else:
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            data_list = []
            for row in results:
                data_list.append(dict(zip(columns, row)))
            
            return {'data': data_list}
                

    def stint_cursor(self, stint_keys: dict):
        '''stint_cursor
    ---

    <hr>

    Given keys for a game (SeasonID, GameID, HomeID, AwayID), queries the latest Stint/StintPlayer entries for each team.
        - If a game is in progress, this is how we determine who was on the court as of our last Pipeline execution.
        ### Team Stats
            - In the query below, season_id and game_id are replaced with values from stint_keys
            ```
    with TeamsOnCourt as(
    select s.*
         , dense_rank() over(partition by TeamID order by StintID desc) OnCourt
    from Stint s
    where s.SeasonID = season_id and s.GameID = game_id
    )
    select *
    from TeamsOnCourt
    where OnCourt = 1
    order by OnCourt asc, TeamID
        ```
    >>> SeasonID  GameID     TeamID      StintID  QtrStart   QtrEnd ... PtsScored  PtsAllowed  FG2M  FG2A  FG3M  FG3A    
    >>> 2025      22500857   1610612746  19       4          4      ... 0          2           0     0     0     0       
    >>> 2025      22500857   1610612750  21       4          4      ... 2          0           0     0     0     0       
    >>> FGM  FGA  FTM  FTA  OREB  DREB  REB  AST  TOV  STL  BLK  BLKd  F  FDrwn  OnCourt
    >>> 0    0    0    0    0     0     0    0    2    0    0    0     2  0      1
    >>> 0    0    2    2    0     0     0    0    0    1    0    0     0  1      1

    Then the query to get Player stats is run and we retrieve the same stat categories for the Players with each Team's respective StintID 
    
    This allows us to pick up right where we left off and perform clean upserts.

    :param dict stint_keys: These values will replace the placeholder key values in the Stint and StintPlayer check_query variables

    Returns
    ===    
    * home_stats, away_stats
        * From query results, formatted stat dictionaries to match home_stats and away_stats used in StintProcessor
            >>> {'SeasonID': 2025, 'GameID': 22500857, 'TeamID': 1610612746, 'StintID': 19, 'QtrStart': 4, 'QtrEnd': 4, 'ClockStart': '00:42.90', 'ClockEnd': '00:00.00', 'MinElapsedStart': 47.285, 'MinElapsedEnd': 48.0, 'MinutesPlayed': 0.72, 'Possessions': 1, 'PtsScored': 0, 'PtsAllowed': 2, 'FG2M': 0, 'FG2A': 0, 'FG3M': 0, 'FG3A': 0, 'FGM': 0, 'FGA': 0, 'FTM': 0, 'FTA': 0, 'OREB': 0, 'DREB': 0, 'REB': 0, 'AST': 0, 'TOV': 2, 'STL': 0, 'BLK': 0, 'BLKd': 0, 'F': 2, 'FDrwn': 0, 
        'Lineup': {201572: {...}, 201587: {...}, 1627739: {...}, 1627884: {...}, 1631097: {...}}}

    - *None, None*
        * Returns (None, None) tuple if error

        '''
        cursor = self.pyodbc_connection.cursor()
        stint = self.tables['Stint'].copy()
        stint_player = self.tables['StintPlayer'].copy()
        for table in [stint, stint_player]:
            table['check_query'] = table['check_query'].replace('season_id', stint_keys['season_id']).replace('game_id', stint_keys['game_id'])
            
        try:
            cursor.execute(stint['check_query'])
            team_results = cursor.fetchall()
            stint_columns = [col[0] for col in cursor.description if col[0] != 'OnCourt']
            home_stats = dict(zip(stint_columns, next(row for row in team_results if row[2] == stint_keys['home_id'])))
            away_stats = dict(zip(stint_columns, next(row for row in team_results if row[2] == stint_keys['away_id'])))


            cursor.execute(stint_player['check_query'])
            player_results = cursor.fetchall()            
            p_stint_columns = [col[0] for col in cursor.description if col[0] != 'OnCourt']
            
            home_players = {
                row[4]: dict(zip(p_stint_columns, row))
                for row in player_results if row[2] == stint_keys['home_id']
            }
            away_players = {
                row[4]: dict(zip(p_stint_columns, row))
                for row in player_results if row[2] == stint_keys['away_id']
            }

            home_stats['Lineup'] = home_players
            away_stats['Lineup'] = away_players
            return home_stats, away_stats
        except Exception as e:
            self.logger.error(f'Error getting OnCourt Lineups!')
            # self.logger.error(f'\nStintPlayer: {stint_player['check_query']}')
            # self.logger.error(f'\nStint: {stint['check_query']}')
            return None, None


    def delete_rows(self, query: str):
        cursor = self.pyodbc_connection.cursor()
        rows = cursor.execute(query)
        cursor.commit()




    def check_tables(self):
        cursor = self.pyodbc_connection.cursor()
        for table, config in self.tables.items():
            query = config['create']
            result = cursor.execute(query)
            cursor.commit()
            bp = 'here'
            
    def check_specific_table(self, table: str):
        '''`check_specific_table`(self, table)
    ===
    Given the name of a Table, checks for its entry in *self.tables* **(*TABLES* in config/settings.py)**

    If the corresponding dict entry for the table is found, executes that table's 'create' value.    
        - The table creation query behaves as follows:
                **1)** Determine if **Schema** exists
                    **a.** Create Schema if not found
                    **b.** Move on to *Table creation* if found

                **2)** Determine if **Table** exists
                    **a.** Create Table if not found
                    **b.** Skip creation if found

    Parameters
    ---
        <hr>

    - **table** (*str*): Name of table in Database
    '''
        self.logger.info(f'Checking if {table} exists, creating if needed')
        cursor = self.pyodbc_connection.cursor()
        table_config = self.tables[table]
        query = table_config['create']
        result = cursor.execute(query)
        cursor.commit()
        bp = 'here'


    def raw_execute(self, query: str):
        '''`raw_execute`(self, query)
    ===
    Given an Insert, Update or Delete command, will return number of rows affected

    Parameters
    ---
        <hr>

    - **query** (*str*): Query to be executed in Database as plain text

    '''
        cursor = self.pyodbc_connection.cursor()
        db_msg = cursor.execute(query)
        cursor.commit()
        return db_msg.rowcount

    def query_to_dataframe(self, query: Query):
        """query_to_dataframe
===
	<hr>
	
Given a *Query*, will execute the str value of Query.query

>>> class Query:
    name: str
    query: str

Parameters
-------------
<hr>

    __query__ (*Query*): An instance of the Query class, the text of which will be executed and the results output to a polars DataFrame

        schedule_backfill: ClassVar[Query] = Query(
            name = 'schedule_backfill',
            query = query('schedule_backfill')
        )
        example_query = ClassVar[Query] = Query('test', 'select * from Game')




Returns
-------------
<hr>

    __data__ (*pl.DataFrame*): polars DataFrame with results of query
    """
        self.logger.info(f'Running {query.name} query...')
        data = pl.read_database(query.query, self.engine)
        self.logger.info(f'{data.height} rows returned')
        return data



    def _dict_to_params(self, d: dict, keys: list) -> tuple:
        '''_dict_to_params
    ===
        <hr>
        
    Utility function used by **checked_upsert** to format table keys, columns and update_columns with their respective values to parameters

    Parameters
    -------------
    <hr>

        __d__ (*dict*): dict containing the data to be inserted to database
        __keys__ (*list*): list containing the names of keys, columns and update_columns of table receiving insert


    Function Calls
    -------------
    <hr>

    *   **FunctionName()**
        - BulletPoint


    Returns
    -------------
    <hr>

        __      __ (type): Return value and type
        '''
        
        return tuple(d[k.replace('[', '').replace(']', '')] for k in keys)
    

    def _parse_pyodbc_query(self, query: str, params: list):
        queries = []
        import pyperclip
        for tuple in params:
            for value in tuple:
                if type(value) == str:
                    value = f"'{value.replace("'", "''")}'"
                elif type(value) == datetime.datetime:
                    value = f"'{value.strftime('%Y-%m-%d %H:%M:%S.000')}'"
                elif value == None:
                    value = 'null'
                index = query.find('?')
                test = query[index + 1:]
                query = f'{query[:index]}{value}{query[index + 1:]}'
            
            pyperclip.copy(query)
            queries.append(query)

        