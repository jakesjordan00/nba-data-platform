from config.settings import DATABASES, TABLES 
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text, Numeric
from transforms.stint_processor import StintResult
import pyodbc
import logging
import datetime

class SQLConnector:

    def __init__(self, pipeline_name, database_name: str):
        if database_name not in DATABASES:
            raise ValueError(f'Unknown db!')
        self.pipeline_name = pipeline_name
        self.database_name = database_name
        self.config = DATABASES[database_name]
        self.tables = TABLES
        self.engine = self._create_engine()
        self.pyodbc_connection = pyodbc.connect(self._get_pyodbc_connection())
        self.logger = logging.getLogger(f'{pipeline_name}.load')
        self.tag = 'sql'
        


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


                

        return data_transformed #change this to some sort of logging mechanism
    


    def checked_insert(self, table_name: str, data: list):
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
        sql_table = self.tables[table_name]

        test = f'update {table_name} set {' = ?, '.join(col for col in sql_table['update_columns'])} = ?'
        test2 = f'where {' = ? and '.join(sql_table['keys'])} = ?'
        bp = 'here'


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
            cursor.executemany(upsert_string, params)
            self.logger.info(f'{table_name} ╍ Upserted {len(data)} rows')
            cursor.commit()
        except Exception as e:
            self.logger.error({
                'Table': table_name,
                'err_msg': e
            })

    def cursor_query(self, table_name: str, keys: dict) -> dict :
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
            return {}
                

    def stint_cursor(self, stint_keys: dict):  
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
            self.logger.error(f'\nStintPlayer: {stint_player['check_query']}')
            self.logger.error(f'\nStint: {stint['check_query']}')
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





    def _dict_to_params(self, d: dict, keys: list) -> tuple:
        return tuple(d[k.replace('[', '').replace(']', '')] for k in keys)
    

    def _parse_pyodbc_query(self, query: str, params: list):
        queries = []
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
            # pyperclip.copy(query)
            queries.append(query)
