from sql import query, table

import os
from dotenv import load_dotenv
load_dotenv() 


DATABASES = {
    'JJsNBA':{
        'server': os.getenv('ServerIP'),
        'database': 'JJsNBA', #os.getenv('Database'),
        'username': 'jjAdmin',
        'password': os.getenv('AdminPass')
    }
}

TABLES = {
    'Team':{
        'keys': ['SeasonID', 'TeamID',],
        'columns': ['SeasonID',
            'TeamID',
            'City',
            'Name',
            'Tricode',
            'Wins',
            'Losses',
            'FullName',
            'Conference',
            'Division'
        ],
        'update_columns': ['Wins', 'Losses'],
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'Team'
) begin
create table Team(
SeasonID			int,
TeamID				int,
City				varchar(255),
Name				varchar(255),
Tricode				varchar(255),
Wins				int,
Losses				int,
FullName			varchar(255),
Conference          varchar(255),
Division            varchar(255),
Primary Key(SeasonID, TeamID))
end
'''
    },
    'Arena':{
        'keys': ['SeasonID', 'ArenaID'],
        'columns': [
            'SeasonID',
            'ArenaID',
            'TeamID',
            'City',
            'Country',
            'Name',
            'PostalCode',
            'State',
            'StreetAddress',
            'Timezone'
        ],
        'update_columns': [],
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'Arena'
) begin
create table Arena(
SeasonID			int,
ArenaID				int,
TeamID				int,
City				varchar(255),
Country				varchar(255),
Name				varchar(255),
PostalCode			varchar(255),
State				varchar(255),
StreetAddress		varchar(255),
Timezone			varchar(255),
Primary Key (SeasonID, ArenaID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID))
end
'''
    },
    'Official':{
        'keys': ['SeasonID', 'OfficialID'],
        'columns': [
            'SeasonID',
            'OfficialID',
            'Name',
            'Number'
        ],
        'update_columns': [],
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'Official'
) begin
create table Official(
SeasonID			int,
OfficialID			int,
Name				varchar(255),
Number				varchar(3)
Primary Key(SeasonID, OfficialID))
end
'''
    },
    'Player':{
        'keys': ['SeasonID', 'PlayerID'],
        'columns': [
            'SeasonID',
            'PlayerID',
            'Name',
            'Number',
            'Position',
            'NameInitial',
            'NameLast',
            'NameFirst'
        ],
        'update_columns': [],
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'Player'
) begin
create table Player(
SeasonID			int,
PlayerID			int,
Name				varchar(255),
Number				varchar(3),
Position			varchar(100),
NameInitial			varchar(100),
NameLast			varchar(100),
NameFirst			varchar(100),
Primary Key(SeasonID, PlayerID))
end
'''
    },
    'Game':{
        'keys': ['SeasonID', 'GameID'],
        'columns':[
            'SeasonID',
            'GameID',
            'Date',
            'GameType',
            'HomeID',
            'HScore',
            'AwayID',
            'AScore',
            'WinnerID',
            'WScore',
            'LoserID',
            'LScore',
            'SeriesID',
            'Datetime',
        ],
        'update_columns':[
            'HScore', 
            'AScore', 
            'WinnerID', 
            'WScore', 
            'LoserID', 
            'LScore'
        ],
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'Game'
) begin
create table Game(
SeasonID			int,
GameID				int,
Date				date,
GameType			varchar(10),
HomeID				int,
HScore				int,
AwayID				int,
AScore				int,
WinnerID			int,
WScore				int,
LoserID				int,
LScore				int,
SeriesID			varchar(20),
Datetime			datetime,
Primary Key (SeasonID, GameID),
Foreign Key (SeasonID, HomeID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, AwayID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, WinnerID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, LoserID) references Team(SeasonID, TeamID))
end
'''
    },
    'GameExt':{
        'keys': ['SeasonID', 'GameID'],
        'columns':[
            'SeasonID',
            'GameID',
            'ArenaID', 
            'Attendance', 
            'Sellout', 
            'Label', 
            'LabelDetail', 
            'OfficialID', 
            'Official2ID', 
            'Official3ID', 
            'OfficialAlternateID', 
            'Status', 
            'Periods'
        ],
        'update_columns':[
            'Attendance', 
            'Sellout', 
            'Label', 
            'LabelDetail', 
            'Status', 
            'Periods'
        ],
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'GameExt'
) begin
create table GameExt(
SeasonID			int,
GameID				int,
ArenaID             int,
Attendance			int,
Sellout				int,
Label				varchar(100),
LabelDetail			varchar(100),
OfficialID			int,
Official2ID			int,
Official3ID			int,
OfficialAlternateID int,
Status				varchar(50),
Periods				int,
Primary Key(SeasonID, GameID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, ArenaID) references Arena(SeasonID, ArenaID),
Foreign Key (SeasonID, OfficialID) references Official(SeasonID, OfficialID),
Foreign Key (SeasonID, Official2ID) references Official(SeasonID, OfficialID),
Foreign Key (SeasonID, Official3ID) references Official(SeasonID, OfficialID))
end
'''
    },
    'TeamBox':{
        'keys':['SeasonID', 'GameID', 'TeamID', 'MatchupID'],
        'columns':[
            'SeasonID',
            'GameID',
            'TeamID',
            'MatchupID',
            'Points',
            'PointsAgainst',
            'FG2M',
            'FG2A',
            '[FG2%]',
            'FG3M',
            'FG3A',
            '[FG3%]',
            'FGM',
            'FGA',
            '[FG%]',
            'FieldGoalsEffectiveAdjusted',
            'FTM',
            'FTA',
            '[FT%]',
            'SecondChanceFGM',
            'SecondChanceFGA',
            '[SecondChanceFG%]',
            'TrueShootingAttempts',
            'TrueShootingPercentage',
            'PtsFromTurnovers',
            'PtsSecondChance',
            'PtsInThePaint',
            'PaintFGM',
            'PaintFGA',
            '[PaintFG%]',
            'PtsFastBreak',
            'FastBreakFGM',
            'FastBreakFGA',
            '[FastBreakFG%]',
            'BenchPoints',
            'ReboundsDefensive',
            'ReboundsOffensive',
            'ReboundsPersonal',
            'ReboundsTeam',
            'ReboundsTeamDefensive',
            'ReboundsTeamOffensive',
            'ReboundsTotal',
            'Assists',
            'AssistsTurnoverRatio',
            'BiggestLead',
            'BiggestLeadScore',
            'BiggestScoringRun',
            'BiggestScoringRunScore',
            'TimeLeading',
            'TimesTied',
            'LeadChanges',
            'Steals',
            'Turnovers',
            'TurnoversTeam',
            'TurnoversTotal',
            'Blocks',
            'BlocksReceived',
            'FoulsDrawn',
            'FoulsOffensive',
            'FoulsPersonal',
            'FoulsTeam',
            'FoulsTeamTechnical',
            'FoulsTechnical',
            'Wins',
            'Losses',
            'Win',
            'Seed'
        ],
        'update_columns': [
            'Points',
            'PointsAgainst',
            'FG2M',
            'FG2A',
            '[FG2%]',
            'FG3M',
            'FG3A',
            '[FG3%]',
            'FGM',
            'FGA',
            '[FG%]',
            'FieldGoalsEffectiveAdjusted',
            'FTM',
            'FTA',
            '[FT%]',
            'SecondChanceFGM',
            'SecondChanceFGA', 
            '[SecondChanceFG%]',
            'TrueShootingAttempts',
            'TrueShootingPercentage',
            'PtsFromTurnovers',
            'PtsSecondChance',
            'PtsInThePaint',
            'PaintFGM',
            'PaintFGA',
            '[PaintFG%]',
            'PtsFastBreak',
            'FastBreakFGM',
            'FastBreakFGA',
            '[FastBreakFG%]',
            'BenchPoints',
            'ReboundsDefensive',
            'ReboundsOffensive',
            'ReboundsPersonal',
            'ReboundsTeam',
            'ReboundsTeamDefensive',
            'ReboundsTeamOffensive',
            'ReboundsTotal',
            'Assists',
            'AssistsTurnoverRatio',
            'BiggestLead',
            'BiggestLeadScore',
            'BiggestScoringRun',
            'BiggestScoringRunScore',
            'TimeLeading',
            'TimesTied',
            'LeadChanges',
            'Steals',
            'Turnovers',
            'TurnoversTeam',
            'TurnoversTotal',
            'Blocks',
            'BlocksReceived',
            'FoulsDrawn',
            'FoulsOffensive',
            'FoulsPersonal',
            'FoulsTeam',
            'FoulsTeamTechnical',
            'FoulsTechnical',
            'Wins', 
            'Losses',
            'Win',
            'Seed'
        ],
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'TeamBox'
) begin
create table TeamBox(
SeasonID					int,
GameID						int,
TeamID						int,
MatchupID					int,
--Points
Points int,
PointsAgainst int,
--Field Goals			
FG2M int,
FG2A int,	
[FG2%] float,
FG3M int,
FG3A int,
[FG3%] float,
FGM int,
FGA int,
[FG%] float,
FieldGoalsEffectiveAdjusted float,
FTM int,
FTA int,
[FT%]				float,
SecondChanceFGM int,
SecondChanceFGA int,
[SecondChanceFG%] float,
TrueShootingAttempts float,
TrueShootingPercentage float,
PtsFromTurnovers int,
PtsSecondChance int,
PtsInThePaint int,
PaintFGM int,
PaintFGA int,
[PaintFG%] float,
PtsFastBreak int,
FastBreakFGM int,
FastBreakFGA int,
[FastBreakFG%] float,
BenchPoints int,
--Rebounds
ReboundsDefensive int,
ReboundsOffensive int,
ReboundsPersonal int,
ReboundsTeam int,
ReboundsTeamDefensive int,
ReboundsTeamOffensive int,
ReboundsTotal int,
Assists int,
AssistsTurnoverRatio float,
BiggestLead int,
BiggestLeadScore varchar(30),
BiggestScoringRun int,
BiggestScoringRunScore varchar(30),
TimeLeading varchar(30),			
TimesTied int,
LeadChanges int,
Steals int,
--Turnovers
Turnovers int,
TurnoversTeam int,
TurnoversTotal int,
Blocks int,
BlocksReceived int,
FoulsDrawn int,
FoulsOffensive int,
FoulsPersonal int,
FoulsTeam int,
FoulsTeamTechnical int,
FoulsTechnical int,
Wins int,
Losses int,
Win	int,
Seed int,
Primary Key (SeasonID, GameID, TeamID, MatchupID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, MatchupID) references Team(SeasonID, TeamID))
end
'''
    },
    'PlayerBox':{
        'keys': ['SeasonID', 'GameID', 'TeamID', 'MatchupID', 'PlayerID'],
        'columns': [
            'SeasonID',
            'GameID',
            'TeamID',
            'MatchupID',
            'PlayerID',
            'Status',
            'Starter', 
            'Position', 
            'Minutes', 
            'MinutesCalculated', 
            'Points',
            'Assists',
            'ReboundsTotal',
            'FG2M',
            'FG2A',
            '[FG2%]',
            'FG3M', 
            'FG3A',
            '[FG3%]',
            'FGM', 
            'FGA', 
            '[FG%]', 
            'FTM', 
            'FTA', 
            '[FT%]', 
            'ReboundsDefensive', 
            'ReboundsOffensive', 
            'Blocks', 
            'BlocksReceived', 
            'Steals', 
            'Turnovers', 
            'AssistsTurnoverRatio', 
            'Plus', 
            'Minus', 
            'PlusMinusPoints',
            'PtsFastBreak',
            'PtsInThePaint',
            'PtsSecondChance', 
            'FoulsOffensive', 
            'FoulsDrawn', 
            'FoulsPersonal', 
            'FoulsTechnical', 
            'StatusReason', 
            'StatusDescription'

        ],
        'update_columns': [
            'Status',
            'Starter',
            'Position',
            'Minutes',
            'MinutesCalculated',
            'Points',
            'Assists',
            'ReboundsTotal',
            'FG2M',
            'FG2A',
            '[FG2%]',
            'FG3M',
            'FG3A',
            '[FG3%]',
            'FGM',
            'FGA',
            '[FG%]',
            'FTM',
            'FTA',
            '[FT%]',
            'ReboundsDefensive',
            'ReboundsOffensive',
            'Blocks',
            'BlocksReceived',
            'Steals',
            'Turnovers',
            'AssistsTurnoverRatio',
            'Plus',
            'Minus',
            'PlusMinusPoints',
            'PtsFastBreak',
            'PtsInThePaint',
            'PtsSecondChance',
            'FoulsOffensive',
            'FoulsDrawn',
            'FoulsPersonal',
            'FoulsTechnical',
            'StatusReason',
            'StatusDescription'
        ],
        'create': '''
if not exists(
select 1 
from sys.tables t 
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'PlayerBox' and s.name = 'dbo'
) begin
create table PlayerBox(
SeasonID					int,
GameID						int,
TeamID						int,
MatchupID					int,
PlayerID					int,
Status						varchar(20),
Starter						int,
Position					varchar(2),
Minutes						varchar(30),
MinutesCalculated			float,
Points						int,
Assists						int,
ReboundsTotal				int,
FG2M						int,
FG2A						int,
[FG2%]						float,
FG3M						int,
FG3A						int,
[FG3%]						float,
FGM							int,
FGA							int,
[FG%]						float,
FTM							int,
FTA							int,
[FT%]						float,
ReboundsDefensive			int,
ReboundsOffensive			int,
Blocks						int,
BlocksReceived				int,
Steals						int,
Turnovers					int,
AssistsTurnoverRatio		float,
Plus						float,
Minus						float,
PlusMinusPoints				float,
PtsFastBreak				int,
PtsInThePaint			int,
PtsSecondChance			int,
FoulsOffensive				int,
FoulsDrawn					int,
FoulsPersonal				int,
FoulsTechnical				int,
StatusReason				varchar(100),
StatusDescription			varchar(200),
Primary Key(SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, PlayerID) references Player(SeasonID, PlayerID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID) references TeamBox(SeasonID, GameID, TeamID, MatchupID))
end
'''
    },    
    'StartingLineups':{
        'keys': ['SeasonID', 'GameID', 'TeamID', 'MatchupID', 'PlayerID'],
        'columns': [
            'SeasonID',
            'GameID',
            'TeamID',
            'MatchupID',
            'PlayerID',
            'Unit',
            'Position'
        ],
        'update_columns': [],
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'StartingLineups'
) begin
create table StartingLineups(
SeasonID		int,
GameID			int,
TeamID			int,
MatchupID		int,
PlayerID		int,
Unit			varchar(30),
Position		varchar(10),
Primary Key(SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID),
Foreign Key (SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign Key (SeasonID, PlayerID) references Player(SeasonID, PlayerID),
Foreign Key (SeasonID, GameID, TeamID, MatchupID, PlayerID) references PlayerBox(SeasonID, GameID, TeamID, MatchupID, PlayerID))
end
'''
    },
    'PlayByPlay':{
        # 'keys': ['SeasonID', 'GameID', 'ActionID', 'ActionNumber'],
        'keys': [],
        'columns': [
            'SeasonID',
            'GameID',
            'ActionID',
            'ActionNumber',
            'Qtr',
            'Clock',
            'TimeActual',
            'ScoreHome',
            'ScoreAway',
            'Possession',
            'TeamID',
            'Tricode',
            'PlayerID',
            'Description',
            'SubType',
            'IsFieldGoal',
            'ShotResult',
            'ShotValue',
            'ActionType',
            'ShotDistance',
            'Xlegacy',
            'Ylegacy',
            'X',
            'Y',
            'Location',
            'Area',
            'AreaDetail',
            'Side',
            'ShotType',
            'PtsGenerated',
            'Descriptor',
            'Qual1',
            'Qual2',
            'Qual3',
            'ShotActionNbr',
            'PlayerIDAst',
            'PlayerIDBlk',
            'PlayerIDStl',
            'PlayerIDFoulDrawn',
            'PlayerIDJumpW',
            'PlayerIDJumpL',
            'OfficialID',
            'QtrType'
        ],
        'update_columns': [],
        'check_query': '''select count(p.ActionID) Actions
, max(ActionNumber) LastActionNumber
, s.Status
from PlayByPlay p
left join StintStatus s on p.SeasonID = s.SeasonID and p.GameID = s.GameID
where p.SeasonID = season_id and p.GameID = game_id
group by s.Status''',
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'PlayByPlay'
) begin
create table PlayByPlay(
SeasonID			int,
GameID				int,
ActionID			int,
ActionNumber		int,
Qtr					int,
Clock				varchar(20),
TimeActual			datetime,
ScoreHome			int,
ScoreAway			int,
Possession			int,
TeamID				int,
Tricode				varchar(3),
PlayerID			int,
Description			varchar(999),
SubType				varchar(999),
IsFieldGoal			int,
ShotResult			varchar(999),
ShotValue			int,
ActionType			varchar(999),
ShotDistance		float,
Xlegacy				float,
Ylegacy				float,
X					float,
Y					float,
Location			varchar(35),
Area				varchar(50),
AreaDetail			varchar(50),
Side				varchar(30),
ShotType			varchar(4),
PtsGenerated		int,
Descriptor			varchar(30),
Qual1				varchar(30),
Qual2				varchar(30),
Qual3				varchar(30),
ShotActionNbr		int,
PlayerIDAst			int,
PlayerIDBlk			int,
PlayerIDStl			int,
PlayerIDFoulDrawn	int,
PlayerIDJumpW		int,
PlayerIDJumpL		int,
OfficialID			int,
QtrType				varchar(20),
Primary Key(SeasonID, GameID, ActionNumber, ActionID),
Foreign Key (SeasonID, GameID) references Game(SeasonID, GameID))
end
'''
    },
    'StintStatus':{
        'keys': ['SeasonID', 'GameID'],
        'columns': [
            'SeasonID',
            'GameID'
        ],
        'update_columns': ['status'],
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'StintStatus'
) begin
create table StintStatus(
SeasonID int not null,
GameID int not null,
Status varchar(20),
primary key(SeasonID, GameID))
end
'''
    },
    'Stint':{
        'keys': ['SeasonID', 'GameID', 'TeamID', 'StintID'],
        'columns': [
            'SeasonID',
            'GameID',
            'TeamID',
            'StintID',
            'QtrStart',
            'QtrEnd',
            'ClockStart',
            'ClockEnd',
            'MinElapsedStart',
            'MinElapsedEnd',
            'MinutesPlayed',
            'Possessions',
            'PtsScored',
            'PtsAllowed',
            'FG2M',
            'FG2A',
            'FG3M',
            'FG3A',
            'FGM',
            'FGA',
            'FTM',
            'FTA',
            'OREB',
            'DREB',
            'REB',
            'AST',
            'TOV',
            'STL',
            'BLK',
            'BLKd',
            'F',
            'FDrwn'
        ],
        'update_columns': [
            'QtrEnd',
            'ClockEnd',
            'MinElapsedEnd',
            'MinutesPlayed',
            'Possessions',
            'PtsScored',
            'PtsAllowed',
            'FG2M',
            'FG2A',
            'FG3M',
            'FG3A',
            'FGM',
            'FGA',
            'FTM',
            'FTA',
            'OREB',
            'DREB',
            'REB',
            'AST',
            'TOV',
            'STL',
            'BLK',
            'BLKd',
            'F',
            'FDrwn'
        ],
        'check_query': '''
with TeamsOnCourt as(
select s.*
, dense_rank() over(partition by TeamID order by StintID desc) OnCourt
from Stint s
where s.SeasonID = season_id and s.GameID = game_id
)
select *
from TeamsOnCourt
where OnCourt = 1
order by OnCourt asc,TeamID''',
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'Stint'
) begin
create table Stint(
SeasonID int not null,
GameID int not null,
TeamID int not null,
StintID int not null,
QtrStart int,
QtrEnd int,
ClockStart varchar(10),
ClockEnd varchar(10),
MinElapsedStart float,
MinElapsedEnd float,
MinutesPlayed float,
Possessions int,
PtsScored int,
PtsAllowed int,
FG2M int,
FG2A int,
FG3M int,
FG3A int,
FGM int,
FGA int,
FTM int,
FTA int,
OREB int,
DREB int,
REB int,
AST int,
TOV int,
STL int,
BLK int,
BLKd int,
F int,
FDrwn int,
primary key(SeasonID, GameID, TeamID, StintID))
end
'''
    },
    'StintPlayer':{
        'keys': ['SeasonID', 'GameID', 'TeamID', 'StintID', 'PlayerID'],
        'columns': [
            'SeasonID',
            'GameID',
            'TeamID',
            'StintID',
            'PlayerID',
            'MinutesPlayed',
            'PlusMinus',
            'PTS',
            'AST',
            'REB',
            'FG2M',
            'FG2A',
            'FG3M',
            'FG3A',
            'FGM',
            'FGA',
            'FTM',
            'FTA',
            'OREB',
            'DREB',
            'TOV',
            'STL',
            'BLK',
            'BLKd',
            'F',
            'FDrwn'
        ],
        'update_columns': [
            'MinutesPlayed',
            'PlusMinus',
            'PTS',
            'AST',
            'REB',
            'FG2M',
            'FG2A',
            'FG3M',
            'FG3A',
            'FGM',
            'FGA',
            'FTM',
            'FTA',
            'OREB',
            'DREB',
            'TOV',
            'STL',
            'BLK',
            'BLKd',
            'F',
            'FDrwn'
        ],
        'check_query': '''
with PlayersOnCourt as(
select sp.*
, dense_rank() over(partition by TeamID order by StintID desc) OnCourt
from StintPlayer sp
where sp.SeasonID = season_id and sp.GameID = game_id
)
select *
from PlayersOnCourt
where OnCourt = 1
order by OnCourt asc,TeamID, PlayerID
''',
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'StintPlayer'
) begin
create table StintPlayer(
SeasonID int not null,
GameID int not null,
TeamID int not null,
StintID int not null,
PlayerID int not null,
MinutesPlayed float,
PlusMinus int,
PTS int,
AST int,
REB int,
FG2M int,
FG2A int,
FG3M int,
FG3A int,
FGM int,
FGA int,
FTM int,
FTA int,
OREB int,
DREB int,
TOV int,
STL int,
BLK int,
BLKd int,
F int,
FDrwn int,
primary key(SeasonID, GameID, TeamID, StintID, PlayerID),
foreign key(SeasonID, GameID, TeamID, StintID) references Stint(SeasonID, GameID, TeamID, StintID))
end
'''
    },
    'Schedule': {
        'check_query': '''
select s.SeasonID
, s.GameID
, s.Status
, s.HomeID
, h.Name HomeName
, h.City HomeCity
, h.Tricode HomeTri
, s.HomeWins
, s.HomeLosses
, s.HomeScore
, s.HomeSeed
, s.AwayID
, a.Name AwayName
, a.City AwayCity
, a.Tricode AwayTri
, s.AwayWins
, s.AwayLosses
, s.AwayScore
, s.AwaySeed
, s.IfNecessary
, s.SeriesText
from Schedule s
left join Team h on s.SeasonID = h.SeasonID and s.HomeID = h.TeamID
left join Team a on s.SeasonID = a.SeasonID and s.AwayID = a.TeamID
where s.SeasonID = season_id and s.GameID in(game_id)
''',
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'Schedule'
) begin
create table Schedule(
SeasonID		int,
GameID			int,
GameType		varchar(10),
Status			varchar(255),
Sequence		int,
GameTimeEST		datetime,
GameTimeUTC		datetime,
GameTimeHome	datetime,
GameTimeAway	datetime,
Day				varchar(255),
Week			int,
Label			varchar(255),
LabelDetail		varchar(255),
IsNeutral		int,
HomeID			int,
HomeWins		int,
HomeLosses		int,
HomeScore		int,
HomeSeed		int,
AwayID			int,
AwayWins		int,
AwayLosses		int,
AwayScore		int,
AwaySeed		int,
IfNecessary		int,
SeriesText		varchar(255),
Primary Key (SeasonID, GameID))
end
'''
    },
    'DailyLineups': {
        'keys': ['SeasonID', 'GameID', 'TeamID', 'MatchupID', 'PlayerID'],
        'columns': [
            'SeasonID', 
            'GameID', 
            'TeamID', 
            'MatchupID', 
            'PlayerID', 
            'Position',
            'LineupStatus',
            'RosterStatus',
            'Timestamp'
        ],
        'update_columns': [
            'Position',
            'LineupStatus',
            'RosterStatus',
            'Timestamp'
        ],
        'create': '''
if not exists(
select 1 from sys.tables t where t.name = 'DailyLineups'
) 
begin
create table DailyLineups(
SeasonID        int,
GameID          int,
TeamID          int,
MatchupID       int,
PlayerID        int,
Position        varchar(10),
LineupStatus    varchar(20),
RosterStatus    varchar(20),
Timestamp       datetime,
Primary key (SeasonID, GameID, TeamID, MatchupID, PlayerID),
Foreign key(SeasonID, TeamID) references Team(SeasonID, TeamID),
Foreign key(SeasonID, MatchupID) references Team(SeasonID, TeamID),
Foreign key(SeasonID, PlayerID) references Player(SeasonID, PlayerID))
end
'''
    },
    'adv.PlayerBox': {
        'keys': ['SeasonID', 'GameID', 'TeamID', 'MatchupID', 'PlayerID'],
        'columns': [
            'SeasonID','GameID', 'TeamID', 'MatchupID', 'PlayerID',
            'Minutes',
            'OffRTG',
            'DefRTG',
            'NetRTG',
            '[Ast%]',
            'ATR',
            'AstRatio',
            '[OReb%]',
            '[DReb%]',
            '[Reb%]',
            '[TeamTO%]',
            '[EFG%]',
            '[TS%]',
            '[Usage%]',
            'Pace',
            'PacePer40',
            'PIE',
            'POSS',
            'FGM',
            'FGA',
            '[FG%]',
        ],
        'update_columns': [
            'Minutes',
            'OffRTG',
            'DefRTG',
            'NetRTG',
            '[Ast%]',
            'ATR',
            'AstRatio',
            '[OReb%]',
            '[DReb%]',
            '[Reb%]',
            '[TeamTO%]',
            '[EFG%]',
            '[TS%]',
            '[Usage%]',
            'Pace',
            'PacePer40',
            'PIE',
            'POSS',
            'FGM',
            'FGA',
            '[FG%]',
        ],
        'create': table('adv.PlayerBox')
    },
#     'placeholder': {
#         'keys': [],
#         'columns': [
#         ],
#         'update_columns': [
#         ],
#         'create': '''
# if not exists(
# select 1 from sys.tables t where t.name = 'DailyLineups'
# ) 
# begin

# end
# '''    }
}