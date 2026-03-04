import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from GetSchedule import GetSchedule
from Directions import GetGamesInProgress, Wait, GameDictionary
from SQL_Reads import FirstIteration
from GetDataNBA import GetBox, GetPlayByPlay, InsertBox, InsertPbp, UpdateBox
from FirstRunCoDriver import NewGameData, ExistingGameData
from DBConfig import nbaCursor, nbaEngine
import pandas as pd
import time
print('-')


def MainFunction():
    '''
    Function that runs pipeline. Will get Box and/or PlayByPlay data
    
    :param iterations: How many times MainFunction has executed
    :type iterations: int

    :param dbGames: List of Game dictionaries. Contains SeasonID, GameID and a count of the PlayByPlay actions
    :type dbGames: list[dict]
    '''
    #Get the Games in Today's Scoreboard
    print('Getting Scoreboard...')
    dfGames = GetSchedule()
    dbGames = []
    gamesInProgDict =[]
    print(f'{len(dfGames)} Games in progress')
    for i, game in dfGames.iterrows():
        gamesInProgDict.append(GameDictionary(game))
        bp = 'here'
    #Declare Box and PlayByPlay as none
    Box = None
    PlayByPlay = None
    gameIDs = dfGames['GameID'].to_list()
    #If we're on our first iteration or every fifth, see what games exist from the Scoreboard in the Db
    existingGames, programMap = FirstIteration(gamesInProgDict, '')
    existingGameIDs = list(g['GameID']for g in existingGames )
    notInDbGames = [game for game in gamesInProgDict if game['GameID'] not in existingGameIDs]
    if len(notInDbGames) > 0:
        newDbGames, programMap = NewGameData(notInDbGames, programMap, 'ScheduleDriver')
        dbGames.extend(newDbGames)
    if len(existingGames) > 0:
        existingDbGames, programMap = ExistingGameData(existingGames, programMap)
        
    bp = 'here'



# def InsertPlayByPlay():
#     gameList = []
#     games = input('Enter GameID (if multiple, separate with space): ')
#     if ' ' in games or ' ' in games:
#         games = games.replace(',', '').replace(' ', ' ').strip().split(' ')
#     else:
#         games = [int(games)]
#     bp = 'here'
#     for game in games:
#         GameID = game
#         SeasonID = int(f'20{str(GameID)[1:3]}')
#         gameList.append({
#         'SeasonID': SeasonID,
#         'GameID': GameID,
#         'Actions': 0
#         })

#     deleteCmd = ', '.join(str(game['GameID']) for game in gameList)
#     DeleteGames(deleteCmd)
#     ExistingGameData(gameList)





def DeleteGames(deleteCmd: str):
    deleteCmd = f'''
delete from StintPlayer 
where SeasonID = 2025 and GameID in({deleteCmd})

delete from Stint 
where SeasonID = 2025 and GameID in({deleteCmd})

delete from PlayByPlay 
where SeasonID = 2025 and GameID in({deleteCmd})
'''
    nbaCursor.execute(deleteCmd)
    nbaCursor.commit()


def GamesNotInDb():
    query = '''
select s.GameID
from Schedule s
left join Game g on s.SeasonID = g.SeasonID and s.GameID = g.GameID
where g.GameID is null
and s.SeasonID = 2025 and s.GameTimeEST <= getdate()
'''
    GameIDs = []
    dfGames = pd.read_sql(query, nbaEngine)
    for i, game in dfGames.iterrows():
        GameIDs.append(int(game['GameID']))
    return GameIDs
    
def PlayByPlaysNotInDb():
    query = '''
select distinct g.SeasonID, g.GameID, p.GameID pbpGameID
from Game g
left join PlayByPlay p on g.SeasonID = p.SeasonID and g.GameID = p.GameID
where g.GameType != 'PRE' and g.SeasonID = 2025
and p.GameID is null
'''
    GameIDs = []
    dfGames = pd.read_sql(query, nbaEngine)
    for i, game in dfGames.iterrows():
        GameIDs.append(int(game['GameID']))
    return GameIDs
    



def GamesFromInput():
    gameList = [] 
    games = input('Enter GameID (if multiple, separate with space): ')
    dfGames = GetSchedule()
    if ' ' in games or ' ' in games:
        games = games.replace(',', '').replace(' ', ' ').strip().split(' ')
    else:
        games = [int(games)]
    deleteCmd = ', '.join(str(game['GameID']) for game in gameList)
    for i, game in dfGames.iterrows():
        if str(game['GameID']) in games  or game['GameID'] in games:
            gameList.append(GameDictionary(game))
            bp = 'here'
    DeleteGames(deleteCmd)
    programMap = ExistingGameData(gameList, 'ScheduleDriver.ExistingGameData')
    

def GamesReInsertPbp():
    gameList = [] 
    games = PlayByPlaysNotInDb()
    dfGames = GetSchedule()
    for i, game in dfGames.iterrows():
        if str(game['GameID']) in games  or game['GameID'] in games:
            gameList.append(GameDictionary(game))
    programMap = NewGameData(gameList, 'ScheduleDriver.NewGames', 'ScheduleDriver.NewGameData')
    bp = 'here'




def GamesFromSchedule():
    gameList = []
    games = GamesNotInDb()
    dfGames = GetSchedule()
    bp = 'here'
    for i, game in dfGames.iterrows():
        if str(game['GameID']) in games  or game['GameID'] in games:
            gameList.append(GameDictionary(game))
            bp = 'here'
    programMap = NewGameData(gameList, 'ScheduleDriver.NewGames', 'ScheduleDriver.NewGameData')
    bp = 'here'


# MainFunction()
# GamesFromSchedule()
# GamesReInsertPbp()
# InsertPlayByPlay()


GamesReInsertPbp()
# GamesFromSchedule()