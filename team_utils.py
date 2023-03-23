import pymysql
import pandas as pd
import requests
from tqdm import tqdm
import time
import random

tqdm.pandas()

riot_api_key = 'RGAPI-93425bef-e7f4-4559-a315-331d6d1f4450'



'''
mysql
'''


def connect_mysql(db):
    conn = pymysql.connect(host='localhost', user='root', password='1234', db=db,
                           charset='utf8')
    return conn

def mysql_execute(query, conn):
    cursor_mysql = conn.cursor()
    cursor_mysql.execute(query)
    result = cursor_mysql.fetchall()
    return result

def mysql_execute_dict(query, conn):
    cursor_mysql = conn.cursor(cursor = pymysql.cursors.DictCursor)
    cursor_mysql.execute(query)
    result = cursor_mysql.fetchall()
    return result


def get_puuid(user):
    my_url =f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{user}?api_key={riot_api_key}'
    my_res = requests.get(my_url).json()
    my_puuid = my_res['puuid']
    return my_puuid

def get_matchId(puuid,num):
    my_url2=f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?type=ranked&start=0&count={num}&api_key={riot_api_key}'
    my_res_matchId = requests.get(my_url2).json()
    return my_res_matchId

def get_matches_timelines(match_one):
    my_url3 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_one}?api_key={riot_api_key}'
    my_res_match = requests.get(my_url3).json()
    my_url4 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_one}/timeline?api_key={riot_api_key}'
    my_res_match_timeline = requests.get(my_url4).json()
    return my_res_match , my_res_match_timeline

# def my_get_match_timeline(match_one):
#     my_url4 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_one}/timeline?api_key={riot_api_key}'
#     my_res_match_timeline = requests.get(my_url4).json()
#     return my_res_match_timeline





def get_match_timeline_df(df):
    df_creater = []
    info = df.iloc[0].matches['info']['participants']
    columns = ['match_id', 'teamBaronKills', 'gameDuration', 'gameVersion', 'summonerName', 'summonerLevel',
               'participantId'
        , 'championName', 'teamId', 'teamPosition', 'win', 'kills', 'deaths', 'assists'
        , 'totalDamageDealtToChampions', 'totalDamageTaken'
        , 'firstBloodKill', 'cs5', 'cs10', 'cs15', 'cs20', 'cs25'
        , 'ad5', 'ad10', 'ad15', 'ad20', 'ad25'
        , 'ap5', 'ap10', 'ap15', 'ap20', 'ap25']

    print('소환사 스텟 생성중....')
    for i in tqdm(range(len(df))):
        try:
            if df.iloc[i].matches['info']['gameDuration'] > 900:
                # matches 관련된 데이터
                for j in range(len(df.iloc[0].matches['info']['participants'])):
                    lst = []
                    lst.append(df.iloc[i].match_id)
                    if df.iloc[i].matches['info']['participants'][0]['win'] == True:
                        lst.append(df.iloc[i].matches['info']['participants'][0]['challenges']['teamBaronKills'])
                    elif df.iloc[i].matches['info']['participants'][5]['win'] == True:
                        lst.append(df.iloc[i].matches['info']['participants'][5]['challenges']['teamBaronKills'])

                    lst.append(df.iloc[i].matches['info']['gameDuration'])
                    lst.append(df.iloc[i].matches['info']['gameVersion'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['summonerName'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['summonerLevel'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['championName'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['teamId'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['teamPosition'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['win'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['kills'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['deaths'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['assists'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken'])
                    lst.append(df.iloc[i].matches['info']['participants'][j]['firstBloodKill'])
                    # 미니언,ad,ap
                    for k in range(5, 26, 5):
                        try:
                            lst.append(df.iloc[i].timeline['info']['frames'][k]['participantFrames'][str(j + 1)][
                                           'minionsKilled'] +
                                       df.iloc[i].timeline['info']['frames'][k]['participantFrames'][str(j + 1)][
                                           'jungleMinionsKilled'])
                        except:
                            lst.append(0)
                    for m in range(5, 26, 5):
                        try:
                            lst.append(df.iloc[i].timeline['info']['frames'][m]['participantFrames'][str(j + 1)][
                                           'championStats']['attackDamage'])
                        except:
                            lst.append(0)
                    for n in range(5, 26, 5):
                        try:
                            lst.append(df.iloc[i].timeline['info']['frames'][n]['participantFrames'][str(j + 1)][
                                           'championStats']['abilityPower'])
                        except:
                            lst.append(0)
                    df_creater.append(lst)
                    lol_df = pd.DataFrame(df_creater, columns=columns)
        except:
            print(i)
            continue
    return lol_df


def get_rawdata(tier):
    division_list = ['I', 'II', 'III', 'IV']
    lst = []
    page = random.randrange(1, 99)
    print('get_SummonerName....from', page,'page')
    for division in tqdm(division_list):
        url = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}&api_key={riot_api_key}'
        res = requests.get(url).json()
        lst += random.sample(res, 5)
    summonerName_lst = list(map(lambda x: x['summonerName'], lst))

    print('get puuid.....')
    puuid_lst = []
    for i in tqdm(summonerName_lst):
        try:
            puuid_lst.append(get_puuid(i))
        except:
            print(i)
            continue

    print('get_match_id........')
    match_id_lst = []
    for j in tqdm(puuid_lst):
        match_id_lst += get_matchId(j, 3)

    print('get matches & timeline ......')
    df_create = []
    for match_id in tqdm(match_id_lst):
        matches, timeline = get_matches_timelines(match_id)
        time.sleep(2)
        df_create.append([match_id, matches, timeline])

    df = pd.DataFrame(df_create, columns=['match_id', 'matches', 'timeline'])
    print('complete')

    return df

#create 쿼리 부분
    # query = """
    # CREATE TABLE team_lol(match_id varchar(20), teamBaronKills int, gameDuration int,
    # gameVersion varchar(20), summonerName varchar(50),
    # summonerLevel int, participantId int, championName varchar(50), teamId int,
    # teamPosition varchar(20), win bool, kills int, deaths int, assists int,
    # totalDamageDealtToChampions int,
    # totalDamageTaken int,firstBloodKill bool, cs5 int, cs10 int, cs15 int,
    # cs20 int, cs25 int, ad5 int, ad10 int, ad15 int, ad20 int,
    # ad25 int,ap5 int, ap10 int, ap15 int, ap20 int, ap25 int,
    # primary key(match_id, participantId))
    # """

def insert_mysql(x,conn):

    query=(
        f'insert into team_lol(match_id,teamBaronKills,gameDuration,gameVersion,summonerName,summonerLevel,'
        f'participantId,championName,teamId,teamPosition,'
        f'win,kills,deaths,assists,totalDamageDealtToChampions,totalDamageTaken,firstBloodKill,cs5,cs10,cs15,cs20,cs25,'
        f'ad5,ad10,ad15,ad20,ad25,ap5,ap10,ap15,ap20,ap25)'
        f'values({repr(x.match_id)},{x.teamBaronKills},{x.gameDuration},{repr(x.gameVersion)},{repr(x.summonerName)},'
        f'{x.summonerLevel},{x.participantId},{repr(x.championName)},{x.teamId},'
        f'{repr(x.teamPosition)},{x.win},{x.kills},{x.deaths},{x.assists},'
        f'{x.totalDamageDealtToChampions},{x.totalDamageTaken},{x.firstBloodKill},{x.cs5},{x.cs10},{x.cs15},{x.cs20},{x.cs25},{x.ad5},'
        f'{x.ad10},{x.ad15},{x.ad20},{x.ad25},{x.ap5},{x.ap10},{x.ap15},{x.ap20},{x.ap25})'
        f'ON DUPLICATE KEY UPDATE '
        f'match_id  = {repr(x.match_id)},teamBaronKills= {x.teamBaronKills},gameDuration = {x.gameDuration},gameVersion = {repr(x.gameVersion)},summonerName = {repr(x.summonerName)}'
        f',summonerLevel = {x.summonerLevel}, participantId = {x.participantId},championName = {repr(x.championName)},teamId = {x.teamId},teamPosition = {repr(x.teamPosition)}'
        f',win = {x.win}, kills= {x.kills},deaths = {x.deaths},assists = {x.assists},totalDamageDealtToChampions = {x.totalDamageDealtToChampions}'
        f',totalDamageTaken = {x.totalDamageTaken},firstBloodKill={x.firstBloodKill},cs5 ={x.cs5},cs10 ={x.cs10},cs15 ={x.cs15},cs20 ={x.cs20},cs25 ={x.cs25}'
        f',ad5 ={x.ad5},ad10 ={x.ad10},ad15 ={x.ad15},ad20 ={x.ad20},ad25 ={x.ad25},ap5 ={x.ap5},ap10 ={x.ap10},ap15 ={x.ap15},ap20 ={x.ap20},ap25={x.ap25}'
    )
    mysql_execute(query,conn)
    return
