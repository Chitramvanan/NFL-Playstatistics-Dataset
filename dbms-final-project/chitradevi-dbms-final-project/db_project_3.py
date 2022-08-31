# !/usr/bin/env python
# coding: utf-8

# In[11]:


import psycopg2
import pandas as pd
import time


def initialize():
    connection = psycopg2.connect(
        user="postgres",  # username that you use
        password="feb102016",  # password that you use
        host="localhost",
        port="5432",
        database="postgres"
    )
    connection.autocommit = True
    return connection


def runQuery1(conn):
    print('\n    Q1) Finding out the DOB and highest visiting team final score of Steven Miller.  \n')
    select_Query = "select gp.nameFull,gp.dob,MAX(g.visitingteamfinalscore)AS highestVisitingteamFinalscore from games g INNER JOIN gameParticipation gp ON g.gameid = gp.gameid  GROUP BY gp.nameFull,gp.dob Having gp.nameFull = 'Steven Miller'"
    highestVisitingteamFinalscore_df = pd.DataFrame(columns=['nameFull', 'dob', 'highestvisitingteamfinalscore'])

    with conn.cursor() as cursor:
        cursor.execute(select_Query)
        records = cursor.fetchall()
        for row in records:
            output_df = {'nameFull': row[0], 'dob': row[1], 'highestvisitingteamfinalscore': row[2]}
            highestVisitingteamFinalscore_df = pd.concat(
                [highestVisitingteamFinalscore_df, pd.DataFrame.from_records([output_df])])

        print(highestVisitingteamFinalscore_df)


def runQuery2(conn):
    print('\n       Q2) Finding out the lowest five tackle yds scrim with tackle        \n')
    select_Query = "select distinct  tackletype,tackleydsscrim from tackles order by tackleydsscrim,tackletype desc limit 5"
    tackletype_df = pd.DataFrame(columns=['tackletype', 'tackleydscrim'])

    with conn.cursor() as cursor:
        cursor.execute(select_Query)
        records = cursor.fetchall()
        for row in records:
            output_df = {'tackletype': row[0], 'tackleydscrim': row[1]}
            tackletype_df = pd.concat([tackletype_df, pd.DataFrame.from_records([output_df])])

        print(tackletype_df)


def runQuery3(conn):
    print('\n        Q3) Find game participant name, unit and snap count for player who lives in Vermont       \n')
    select_Query = "select gameid,nameFirst,gamePartUnit,gamepartSnapCount from gameparticipation where homeState ='VT'"
    vermontgameParticipant_df = pd.DataFrame(columns=['gameid', 'nameFirst', 'gamePartUnit', 'gamepartSnapCount'])

    with conn.cursor() as cursor:
        cursor.execute(select_Query)
        records = cursor.fetchall()
        for row in records:
            output_df = {'gameid': row[0], 'nameFirst': row[1], 'gamePartUnit': row[2], 'gamepartSnapCount': row[3]}
            vermontgameParticipant_df = pd.concat([vermontgameParticipant_df, pd.DataFrame.from_records([output_df])])

        print(vermontgameParticipant_df)


def runQuery4a(conn):
    print(
        '\n   Q4a) List the name of the college for the players in the SF 25 games field position and the type of play is field goal    \n')

    select_Query = "select distinct gp.college from gameparticipation gp INNER JOIN games g on gp.gameid = g.gameid INNER join plays p on p.gameid = g. gameid where p.fieldposition = 'SF 25' and p.playtype = 'field goal'"
    college_df = pd.DataFrame(columns=['college'])

    with conn.cursor() as cursor:
        ms = time.time_ns() / 1e6
        print('Time stamp before Query execution:', ms, 'milliseconds')
        cursor.execute(select_Query)
        ms1 = time.time_ns() / 1e6
        print('Time stamp after Query execution:', ms1, 'milliseconds')
        ms_diff = ms1 - ms
        records = cursor.fetchall()
        for row in records:
            output_df = {'college': row[0]}
            college_df = pd.concat([college_df, pd.DataFrame.from_records([output_df])])

        print(college_df)
        print('Query execution time:', ms_diff, 'milliseconds')


def runQuery4b(conn):
    print('\n        Q4b) second query for question 4a        \n')
    select_Query = "select distinct gp.college from gameparticipation gp where gp.gameid IN (select p.gameid from plays p where p.fieldposition = 'SF 25' and p.playtype = 'field goal')"
    collegeSubQuery_df = pd.DataFrame(columns=['college'])

    with conn.cursor() as cursor:
        ms = time.time_ns() / 1e6
        print('Time stamp before Query execution:', ms, 'milliseconds')
        cursor.execute(select_Query)
        ms1 = time.time_ns() / 1e6
        print('Time stamp after Query execution:', ms1, 'milliseconds')
        ms_diff = ms1 - ms
        records = cursor.fetchall()
        for row in records:
            output_df = {'college': row[0]}
            collegeSubQuery_df = pd.concat([collegeSubQuery_df, pd.DataFrame.from_records([output_df])])

        print(collegeSubQuery_df)
        print('Query execution time:', ms_diff, 'milliseconds')


def runQuery5a(conn):
    print('\n        Q5a) Finding out the player’s name and the age(s) of the youngest players      \n')
    select_Query = "select distinct gp.nameFull,gp.ageatdraft FROM gameParticipation gp WHERE gp.ageatdraft = (SELECT MIN(ageatdraft) FROM gameparticipation gp2)"
    college_df = pd.DataFrame(columns=['nameFull', 'ageatdraft'])

    with conn.cursor() as cursor:
        ms = time.time_ns() / 1e6
        print('Time stamp before Query execution:', ms, 'milliseconds')
        cursor.execute(select_Query)
        ms1 = time.time_ns() / 1e6
        print('Time stamp after Query execution:', ms1, 'milliseconds')
        ms_diff = ms1 - ms
        records = cursor.fetchall()
        for row in records:
            output_df = {'nameFull': row[0], 'ageatdraft': row[1]}
            college_df = pd.concat([college_df, pd.DataFrame.from_records([output_df])])

        print(college_df)
        print('Query execution time:', ms_diff, 'milliseconds')


def runQuery5b(conn):
    print('\n        Q5b) second query for question 5a        \n')
    select_Query = "select nameFull, MIN(ageatdraft) AS ageatdraft from gameParticipation GROUP BY nameFull ORDER BY MIN(ageatdraft) ASC LIMIT 1"
    college_df = pd.DataFrame(columns=['nameFull', 'ageatdraft'])

    with conn.cursor() as cursor:
        ms = time.time_ns() / 1e6
        print('Time stamp before Query execution:', ms, 'milliseconds')
        cursor.execute(select_Query)
        ms1 = time.time_ns() / 1e6
        print('Time stamp after Query execution:', ms1, 'milliseconds')
        ms_diff = ms1 - ms
        records = cursor.fetchall()
        for row in records:
            output_df = {'nameFull': row[0], 'ageatdraft': row[1]}
            college_df = pd.concat([college_df, pd.DataFrame.from_records([output_df])])

        print(college_df)
        print('Query execution time:', ms_diff, 'milliseconds')


def runQuery6a(conn):
    print(
        '\n        Q6 a) Finding out the detailed play type, field position and distance to goal when kicklength of the play is greater than 80      \n')
    select_Query = "SELECT p.playtypedetailed,p.fieldposition,p.distancetogoalpre from plays p where p.playid in (SELECT k.playid FROM kicks k where k.kicklength > '80')"
    kickslengthPlaytypes_df = pd.DataFrame(columns=['playtypedetailed', 'fieldposition', 'distancetogoalpre'])

    with conn.cursor() as cursor:
        ms = time.time_ns() / 1e6
        print('Time stamp before Query execution:', ms, 'milliseconds')
        cursor.execute(select_Query)
        ms1 = time.time_ns() / 1e6
        print('Time stamp after Query execution:', ms1, 'milliseconds')
        ms_diff = ms1 - ms
        records = cursor.fetchall()
        for row in records:
            output_df = {'playtypedetailed': row[0], 'fieldposition': row[1], 'distancetogoalpre': row[2]}
            kickslengthPlaytypes_df = pd.concat([kickslengthPlaytypes_df, pd.DataFrame.from_records([output_df])])

        print(kickslengthPlaytypes_df)
        print('Query execution time:', ms_diff, 'milliseconds')


def runQuery6b(conn):
    print('\n        Q6b) second query for question 6a        \n')
    select_Query = "SELECT p.playtypedetailed,p.fieldposition,p.distancetogoalpre from plays p INNER JOIN kicks k ON p.playid = k.playid where k.kicklength>'80'"
    kickslengthPlaytypes_df = pd.DataFrame(columns=['playtypedetailed', 'fieldposition', 'distancetogoalpre'])

    with conn.cursor() as cursor:
        ms = time.time_ns() / 1e6
        print('Time stamp before Query execution:', ms, 'milliseconds')
        cursor.execute(select_Query)
        ms1 = time.time_ns() / 1e6
        print('Time stamp after Query execution:', ms1, 'milliseconds')
        ms_diff = ms1 - ms
        records = cursor.fetchall()
        for row in records:
            output_df = {'playtypedetailed': row[0], 'fieldposition': row[1], 'distancetogoalpre': row[2]}
            kickslengthPlaytypes_df = pd.concat([kickslengthPlaytypes_df, pd.DataFrame.from_records([output_df])])

        print(kickslengthPlaytypes_df)
        print('Query execution time:', ms_diff, 'milliseconds')


def runQuery7(conn):
    print(
        '\n    Q7)    Find the position,type ,length ,returnyards and net yards of kicks with net yards is more than 70   \n')
    select_Query = "select kickposition,kicktype,kicklength,kickreturnyds,kicknetyds from kicks where kickreturnyds>70"
    kicks_df = pd.DataFrame(columns=['kickposition', 'kicktype', 'kicklength', 'kickreturnyds', 'kicknetyds'])
    with conn.cursor() as cursor:
        cursor.execute(select_Query)
        records = cursor.fetchall()
        for row in records:
            output_df = {'kickposition': row[0], 'kicktype': row[1], 'kicklength': row[2], 'kickreturnyds': row[3],
                         'kicknetyds': row[4]}
            kicks_df = pd.concat([kicks_df, pd.DataFrame.from_records([output_df])])
        print(kicks_df)
        outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(select_Query)
        with open('resultsfile_query7.csv', 'w') as f:
            cursor.copy_expert(outputquery, f)


def runQuery8(conn):
    print(
        '\n    Q8)   Find the players fullname, snap count, age, weight, height and home state where their game part unit is defense  \n')
    select_Query = "select nameFull,gamepartsnapcount,ageatdraft,weight,heightinches,homestate from gameparticipation where gamepartunit='defense'"
    defensePlayer_df = pd.DataFrame(
        columns=['nameFull', 'gamepartsnapcount', 'ageatdraft', 'weight', 'heightinches', 'homestate'])

    with conn.cursor() as cursor:
        cursor.execute(select_Query)
        records = cursor.fetchall()
        for row in records:
            output_df = {'nameFull': row[0], 'gamepartsnapcount': row[1], 'ageatdraft': row[2], 'weight': row[3],
                         'heightinches': row[4], 'homestate': row[5]}
            defensePlayer_df = pd.concat([defensePlayer_df, pd.DataFrame.from_records([output_df])])

        print(defensePlayer_df)
        outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(select_Query)
        with open('resultsfile_query8.csv', 'w') as f:
            cursor.copy_expert(outputquery, f)


def runQuery9(conn):
    print(
        '\n    Q9)    Find the fumble type, tackle type, interception yards for the plays where interception yards is more than 1 \n')
    select_Query = "SELECT f.fumtype,t.tackletype,i.intyards,count(*)FROM fumbles f, tackles t, plays p,interceptions i WHERE p.playid = f.playid AND p.playid = t.playid AND p.playid = i.playid GROUP BY f.fumtype, t.tackletype,i.intyards HAVING COUNT(*)> 1"
    typesintyards_df = pd.DataFrame(columns=['fumtype', 'tackletype', 'intyards', 'count'])
    with conn.cursor() as cursor:
        cursor.execute(select_Query)
        records = cursor.fetchall()
        for row in records:
            output_df = {'fumtype': row[0], 'tackletype': row[1], 'intyards': row[2], 'count': row[3]}
            typesintyards_df = pd.concat([typesintyards_df, pd.DataFrame.from_records([output_df])])
        print(typesintyards_df)
        outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(select_Query)
        with open('resultsfile_query9.csv', 'w') as f:
            cursor.copy_expert(outputquery, f)


def runQuery10(conn):
    print(
        '\n    Q10)   Find the name, age, height, weight and college of the players from Oregon who played in the 2019 season  \n')
    select_Query = "SELECT gp.nameFull,gp.ageatdraft,gp.heightinches,gp.weight,gp.college FROM gameParticipation gp JOIN games g ON g.gameid = gp.gameid  where homeState = 'OR' and g.season = 2019"
    oregonPlayers_df = pd.DataFrame(columns=['nameFull', 'ageatdraft', 'heightinches', 'weight', 'college'])
    with conn.cursor() as cursor:
        cursor.execute(select_Query)
        records = cursor.fetchall()
        for row in records:
            output_df = {'nameFull': row[0], 'ageatdraft': row[1], 'heightinches': row[2], 'weight': row[3],
                         'college': row[4]}
            oregonPlayers_df = pd.concat([oregonPlayers_df, pd.DataFrame.from_records([output_df])])
        print(oregonPlayers_df)


def main():
    conn = initialize()
    runQuery1(conn)
    runQuery2(conn)
    runQuery3(conn)
    runQuery4a(conn)
    runQuery4b(conn)
    runQuery5a(conn)
    runQuery5b(conn)
    runQuery6a(conn)
    runQuery6b(conn)
    runQuery7(conn)
    runQuery8(conn)
    runQuery9(conn)
    runQuery10(conn)


if __name__ == "__main__":
    main()
