import json
import psycopg2
import os

def main():
    # Connect to the database
    try:
        #? This version of the json parser was done using psycopg2 and not psycopg3.
        #? If you need to run it and it's not working with psycopg3, try using psycopg2 instead.
        connection = psycopg2.connect(
            user = "postgres",
            password = "d@t@SQLbases",
            host = "127.0.0.1",
            port = "5432",
            database = "project_database"
        )
        cursor = connection.cursor()


        #* Iterate over all competition objects
        # Load JSON data from competitions.json file
        with open("./JSONdata/competitions.json", 'r') as json_data:
            data = json.load(json_data)
            # Iterate over every competition
            for competition in data:
                comp_name = competition.get('competition_name')
                season_name = competition.get('season_name')
                #? Select only competitions from the La Liga 2018/2019, 2019/2020, 2020/2021
                #? season or from the 2003/2004 Premier League season
                if((season_name in ["2018/2019", "2019/2020", "2020/2021"] and comp_name == "La Liga") or (season_name == "2003/2004" and comp_name == "Premier League")):
                    # Load in all data for 1 entry in the table
                    comp_id = competition.get('competition_id')
                    comp_name = competition.get('competition_name')
                    comp_gender = competition.get('competition_gender')
                    comp_youth = competition.get('competition_youth')
                    comp_international = competition.get('competition_international')
                    season_id = competition.get('season_id')
                    country_name = competition.get('country_name')
                    
                    query = "INSERT INTO Competitions(comp_id, comp_name, comp_gender, comp_youth, comp_international, season_id, season_name, country_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);"
                    data = (comp_id, comp_name, comp_gender, comp_youth, comp_international, season_id, season_name, country_name)
                    cursor.execute(query, data)

            connection.commit()
        print("Finished parsing competitions")

        #* Iterate over all matches
        home_ids = []
        away_ids = []
        manager_ids = []
        match_ids = []
        homeManagerPairs = [] #? Keep track of manager,hometeam pairs for ManagesHomeTeam table
        awayManagerPairs = [] #? Keep track of manager,awayteam pairs for ManagesAwayTeam table
        for path in ["./JSONdata/matches/11/90.json", "./JSONdata/matches/11/42.json", "./JSONdata/matches/11/4.json", "./JSONdata/matches/2/44.json"]:
            with open(path, 'r', encoding="utf8") as json_data:
                data = json.load(json_data)
                for match in data:
                    #? Used to create the Hometeam, Awayteam, and Managers tables
                    hometeam = match.get('home_team')
                    homeManagers = hometeam.get('managers')
                    awayteam = match.get('away_team')
                    awayManagers = awayteam.get('managers')
                    match_season_id = match.get('season').get('season_id')

                    #? Insert data into matches table
                    match_id = match.get('match_id')
                    match_date = match.get('match_date')
                    kickoff = match.get('kick_off')
                    home_score = match.get('home_score')
                    away_score = match.get('away_score')
                    match_week = match.get('match_week')

                    if(match.get('competition_stage') is not None):
                        comp_stage_id = match.get('competition_stage').get('id')
                        comp_stage_name = match.get('competition_stage').get('name')
                    else:
                        comp_stage_id = None
                        comp_stage_name = None

                    if(match.get('stadium') is not None):
                        stadium_id = match.get('stadium').get('id')
                        stadium_name = match.get('stadium').get('name')
                        stadium_country_id = match.get('stadium').get('country').get('id')
                        stadium_country_name = match.get('stadium').get('country').get('name')
                    else:
                        stadium_id = None
                        stadium_name = None
                        stadium_country_id = None
                        stadium_country_name = None

                    if(match.get('referee') is not None):
                        referee_id = match.get('referee').get('id')
                        referee_name = match.get('referee').get('name')
                    else:
                        referee_id = None
                        referee_name = None

                    query1 = "INSERT INTO Matches(match_id, match_date, kickoff, home_score, away_score, match_week, comp_stage_id, comp_stage_name, stadium_id, stadium_name, stadium_country_id, stadium_country_name, referee_id, referee_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    data1 = (match_id, match_date, kickoff, home_score, away_score, match_week, comp_stage_id, comp_stage_name, stadium_id, stadium_name, stadium_country_id, stadium_country_name, referee_id, referee_name)
                    cursor.execute(query1, data1)
                    match_ids.append(match_id) #? Store match ids we've seen so we know which lineups and events to parse later

                    #? Insert data into Hometeam
                    hometeam_id = hometeam.get('home_team_id')
                    hometeam_name = hometeam.get('home_team_name')
                    hometeam_gender = hometeam.get('home_team_gender')
                    hometeam_group = hometeam.get('home_team_group')
                    homecountry_id = hometeam.get('country').get('id')
                    homecountry_name = hometeam.get('country').get('name')

                    if(hometeam_id not in home_ids):
                        home_ids.append(hometeam_id)
                        query2 = "INSERT INTO Hometeam(hometeam_id, hometeam_name, gender, hometeam_group, country_id, country_name) VALUES(%s, %s, %s, %s, %s, %s)"
                        data2 = (hometeam_id, hometeam_name, hometeam_gender, hometeam_group, homecountry_id, homecountry_name)
                        cursor.execute(query2, data2)

                    #? Insert data into Awayteam
                    awayteam_id = awayteam.get('away_team_id')
                    awayteam_name = awayteam.get('away_team_name')
                    awayteam_gender = awayteam.get('away_team_gender')
                    awayteam_group = awayteam.get('away_team_group')
                    awaycountry_id = awayteam.get('country').get('id')
                    awaycountry_name = awayteam.get('country').get('name')

                    if(awayteam_id not in away_ids):
                        away_ids.append(awayteam_id)
                        query3 = "INSERT INTO Awayteam(awayteam_id, awayteam_name, gender, awayteam_group, country_id, country_name) VALUES(%s, %s, %s, %s, %s, %s)"
                        data3 = (awayteam_id, awayteam_name, awayteam_gender, awayteam_group, awaycountry_id, awaycountry_name)
                        cursor.execute(query3, data3)

                    #? Insert data into Managers and ManagesHomeTeam
                    for managers in [homeManagers]:
                        if managers is None:
                            continue
                        for manager in managers:
                            manager_id = manager.get('id')
                            manager_name = manager.get('name')
                            nickname = manager.get('nickname')
                            dob = manager.get('dob')
                            country_id = manager.get('country').get('id')
                            country_name = manager.get('country').get('name')

                            # Query for Managers table
                            if(manager_id not in manager_ids):
                                manager_ids.append(manager_id)
                                query4 = "INSERT INTO Managers(manager_id, manager_name, nickname, dob, country_id, country_name) VALUES(%s, %s, %s, %s, %s, %s)"
                                data4 = (manager_id, manager_name, nickname, dob, country_id, country_name)
                                cursor.execute(query4, data4)

                            # Query for ManagesHomeTeam table
                            if((manager_id, hometeam_id) not in homeManagerPairs):
                                homeManagerPairs.append((manager_id, hometeam_id))
                                query5 = "INSERT INTO ManagesHomeTeam(manager_id, hometeam_id) VALUES(%s, %s)"
                                data5 = (manager_id, hometeam_id)
                                cursor.execute(query5, data5)
                    
                    #? Insert data into Managers and ManagesAwayTeam
                    for managers in [awayManagers]:
                        if managers is None:
                            continue
                        for manager in managers:
                            manager_id = manager.get('id')
                            manager_name = manager.get('name')
                            nickname = manager.get('nickname')
                            dob = manager.get('dob')
                            country_id = manager.get('country').get('id')
                            country_name = manager.get('country').get('name')

                            # Query for Managers table
                            if(manager_id not in manager_ids):
                                manager_ids.append(manager_id)
                                query4 = "INSERT INTO Managers(manager_id, manager_name, nickname, dob, country_id, country_name) VALUES(%s, %s, %s, %s, %s, %s)"
                                data4 = (manager_id, manager_name, nickname, dob, country_id, country_name)
                                cursor.execute(query4, data4)

                            # Query for ManagesAwayTeam table
                            if((manager_id, awayteam_id) not in awayManagerPairs):
                                awayManagerPairs.append((manager_id, awayteam_id))
                                query5 = "INSERT INTO ManagesAwayTeam(manager_id, awayteam_id) VALUES(%s, %s)"
                                data5 = (manager_id, awayteam_id)
                                cursor.execute(query5, data5)

                    #? Insert into Associated
                    query6 = "INSERT INTO Associated(season_id, match_id) VALUES(%s, %s)"
                    data6 = (match_season_id, match_id)
                    cursor.execute(query6, data6)

                    #? Insert into PlayedIn
                    query7 = "INSERT INTO PlayedIn(match_id, hometeam_id, awayteam_id) VALUES(%s, %s, %s)"
                    data7 = (match_id, hometeam_id, awayteam_id)
                    cursor.execute(query7, data7)

                connection.commit()
        print("Finished parsing matches")

        #* Iterate over all Lineup files in lineups directory
        player_ids = []
        home_players = []
        away_players = []
        for fileName in os.listdir("./JSONdata/lineups"):
            lineup_in_match_id = fileName[:-5]
            #? Check to see if the match the lineup is in has been added to the database
            if(int(lineup_in_match_id) in match_ids):
                lineupFilePath = "./JSONdata/lineups/" + lineup_in_match_id + ".json"
                with open(lineupFilePath, 'r', encoding="utf8") as json_data:
                    data = json.load(json_data)

                    #? Iterate over all objects in the lineup file
                    for team in data:
                        team_id = team.get('team_id')
                        team_name = team.get('team_name')
                        lineup = team.get('lineup')
                        
                        #? Iterate over all players in the current object's lineup array
                        for player in lineup:
                            player_id = player.get('player_id')
                            player_name = player.get('player_name')
                            player_nickname = player.get('player_nickname')
                            jersey_number = player.get('jersey_number')
                            country_id = player.get('country').get('id')
                            country_name = player.get('country').get('name')
                            cards = player.get('cards')
                            positions = player.get('positions')

                            #? Insert current player into Lineups table
                            if(player_id not in player_ids):
                                player_ids.append(player_id)
                                query8 = "INSERT INTO Lineups(player_id, player_name, player_nickname, team_id, team_name, jersey_number, country_id, country_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
                                data8 = (player_id, player_name, player_nickname, team_id, team_name, jersey_number, country_id, country_name)
                                cursor.execute(query8, data8)

                            #? Iterate over all current player's cards
                            for card in cards:
                                card_time = card.get('time')
                                card_type = card.get('card_type')
                                reason = card.get('reason')
                                card_period = card.get('period')
                                
                                #? Insert current card into Cards table
                                query9 = "INSERT INTO Cards(card_time, card_type, reason, card_period) VALUES(%s, %s, %s, %s)"
                                data9 = (card_time, card_type, reason, card_period)
                                cursor.execute(query9, data9)

                                #? Insert player's associated card into GetCards table
                                query10 = "INSERT INTO GetCards(player_id) VALUES(%s)"
                                data10 = (player_id,)
                                cursor.execute(query10, data10)
                                connection.commit()

                            #? Iterate over all current player's positions
                            for position in positions:
                                position_id = position.get('position_id')
                                pos_name = position.get('position')
                                pos_from = position.get('from')
                                pos_to = position.get('to')
                                from_period = position.get('from_period')
                                to_period = position.get('to_period')
                                start_reason = position.get('start_reason')
                                end_reason = position.get('end_reason')

                                #? Insert current position into Positions table
                                query11 = "INSERT INTO Positions(position_id, pos_name, pos_from, pos_to, from_period, to_period, start_reason, end_reason) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
                                data11 = (position_id, pos_name, pos_from, pos_to, from_period, to_period, start_reason, end_reason)
                                cursor.execute(query11, data11)
                                connection.commit()

                                #? Insert player's associated position into Change table
                                query12 = "INSERT INTO Change(player_id) VALUES(%s)"
                                data12 = (player_id,)
                                cursor.execute(query12, data12)
                            
                            #? Insert into ContainsHomePlayer
                            if(team_id in home_ids and (player_id, team_id) not in home_players):
                                home_players.append((player_id, team_id))
                                query13 = "INSERT INTO ContainsHomePlayer(player_id, hometeam_id) VALUES(%s, %s)"
                                data13 = (player_id, team_id)
                                cursor.execute(query13, data13)

                            #? Insert into ContainsAwayPlayer
                            if(team_id in away_ids and (player_id, team_id) not in away_players):
                                away_players.append((player_id, team_id))
                                query14 = "INSERT INTO ContainsAwayPlayer(player_id, awayteam_id) VALUES(%s, %s)"
                                data14 = (player_id, team_id)
                                cursor.execute(query14, data14)
        connection.commit()
        print("Finished parsing lineup")

        #* Iterate over all Event files in events directory
        for fileName in os.listdir("./JSONdata/events"):
            event_in_match_id = fileName[:-5]
            #? Check to see if event has its corresponding match added to database
            if(int(event_in_match_id) in match_ids):
                print(event_in_match_id)  #? Uncomment to see progress of data parsing
                eventFilePath = "./JSONdata/events/" + event_in_match_id + ".json"
                with open(eventFilePath, 'r', encoding="utf8") as json_data:
                    data = json.load(json_data)

                    #? Iterate over all event objects
                    for event in data:
                        event_id = event.get('id')
                        event_index = event.get('index')
                        event_period = event.get('period')
                        event_timestamp = event.get('timestamp')
                        event_minute = event.get('minute')
                        event_second = event.get('second')

                        type_id = None
                        type_name = None
                        eventType = event.get('type')
                        if(event is not None):
                            type_id = eventType.get('id')
                            type_name = eventType.get('name')

                        possession = event.get('possession')
                        poss_team_id = None
                        poss_team_name = None
                        poss_team = event.get('possession_team')
                        if(poss_team is not None):
                            poss_team_id = poss_team.get('id')
                            poss_team_name = poss_team.get('name')
                        
                        play_pattern_id = None
                        play_pattern_name = None
                        play_pattern = event.get('play_pattern')
                        if(play_pattern is not None):
                            play_pattern_id = play_pattern.get('id')
                            play_pattern_name = play_pattern.get('name')
                        
                        team_id = None
                        team_name = None
                        team = event.get('team')
                        if(team is not None):
                            team_id = team.get('id')
                            team_name = team.get('name')

                        duration = event.get('duration')
                        ev_location = event.get('location')

                        #? Get event's associated player if they exist
                        player = event.get('player')
                        player_id = None
                        player_name = None
                        if(player is not None):
                            player_id = player.get('id')
                            player_name = player.get('name')

                        #? Get player's associated position if they exist
                        position = event.get('position')
                        position_id = None
                        position_name = None
                        if(position is not None):
                            position_id = position.get('id')
                            position_name = position.get('name')
                        
                        underpressure = event.get('under_pressure')

                        #? Insert into AllEvents table
                        query15 = "INSERT INTO AllEvents(event_id, event_index, event_period, event_timestamp, event_minute, event_second, type_id, type_name, possession, poss_team_id, poss_team_name, play_pattern_id, play_pattern_name, team_id, team_name, duration, ev_location, player_id, player_name, position_id, position_name, underpressure) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        data15 = (event_id, event_index, event_period, event_timestamp, event_minute, event_second, type_id, type_name, possession, poss_team_id, poss_team_name, play_pattern_id, play_pattern_name, team_id, team_name, duration, ev_location, player_id, player_name, position_id, position_name, underpressure)
                        cursor.execute(query15, data15)

                        #? Insert into OccurredIn table
                        query43 = "INSERT INTO OccurredIn(event_id, match_id) VALUES(%s, %s)"
                        data43 = (event_id, int(event_in_match_id))
                        cursor.execute(query43, data43)

                        #? Check to see which event we have and insert into appropriate table
                        #* Insert into RelatedEvents table
                        related_events = event.get('related_events')
                        if(related_events is not None):
                            for related_event in related_events:
                                query16 = "INSERT INTO RelatedEvents(related_id, event_id) VALUES(%s, %s)"
                                data16 = (related_event, event_id)
                                cursor.execute(query16, data16)
                        
                        #* Insert into Duel table
                        if(type_name == "Duel"):
                            duel = event.get('duel')
                            counterpress = event.get('counterpress')
                            if(counterpress is None):
                                counterpress = False
                            type_id = None
                            type_name = None
                            outcome_id = None
                            outcome_name = None
                            if(duel is not None):
                                duelType = duel.get('type')
                                if(duelType is not None):
                                    type_id = duelType.get('id')
                                    type_name = duelType.get('name')
                                
                                outcome = duel.get('outcome')
                                if(outcome is not None):
                                    outcome_id = outcome.get('id')
                                    outcome_name = outcome.get('name')

                            query17 = "INSERT INTO Duel(event_id, counterpress, type_id, type_name, outcome_id, outcome_name) VALUES(%s, %s, %s, %s, %s, %s)"
                            data17 = (event_id, counterpress, type_id, type_name, outcome_id, outcome_name)
                            cursor.execute(query17, data17)

                        #* Insert into Block table
                        elif(type_name == "Block"):
                            block = event.get('block')
                            deflection = False
                            offensive = False
                            save_block = False
                            counterpress = False
                            if(block is not None):
                                deflection = block.get('deflection')
                                offensive = block.get('offensive')
                                save_block = block.get('save_block')
                                counterpress = block.get('counterpress')

                            query18 = "INSERT INTO Block(event_id, deflection, offensive, save_block, counterpress) VALUES(%s, %s, %s, %s, %s)"
                            data18 = (event_id, deflection, offensive, save_block, counterpress)
                            cursor.execute(query18, data18)

                        #* Insert into Clearance table
                        elif(type_name == "Clearance"):
                            clearance = event.get('clearance')
                            body_part_id = None
                            body_part_name = None
                            aerial_won = False
                            if(clearance is not None):
                                aerial_won = clearance.get('aerial_won')
                                if(aerial_won is None):
                                    aerial_won = False
                                
                                body_part_id = None
                                body_part_name = None
                                body_part = clearance.get('body_part')
                                if(body_part is not None):
                                    body_part_id = body_part.get('id')
                                    body_part_name = body_part.get('name')
                            
                            query19 = "INSERT INTO Clearance(event_id, body_part_id, body_part_name, aerial_won) VALUES(%s, %s, %s, %s)"
                            data19 = (event_id, body_part_id, body_part_name, aerial_won)
                            cursor.execute(query19, data19)
                        
                        #* Insert into Interception table
                        elif(type_name == "Interception"):
                            interception = event.get('interception')
                            outcome_id = None
                            outcome_name = None
                            if(interception is not None):
                                outcome = interception.get('outcome')
                                if(outcome is not None):
                                    outcome_id = outcome.get('id')
                                    outcome_name = outcome.get('name')
                            
                            query20 = "INSERT INTO Interception(event_id, outcome_id, outcome_name) VALUES(%s, %s, %s)"
                            data20 = (event_id, outcome_id, outcome_name)
                            cursor.execute(query20, data20)

                        #* Insert into Dribble table
                        elif(type_name == "Dribble"):
                            dribble = event.get('dribble')
                            overrun = False
                            nutmeg = False
                            outcome_id = None
                            outcome_name = None
                            notouch = False
                            if(dribble is not None):
                                overrun = dribble.get('overrun')
                                nutmeg = dribble.get('nutmeg')
                                outcome = dribble.get('outcome')
                                if(outcome is not None):
                                    outcome_id = outcome.get('id')
                                    outcome_name = outcome.get('name')
                                notouch = dribble.get('no_touch')
                            
                            query21 = "INSERT INTO Dribble(event_id, overrun, nutmeg, outcome_id, outcome_name, no_touch) VALUES(%s, %s, %s, %s, %s, %s)"
                            data21 = (event_id, overrun, nutmeg, outcome_id, outcome_name, notouch)
                            cursor.execute(query21, data21)

                        #* Insert into Shot and Freeze Frame table
                        elif(type_name == "Shot"):
                            shot = event.get('shot')
                            if(shot is not None):
                                freeze_frame = event.get('freeze_frame')
                                statsbomb_xg = shot.get('statsbomb_xg')
                                end_location = shot.get('end_location')

                                body_part_id = None
                                body_part_name = None
                                body_part = shot.get('body_part')
                                if(body_part is not None):
                                    body_part_id = body_part.get('id')
                                    body_part_name = body_part.get('name')
                                
                                type_id = None
                                type_name = None
                                shot_type = shot.get('type')
                                if(shot_type is not None):
                                    type_id = shot_type.get('id')
                                    type_name = shot_type.get('name')

                                outcome_id = None
                                outcome_name = None
                                outcome = shot.get('outcome')
                                if(outcome is not None):
                                    outcome_id = outcome.get('id')
                                    outcome_name = outcome.get('name')
                                
                                technique_id = None
                                technique_name = None
                                technique = shot.get('technique')
                                if(technique is not None):
                                    technique_id = technique.get('id')
                                    technique_name = technique.get('name')
                                
                                key_pass_id = shot.get('key_pass_id')
                                aerial_won = shot.get('aerial_won')
                                follows_dribble = shot.get('follows_dribble')
                                first_time = shot.get('first_time')
                                open_goal = shot.get('open_goal')
                                deflected = shot.get('deflected')

                                #? Make sure we insert False instead of none for these boolean values
                                if(aerial_won is None):
                                    aerial_won = False
                                
                                if(follows_dribble is None):
                                    follows_dribble = False
                                
                                if(first_time is None):
                                    first_time = False

                                if(open_goal is None):
                                    open_goal = False

                                if(deflected is None):
                                    deflected = False
                                
                                #? Insert into Shot table
                                query22 = "INSERT INTO Shot(event_id, statsbomb_xg, end_location, body_part_id, body_part_name, type_id, type_name, outcome_id, outcome_name, technique_id, technique_name, key_pass_id, aerial_won, follows_dribble, first_time, open_goal, deflected) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                data22 = (event_id, statsbomb_xg, end_location, body_part_id, body_part_name, type_id, type_name, outcome_id, outcome_name, technique_id, technique_name, key_pass_id, aerial_won, follows_dribble, first_time, open_goal, deflected)
                                cursor.execute(query22, data22)

                                #? Insert into FreezeFrame table
                                if(freeze_frame is not None):
                                    for single_freeze_frame in freeze_frame:
                                        frame_location = single_freeze_frame.get('location')

                                        player_id = None
                                        player_name = None
                                        freezePlayer = single_freeze_frame.get('player')
                                        if(freezePlayer is not None):
                                            player_id = freezePlayer.get('id')
                                            player_name = freezePlayer.get('name')
                                        
                                        position_id = None
                                        position_name = None
                                        playerPosition = single_freeze_frame.get('position')
                                        if(playerPosition is not None):
                                            position_id = playerPosition.get('id')
                                            position_name = playerPosition.get('name')

                                        teammate = single_freeze_frame.get('teammate')

                                        query23 = "INSERT INTO FreezeFrame(event_id, frame_location, player_id, player_name, position_id, position_name, teammate) VALUES(%s, %s, %s, %s, %s, %s, %s)"
                                        data23 = (event_id, frame_location, player_id, player_name, position_id, position_name, teammate)
                                        cursor.execute(query23, data23)

                        #* Insert into Substitution table
                        elif(type_name == "Substitution"):
                            substitution = event.get('substitution')
                            outcome_id = None
                            outcome_name = None
                            replacement_id = None
                            replacement_name = None

                            if(substitution is not None):
                                outcome = substitution.get('outcome')
                                if(outcome_id is not None):
                                    outcome_id = outcome.get('id')
                                    outcome_name = outcome.get('name')
                                
                                replacement = substitution.get('replacement')
                                if(replacement is not None):
                                    replacement_id = replacement.get('id')
                                    replacement_name = replacement.get('name')

                            query24 = "INSERT INTO Substitution(event_id, outcome_id, outcome_name, replacement_id, replacement_name) VALUES(%s, %s, %s, %s, %s)"
                            data24 = (event_id, outcome_id, outcome_name, replacement_id, replacement_name)
                            cursor.execute(query24, data24)

                        #* Insert into FoulWon table
                        elif(type_name == "Foul Won"):
                            foul_won = event.get('foul_won')
                            defensive = False
                            advantage = False
                            penalty = False
                            if(foul_won is not None):
                                defensive = foul_won.get('defensive')
                                advantage = foul_won.get('advantage')
                                penalty = foul_won.get('penalty')
                            
                            if(defensive is None):
                                defensive = False
                            
                            if(advantage is None):
                                advantage = False
                            
                            if(penalty is None):
                                penalty = False
                            
                            query25 = "INSERT INTO FoulWon(event_id, defensive, advantage, penalty) VALUES(%s, %s, %s, %s)"
                            data25 = (event_id, defensive, advantage, penalty)
                            cursor.execute(query25, data25)
                        
                        #* Insert into GoalKeeper table
                        elif(type_name == "Goal Keeper"):
                            goalkeeper = event.get('goalkeeper')
                            if goalkeeper is not None:
                                end_location = goalkeeper.get('end_location')

                                type_id = None
                                type_name = None
                                goalkeep_type = goalkeeper.get('type')
                                if(goalkeep_type is not None):
                                    type_id = goalkeep_type.get('id')
                                    type_name = goalkeep_type.get('name')
                                
                                position_id = None
                                position_name = None
                                goalKeepPosition = goalkeeper.get('position')
                                if(goalKeepPosition is not None):
                                    position_id = goalKeepPosition.get('id')
                                    position_name = goalKeepPosition.get('name')
                                
                                technique_id = None
                                technique_name = None
                                technique = goalkeeper.get('technique')
                                if(technique is not None):
                                    technique_id = technique.get('id')
                                    technique_name = technique.get('name')
                                
                                body_part_id = None
                                body_part_name = None
                                body_part = goalkeeper.get('body_part')
                                if(body_part is not None):
                                    body_part_id = body_part.get('id')
                                    body_part_name = body_part.get('name')
                                
                                outcome_id = None
                                outcome_name = None
                                outcome = goalkeeper.get('outcome')
                                if(outcome is not None):
                                    outcome_id = outcome.get('id')
                                    outcome_name = outcome.get('name')

                                query26 = "INSERT INTO GoalKeeper(event_id, end_location, type_id, type_name, position_id, position_name, technique_id, technique_name, body_part_id, body_part_name, outcome_id, outcome_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                data26 = (event_id, end_location, type_id, type_name, position_id, position_name, technique_id, technique_name, body_part_id, body_part_name, outcome_id, outcome_name)
                                cursor.execute(query26, data26)

                        #* Insert into Pass table
                        elif(type_name == "Pass"):
                            passEvent = event.get('pass')
                            if(passEvent is not None):
                                recipient_id = None
                                recipient_name = None
                                recipient = passEvent.get('recipient')
                                if(recipient is not None):
                                    recipient_id = recipient.get('id')
                                    recipient_name = recipient.get('name')
                                
                                pass_length = passEvent.get('length')
                                angle = passEvent.get('angle')

                                height_id = None
                                height_name = None
                                height = passEvent.get('height')
                                if(height is not None):
                                    height_id = height.get('id')
                                    height_name = height.get('name')

                                end_location = passEvent.get('location')
                                assisted_shot_id = passEvent.get('assisted_shot_id')
                                backheel = passEvent.get('backheel')
                                if(backheel is None):
                                    backheel = False
                                deflected = passEvent.get('deflected')
                                if(deflected is None):
                                    deflected = False
                                miscommunication = passEvent.get('miscommunication')
                                if(miscommunication is None):
                                    miscommunication = False
                                pass_cross = passEvent.get('cross')
                                if(pass_cross is None):
                                    pass_cross = False
                                cut_back = passEvent.get('cut_back')
                                if(cut_back is None):
                                    cut_back = False
                                switch = passEvent.get('switch')
                                if(switch is None):
                                    switch = False
                                shot_assist = passEvent.get('shot_assist')
                                if(shot_assist is None):
                                    shot_assist = False
                                goal_assist = passEvent.get('goal_assist')
                                if(goal_assist is None):
                                    goal_assist = False
                                
                                body_part_id = None
                                body_part_name = None
                                body_part = passEvent.get('body_part')
                                if(body_part is not None):
                                    body_part_id = body_part.get('id')
                                    body_part_name = body_part.get('name')
                                
                                type_id = None
                                type_name = None
                                passType = passEvent.get('type')
                                if(passType is not None):
                                    type_id = passType.get('id')
                                    type_name = passType.get('name')
                                
                                outcome_id = None
                                outcome_name = None
                                outcome = passEvent.get('outcome')
                                if(outcome is not None):
                                    outcome_id = outcome.get('id')
                                    outcome_name = outcome.get('name')
                                
                                technique_id = None
                                technique_name = None
                                technique = passEvent.get('technique')
                                if(technique is not None):
                                    technique_id = technique.get('id')
                                    technique_name = technique.get('name')

                                query27 = "INSERT INTO Pass(event_id, recipient_id, recipient_name, pass_length, angle, height_id, height_name, end_location, assisted_shot_id, backheel, deflected, miscommunication, pass_cross, cut_back, switch, shot_assist, goal_assist, body_part_id, body_part_name, type_id, type_name, outcome_id, outcome_name, technique_id, technique_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                data27 = (event_id, recipient_id, recipient_name, pass_length, angle, height_id, height_name, end_location, assisted_shot_id, backheel, deflected, miscommunication, pass_cross, cut_back, switch, shot_assist, goal_assist, body_part_id, body_part_name, type_id, type_name, outcome_id, outcome_name, technique_id, technique_name)
                                cursor.execute(query27, data27)

                        #* Insert into FiftyFifty table
                        elif(type_name == "50/50"):
                            fiftyfifty = event.get('50_50')
                            counterpress = False
                            outcome_id = None
                            outcome_name = None

                            if(fiftyfifty is not None):
                                counterpress = fiftyfifty.get('counterpress')
                                if(counterpress is None):
                                    counterpress = False
                                outcome_id = fiftyfifty.get('outcome').get('id')
                                outcome_name = fiftyfifty.get('outcome').get('name')

                            query28 = "INSERT INTO FiftyFifty(event_id, counterpress, outcome_id, outcome_name) VALUES(%s, %s, %s, %s)"
                            data28 = (event_id, counterpress, outcome_id, outcome_name)
                            cursor.execute(query28, data28)

                        #* Insert into StartingXI table
                        elif(type_name == "Starting XI"):
                            tactics = event.get('tactics')

                            if(tactics is not None):
                                tactics_formation = tactics.get('formation')
                                lineup = tactics.get('lineup')
                                for player in lineup:
                                    player_id = player.get('player').get('id')
                                    player_name = player.get('player').get('name')
                                    position_id = player.get('position').get('id')
                                    position_name = player.get('position').get('name')
                                    jersey_number = player.get('jersey_number')

                                    query29 = "INSERT INTO StartingXI(event_id, tactics_formation, player_id, player_name, position_id, position_name, jersey_number) VALUES(%s, %s, %s, %s, %s, %s, %s)"
                                    data29 = (event_id, tactics_formation, player_id, player_name, position_id, position_name, jersey_number)
                                    cursor.execute(query29, data29)

                        #* Insert into TacticalShift table
                        elif(type_name == "Tactical Shift"):
                            tactics = event.get('tactics')

                            if(tactics is not None):
                                tactics_formation = tactics.get('formation')
                                lineup = tactics.get('lineup')
                                for player in lineup:
                                    player_id = player.get('player').get('id')
                                    player_name = player.get('player').get('name')
                                    position_id = player.get('position').get('id')
                                    position_name = player.get('position').get('name')
                                    jersey_number = player.get('jersey_number')

                                    query30 = "INSERT INTO TacticalShift(event_id, tactics_formation, player_id, player_name, position_id, position_name, jersey_number) VALUES(%s, %s, %s, %s, %s, %s, %s)"
                                    data30 = (event_id, tactics_formation, player_id, player_name, position_id, position_name, jersey_number)
                                    cursor.execute(query30, data30)

                        #* Insert into Miscontrol table
                        elif(type_name == "Miscontrol"):
                            miscontrol = event.get('miscontrol')
                            aerial_won = False
                            if(miscontrol is not None):
                                aerial_won = miscontrol.get('aerial_won')
                                if(aerial_won is None):
                                    aerial_won = False
                            
                            query31 = "INSERT INTO Miscontrol(event_id, aerial_won) VALUES(%s, %s)"
                            data31 = (event_id, aerial_won)
                            cursor.execute(query31, data31)

                        #* Insert into Carry table
                        elif(type_name == "Carry"):
                            carry = event.get('carry')
                            end_location = None
                            if(carry is not None):
                                end_location = carry.get('location')
                            
                            query32 = "INSERT INTO Carry(event_id, end_location) VALUES(%s, %s)"
                            data32 = (event_id, end_location)
                            cursor.execute(query32, data32)

                        #* Insert into BadBehaviour table
                        elif(type_name == "Bad Behaviour"):
                            bad_behaviour = event.get('bad_behaviour')
                            card_id = None
                            card_name = None
                            if(bad_behaviour is not None):
                                card_id = bad_behaviour.get('card').get('id')
                                card_name = bad_behaviour.get('card').get('name')
                            
                            query33 = "INSERT INTO BadBehaviour(event_id, card_id, card_name) VALUES(%s, %s, %s)"
                            data33 = (event_id, card_id, card_name)
                            cursor.execute(query33, data33)

                        #* Insert into FoulCommitted
                        elif(type_name == "Foul Committed"):
                            foul_committed = event.get('foul_committed')
                            counterpress = False
                            offensive = False
                            type_id = None
                            type_name = None
                            advantage = False
                            penalty = False
                            card_id = None
                            card_name = None
                            
                            if(foul_committed is not None):
                                counterpress = foul_committed.get('counterpress')
                                if(counterpress is None):
                                    counterpress = False
                                
                                offensive = foul_committed.get('offensive')
                                if(offensive is None):
                                    offensive = False
                                
                                foul_type = foul_committed.get('type')
                                if(foul_type is not None):
                                    type_id = foul_type.get('id')
                                    type_name = foul_type.get('name')
                                
                                advantage = foul_committed.get('advantage')
                                if(advantage is None):
                                    advantage = False
                                
                                penalty = foul_committed.get('penalty')
                                if(penalty is None):
                                    penalty = False

                                card = foul_committed.get('card')
                                if(card is not None):
                                    card_id = card.get('id')
                                    card_name = card.get('name')
                            
                            query34 = "INSERT INTO FoulCommitted(event_id, counterpress, offensive, type_id, type_name, advantage, penalty, card_id, card_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            data34 = (event_id, counterpress, offensive, type_id, type_name, advantage, penalty, card_id, card_name)
                            cursor.execute(query34, data34)
                        
                        #* Insert into BallReceipt table
                        elif(type_name == "Ball Receipt*"):
                            ball_receipt = event.get('ball_receipt')
                            outcome_id = None
                            outcome_name = None

                            if(ball_receipt is not None):
                                outcome_id = ball_receipt.get('outcome').get('id')
                                outcome_name = ball_receipt.get('outcome').get('name')
                            
                            query35 = "INSERT INTO BallReceipt(event_id, outcome_id, outcome_name) VALUES(%s, %s, %s)"
                            data35 = (event_id, outcome_id, outcome_name)
                            cursor.execute(query35, data35)

                        #* Insert into BallRecovery table
                        elif(type_name == "Ball Recovery"):
                            ball_recovery = event.get('ball_recovery')
                            recovery_failure = False
                            offensive = False

                            if(ball_recovery is not None):
                                recovery_failure = ball_recovery.get('recovery_failure')
                                if(recovery_failure is None):
                                    recovery_failure = False

                                offensive = ball_recovery.get('offensive')
                                if(offensive is None):
                                    offensive = False
                            
                            query36 = "INSERT INTO BallRecovery(event_id, recovery_failure, offensive) VALUES(%s, %s, %s)"
                            data36 = (event_id, recovery_failure, offensive)
                            cursor.execute(query36, data36)

                        #* Insert into DribbledPast table
                        elif(type_name == "Dribbled Past"):
                            counterpress = event.get('counterpress')
                            if(counterpress is None):
                                counterpress = False
                            
                            query37 = "INSERT INTO DribbledPast(event_id, counterpress) VALUES(%s, %s)"
                            data37 = (event_id, counterpress)
                            cursor.execute(query37, data37)

                        #* Insert into Pressure table
                        elif(type_name == "Pressure"):
                            counterpress = event.get('counterpress')
                            if(counterpress is None):
                                counterpress = False
                            
                            query38 = "INSERT INTO Pressure(event_id, counterpress) VALUES(%s, %s)"
                            data38 = (event_id, counterpress)
                            cursor.execute(query38, data38)

                        #* Insert into PlayerOff table
                        elif(type_name == "Player Off"):
                            player_off = event.get('player_off')

                            permanent = False
                            if(player_off is not None):
                                permanent = player_off.get('permanent')
                            
                            query39 = "INSERT INTO PlayerOff(event_id, permanent) VALUES(%s, %s)"
                            data39 = (event_id, permanent)
                            cursor.execute(query39, data39)

                        #* Insert into InjuryStoppage table
                        elif(type_name == "Injury Stoppage"):
                            injury_stoppage = event.get('injury_stoppage')
                            in_chain = False
                            if(injury_stoppage is not None):
                                in_chain = injury_stoppage.get('in_chain')
                                if(in_chain is None):
                                    in_chain = False
                            
                            query40 = "INSERT INTO InjuryStoppage(event_id, in_chain) VALUES(%s, %s)"
                            data40 = (event_id, in_chain)
                            cursor.execute(query40, data40)

                        #* Insert into HalfStart table
                        elif(type_name == "Half Start"):
                            half_start = event.get('half_start')
                            late_video_start = False

                            if(half_start is not None):
                                late_video_start = half_start.get('late_video_start')
                                if(late_video_start is None):
                                    late_video_start = False
                            
                            query41 = "INSERT INTO HalfStart(event_id, late_video_start) VALUES(%s, %s)"
                            data41 = (event_id, late_video_start)
                            cursor.execute(query41, data41)

                        #* Insert into HalfEnd table
                        elif(type_name == "Half End"):
                            half_end = event.get('half_end')
                            early_video_end = False
                            match_suspended = False
                            if(half_end is not None):
                                early_video_end = half_end.get('early_video_end')
                                match_suspended = half_end.get('match_suspended')

                                if(early_video_end is None):
                                    early_video_end = False
                                
                                if(match_suspended is None):
                                    match_suspended = False
                            
                            query42 = "INSERT INTO HalfEnd(event_id, early_video_end, match_suspended) VALUES(%s, %s, %s)"
                            data42 = (event_id, early_video_end, match_suspended)
                            cursor.execute(query42, data42)                      
        connection.commit()
        print("Finished parsing events")

    except(Exception, psycopg2.Error) as error:
        print("Failed to insert into database.", error)

    finally:
        if(connection):
            cursor.close()
            connection.close()
            print("Database connection is closed.")

if __name__=="__main__":
    main()