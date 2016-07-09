import dota2api
import json
import time
import math

from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["hollowjack"]
api = dota2api.Initialise()

def get_league_matches(league_id, loaded_match_ids=[]):
    league_matches_c = db["league_matches"]
    league4664 = api.get_match_history(league_id=league_id)

    for match in league4664["matches"]:
        if match["match_id"] in loaded_match_ids:
            continue

        league_matches_c.insert_one({
            "league_id": league_id,
            "match_id": match["match_id"]
        })
        get_match(match["match_id"])

def get_match(match_id):
    sleep_count = 0
    matches_c = db["matches"]
    while(True):
        try:
            match = api.get_match_details(match_id=match_id)
            match["_id"] = match_id
            matches_c.insert_one(match)
            sleep_count = 0
            break
        except:
            time.sleep(int(math.pow(2, sleep_count)))
            continue


def get_league_match_ids(league_id):
    league_matches_c = db["league_matches"]
    cursor = league_matches_c.find(
        {"league_id": league_id},
        {"league_id": 0, "_id": 0}
    )
    return list(cursor)

def load_league(league_id):
    loaded_match_ids = get_league_match_ids(league_id)
    get_league_matches(league_id, loaded_match_ids)


if __name__ == "__main__" :
    load_league(4664)
