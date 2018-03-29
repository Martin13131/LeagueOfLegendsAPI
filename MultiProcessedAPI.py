# -*- coding: utf-8 -*-
"""
Created on Tue Mar 20 11:16:19 2018

@author: mljmol
"""
import time
import requests
import json
import multiprocessing as mp

class LeagueOfLegends:
    URL = "https://euw1.api.riotgames.com"

    def __init__(self, API_key):
        self.key = API_key
        
#    @RateLimited(100./120)
    def Retrieve(self, specifyLink, inValue):
        Response = requests.get(self.URL + specifyLink + inValue + "?api_key=" + self.key)
        return Response.json()
    #/lol/summoner/v3/summoners/by-account/
    def GetMatches(self, AccID, Recent=True):
        if Recent:
            Response = self.Retrieve("/lol/match/v3/matchlists/by-account/", str(AccID)+"/recent")
        else:
            Response = self.Retrieve("/lol/match/v3/matchlists/by-account/", str(AccID))
    
        return Response
    
    def GetTimeline(self, MatchID):
        response = self.Retrieve("/lol/match/v3/timelines/by-match/", str(MatchID))
        return response
    
    def GetMatch(self, MatchID):
        response = self.Retrieve("/lol/match/v3/matches/", str(MatchID))
        return response
   

def WriteMatch(lol,GamesToCheck, SummonersToCheck, GamesTaken, SummonersTaken):
    try:
        MatchID = GamesToCheck.get()
        GamesTaken[MatchID]=1
        Match = lol.GetMatch(MatchID)
        if (Match["mapId"] != 11):
            return 2
        for Player in Match["participantIdentities"]:
            if Player["player"]["accountId"] not in SummonersTaken:
                SummonersToCheck.put(Player["player"]["accountId"])
        with open(r"C:\Users\mljmo\OneDrive\Data Science\Big Data\Matches\Match_"+str(MatchID)+".json",'w') as f:
            print("Writing to {}".format("Matches/Match_"+str(MatchID)+".json"))
            json.dump(Match, f)
        return 0
    except Exception as ex:
        print("WriteMatchError", type(ex).__name__, ex.args)
        return 1


def GetUser(lol,SummonersToCheck, GamesToCheck, SummonersTaken, GamesTaken):
    try:
        AccountID = SummonersToCheck.get()
        SummonersTaken[AccountID] = 1
        Summoner = lol.GetMatches(AccountID)
        for match in Summoner["matches"]:
            if match["gameId"] not in GamesTaken:
                GamesToCheck.put(match["gameId"])
        return 0
    except Exception as ex:
        print("GetUserError", type(ex).__name__, ex.args)
        return 1
    

#################################################################
def main():
    lol = LeagueOfLegends("RGAPI-bd010d83-fce7-4dc5-befd-9a8d1feb8a76")
    API_Limit = 1/(100./120) #100 requests per 120 seconds
    
    # Creating thread-safe collections
    SummonersToCheck = mp.Queue()
    GamesToCheck = mp.Queue()
    manager = mp.Manager()
    SummonersTaken = manager.dict()
    GamesTaken = manager.dict()
    
    # populating necessary collections
    AccountID = 42427965
    Matches = lol.GetMatches(AccountID)
    for match in Matches["matches"]:
        if match["gameId"] not in GamesTaken:
            GamesToCheck.put(match["gameId"])
    
    
    
    # running appropriate functons
    print("ramping up")
    for i in range(20):
        mp.Process(target=WriteMatch, args=(lol,GamesToCheck, SummonersToCheck, GamesTaken, SummonersTaken)).start()
        time.sleep(API_Limit)
        mp.Process(target=GetUser, args=(lol,SummonersToCheck, GamesToCheck, SummonersTaken,  GamesTaken)).start()
        time.sleep(API_Limit)
        print(GamesToCheck.qsize(), SummonersToCheck.qsize())
    
    
    
    print("For real now")
    count = 0
    NewCount = 0
    while(True):
        if len(mp.active_children()) < mp.cpu_count()+5:
            NewCount=0
            if GamesToCheck.qsize() < min(count, 100):
                p = mp.Process(target=GetUser, args=(lol,SummonersToCheck, GamesToCheck, SummonersTaken,  GamesTaken))
                p.start()
            else:
                p = mp.Process(target=WriteMatch, args=(lol,GamesToCheck, SummonersToCheck, GamesTaken, SummonersTaken))
                p.start()
                count+=1
        else:
            NewCount+=1
        time.sleep(API_Limit)
        if NewCount == 5:
            print(GamesToCheck.qsize(), SummonersToCheck.qsize())
            for Cur_process in mp.active_children():
                Cur_process.join(5)
                if Cur_process.is_alive():
                    Cur_process.terminate()
            NewCount=0    
    
if __name__== "__main__":
    main()