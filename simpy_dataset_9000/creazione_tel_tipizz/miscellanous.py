import pandas as pd

def raggruppa_e1():
    events1df = pd.read_csv("data/events/events1raw.csv")
    events1dfgrouped = events1df.groupby(by=["timestamp", "X", "Y", "Z", "timestamp2"], as_index=False).sum()
    events1dfgrouped["esito_negativo"] = (events1df.groupby(by=["timestamp", "X", "Y", "Z", "timestamp2"], as_index=False).count()["esito_positivo"]*2 - events1dfgrouped["esito_positivo"]).values
    events1dfgrouped.to_csv("data/events/events1.csv", index=False)
    return

def raggruppa_e2():
    events2df = pd.read_csv("data/events/events2raw.csv")
    events2dfgrouped = events2df.groupby(by=["timestamp", "A", "X", "Y", "timestamp2"], as_index=False).sum()
    events2dfgrouped["esito_negativo"] = (events2df.groupby(by=["timestamp","A", "X", "Y", "timestamp2"], as_index=False).count()["esito_positivo"]*2 - events2dfgrouped["esito_positivo"]).values
    events2dfgrouped.to_csv("data/events/events2.csv", index=False)
    return

def raggruppa_e5():
    events2df = pd.read_csv("data/events/events5raw.csv")
    events2dfgrouped = events2df.groupby(by=["timestamp", "A1", "X", "A2", "timestamp2"], as_index=False).sum()
    events2dfgrouped["esito_negativo"] = (events2df.groupby(by=["timestamp","A1", "X", "A2", "timestamp2"], as_index=False).count()["esito_positivo"]*2 - events2dfgrouped["esito_positivo"]).values
    events2dfgrouped.to_csv("data/events/events5.csv", index=False)
    return

def raggruppa_e6():
    events2df = pd.read_csv("data/events/events2raw.csv")
    events2dfgrouped = events2df.groupby(by=["timestamp", "X1", "A", "X2", "timestamp2"], as_index=False).sum()
    events2dfgrouped["esito_negativo"] = (events2df.groupby(by=["timestamp","X1", "A", "X2", "timestamp2"], as_index=False).count()["esito_positivo"]*2 - events2dfgrouped["esito_positivo"]).values
    events2dfgrouped.to_csv("data/events/events6.csv", index=False)
    return

def raggruppa_e3():
    events3df = pd.read_csv("data/events/events3raw.csv")
    events3df.drop_duplicates(inplace=True)
    events3dfgrouped = events3df.groupby(by=["timestamp", "X", "Y", "A", "Z"], as_index=False).sum()
    events3dfgrouped["esito_negativo"] = (
                events3df.groupby(by=["timestamp", "X", "Y", "A", "Z"], as_index=False).count()[
                    "esito_positivo"] * 3 - events3dfgrouped["esito_positivo"]).values
    events3dfgrouped.to_csv("data/events/events3.csv", index=False)
    return

def raggruppa_e4():
    events4df = pd.read_csv("data/events/events4raw.csv")
    events4df.drop_duplicates(inplace=True)
    events4dfgrouped = events4df.groupby(by=["timestamp", "A", "X", "B", "Y"], as_index=False).sum()
    events4dfgrouped["esito_negativo"] = (
                events4df.groupby(by=["timestamp", "A", "X", "B", "Y"], as_index=False).count()[
                    "esito_positivo"] * 3 - events4dfgrouped["esito_positivo"]).values
    events4dfgrouped.to_csv("data/events/events4.csv", index=False)
    return

def rimozione_duplicati_tabulato():
    tabulato = pd.read_csv("data/tabulato_semplificato.csv")
    print(len(tabulato))
    return


if __name__=="__main__":
    #raggruppa_e1()
    #raggruppa_e2()
    #raggruppa_e3()
    #raggruppa_e4()
    raggruppa_e5()
    raggruppa_e6()
