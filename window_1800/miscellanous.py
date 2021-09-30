import pandas as pd


def dividi_eventi1():
    events_1 = pd.read_csv("data/events1_sign.csv")
    events_1b = events_1.loc[~(events_1['X'] != events_1['Z'])]
    events_1a = events_1.loc[~(events_1['X'] == events_1['Z'])]
    events_1a.to_csv("data/events1a_sign.csv", index = False)
    events_1b.to_csv("data/events1b_sign.csv", index = False)
    return


def dividi_eventi2():
    events_2 = pd.read_csv("data/events2_sign.csv")
    events_2b = events_2.loc[~(events_2['A'] != events_2['Y'])]
    events_2a = events_2.loc[~(events_2['A'] == events_2['Y'])]
    events_2a.to_csv("data/events2a_sign.csv", index=False)
    events_2b.to_csv("data/events2b_sign.csv", index=False)
    return

def dividi_eventi4():
    events_4 = pd.read_csv("data/events4_sign.csv")
    events_4a = events_4.loc[~(events_4['A'] == events_4['B'])]
    events_4a = events_4a.loc[~(events_4a['X'] == events_4a['Y'])]

    events_4b = events_4[events_4['A'] == events_4['B']]
    events_4b = events_4b.loc[~(events_4b['X'] == events_4b['Y'])]
    events_4c = events_4[events_4['X'] == events_4['Y']]
    events_4c = events_4c.loc[~(events_4c['A'] == events_4c['B'])]

    events_4d = events_4[events_4['A'] == events_4['B']]
    events_4d = events_4d[events_4d['X'] == events_4d['Y']]

    events_4a.to_csv("data/events4a_sign.csv", index=False)
    events_4b.to_csv("data/events4b_sign.csv", index=False)
    events_4c.to_csv("data/events4c_sign.csv", index=False)
    events_4d.to_csv("data/events4d_sign.csv", index=False)

    return

def dividi_eventi3():
    events_3 = pd.read_csv("data/events3_sign.csv")
    events_3a = events_3.loc[~(events_3['Y'] == events_3['Z'])]
    events_3a = events_3a.loc[~(events_3a['X'] == events_3a['Z'])]
    events_3b = events_3.loc[(events_3['Y'] == events_3['Z'])]
    events_3c = events_3.loc[(events_3['X'] == events_3['Z'])]
    events_3a.to_csv("data/events3a_sign.csv", index=False)
    events_3b.to_csv("data/events3b_sign.csv", index=False)
    events_3c.to_csv("data/events3c_sign.csv", index=False)
    return

def filtro_eventi_sign():
    events = pd.read_csv("data/events.csv")
    events = events[events["significativo"]<0.05]
    events.to_csv("data/events_sign.csv", index=False)


def unione_eventi():
    event_types = ["1a", "1b", "2", "3a", "3b", "3c", "4a", "4b", "4c", "4d"]
    dfs = []
    for event_type in event_types:
        events = pd.read_csv(f"data/events{event_type}_sign.csv")
        events["evento"] = event_type
        if "1" in event_type:
            dfs.append(events.rename(columns={"X":"u1", "Y":"u2", "Z":"u3"}))
        elif "2" in event_type:
            dfs.append(events.rename(columns={"A":"u1", "X":"u2", "Y":"u3"}))
        elif "3" in event_type:
            dfs.append(events.rename(columns={"X":"u1", "Y":"u2", "A":"u3", "Z": "u4"}))
        elif "4" in event_type:
            dfs.append(events.rename(columns={"A":"u1", "X":"u2", "B":"u3", "Y": "u4"}))

    eventsevents = pd.concat(dfs)
    eventsevents.to_csv("data/events.csv", index=False)
    return

def vettorizzazione():
    events = pd.read_csv("data/events.csv")
    #events = events[events["significativo"]<0.05]
    event_types = {"1a": 0, "1b":1, "2":2, "3a": 3, "3b": 4, "3c":5, "4a":6, "4b":7, "4c":8, "4d":9}
    finestra = 43200  #12 ore
    events = events.drop(columns = ["significativo", "p0", "p_cappuccio", "z"])
    events.sort_values(by=["timestamp"], inplace=True)
    events.reset_index(inplace=True, drop=True)

    vector = []
    for i, row in events.iterrows():
        timestamp = row["timestamp"]
        window = events[(events["timestamp"]<= (timestamp + finestra)) & (events["timestamp"]>= (timestamp - finestra))]
        event_type = event_types[row["evento"]]
        features = [timestamp, event_type]
        features += [len(window[window["evento"] == event_type2]) for event_type2 in event_types.keys()]
        unique_users = list(set(window["u1"].to_list() + window["u2"].to_list() + window["u3"].to_list() + window["u4"].to_list()))
        comune_1 = 0
        comune_2 = 0
        for user in unique_users:
            count = len(window[(window["u1"]==user) | (window["u2"]==user)  | (window["u3"]==user) | (window["u4"]==user)])
            if count == 2:
                comune_1 += 1
            elif count >2:
                comune_2 += 1
        features+=[comune_1,comune_2]
        vector.append(tuple(features))

    vector_df = pd.DataFrame(vector, columns=["timestamp","event_type","n_1a", "n_1b", "n_2", "n_3a", "n_3b", "n_3c", "n_4a", "n_4b", "n_4c", "n_4d", "n_1_comune", "n_2_comune"])
    vector_df.to_csv("data/vector_events.csv", index=False)

if __name__ == "__main__":
    print("Inizio")
    #dividi_eventi1()
    #dividi_eventi2()
    #dividi_eventi3()
    #dividi_eventi4()
    #unione_eventi()
    filtro_eventi_sign()
    vettorizzazione()
    print("Fine")
