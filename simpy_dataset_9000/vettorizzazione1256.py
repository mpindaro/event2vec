import pandas as pd


def vettorizzazione():
    events = pd.read_csv("data/events1256.csv")
    events = events[events["significativo"] <= 0.05]
    event_types = {"1a": 0, "1b": 1, "2": 2, "5": 3, "6": 4}
    finestra = 43200  # 12 ore

    events = events.drop(columns=["significativo", "p0", "p_cappuccio", "z"])
    events.sort_values(by=["timestamp"], inplace=True)
    events.reset_index(inplace=True, drop=True)

    vector = []
    for i, row in events.iterrows():
        timestamp = row["timestamp"]
        u1 = row["u1"]
        u2 = row["u2"]
        u3 = row["u3"]
        window = events[
            (events["timestamp"] <= (timestamp + finestra)) & (events["timestamp"] >= (timestamp - finestra))]
        event_type = event_types[str(row["evento"])]
        features = [timestamp, event_type]
        features += [len(window[window["evento"] == event_type2]) for event_type2 in event_types.keys()]
        # in questo modo vengono considerati tutti gli user di eventi della finestra nel calcolo degli eventi che hanno uno user in comune
#        unique_users = list(set(window["u1"].to_list() + window["u2"].to_list() + window["u3"].to_list()))
        unique_users = set([u1,u2,u3])   # considero gli user dell'evento che sto trattando
        comune_1 = 0
        comune_2 = 0
        comune_3 = 0
        for user in unique_users:
            count = len(window[((window["u1"] == user) | (window["u2"] == user) | (window["u3"] == user)) & ((window["evento"] == '1a') | (window["evento"] == '1b'))])
            if event_type in ['1a','1b']:
                # tolgo 1 perche dovrebbe aver contato anche l'evento che sto esaminando
                comune_1 = count -1
            else:
                comune_1 = count
            count = len(window[((window["u1"] == user) | (window["u2"] == user) | (window["u3"] == user)) & ((window["evento"] == '2') | (window["evento"] == '5') | (window["evento"] == '6'))])
            if event_type in ['2','5','6']:
                # tolgo 1 perche dovrebbe aver contato anche l'evento che sto esaminando
                comune_2 = count -1
            else:
                comune_2 = count
        uu = list(unique_users)
        # conto gli eventi 1a e 1b che hanno in comune i primi 2 user. In pratica dovrebbe essere significativo soprattutto per gli eventi 1b (con i dati che abbiamo... molto ad hoc...)
        count = len(window[(((window["u1"] == uu[0]) & (window["u2"] == uu[1])) | ((window["u1"] == uu[1]) & (window["u2"] == uu[0]))) & ((window["evento"] == '1a') | (window["evento"] == '1b'))])
        if event_type in ['1a','1b']:
            # tolgo 1 perche dovrebbe aver contato anche l'evento che sto esaminando
            comune_3 = count -1
        else:
            comune_3 = count
            
#            count = len(window[(window["u1"] == user) | (window["u2"] == user) | (window["u3"] == user)])
# non avevo capito come facevi il conto...
#            if count == 2:
#                comune_1 += 1
#            elif count > 2:
#                comune_2 += 1
        features += [comune_1, comune_2, comune_3]
        features += [u1, u2, u3]
        vector.append(tuple(features))

    vector_df = pd.DataFrame(vector,
                             columns=["timestamp", "event_type", "n_1a", "n_1b", "n_2", "n_5", "n_6", "n_1_comune",
                                      "n_2_comune", "n_1_comune2", "u1", "u2", "u3"])
    vector_df.to_csv("data/vector_events_1256.csv", index=False)


if __name__ == "__main__":
    print("Inizio")
    vettorizzazione()
    print("Fine")
