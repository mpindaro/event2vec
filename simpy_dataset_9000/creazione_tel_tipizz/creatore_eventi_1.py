import pandas as pd


def event1():
    finestra = 9000
    events1 = []
    ifile = pd.read_csv("simpy_dataset.csv")
    mittenti_indagati = ifile[ifile["is_mittente_intercettato"] == "S"]
    tabulati_indagati = mittenti_indagati[mittenti_indagati["is_destinatario_intercettato"] == "S"]

    tabulati_indagati.sort_values(by=["timestamp"], inplace=True)
    tabulati_indagati.reset_index(inplace=True, drop=True)

    for i1 in range(len(tabulati_indagati)):
        row = tabulati_indagati.iloc[i1]
        x = row["mittente"]
        y = row["destinatario"]
        timestamp1 = row["timestamp"]

        for i2 in range(i1 + 1, len(tabulati_indagati)):
            row2 = tabulati_indagati.iloc[i2]

            if row2["timestamp"] > (timestamp1 + finestra):
                break
            elif row2["mittente"] == y:
                z = row2["destinatario"]
                esito =2


                timestamp2 = row2["timestamp"]
                events1.append((timestamp1, x, y, z, timestamp2, esito))

    return events1


if __name__ == "__main__":
    events1 = event1()
    events1df = pd.DataFrame(events1, columns=["timestamp", "X", "Y", "Z", "timestamp2", "esito_positivo"])
    events1df.to_csv("data/events/events1raw.csv", index=False)
    # events1dfgrouped = events1df.groupby(by=["timestamp", "X", "Y", "Z", "timestamp2"], as_index=False).sum()
    # events1dfgrouped["esito_negativo"] = (events1df.groupby(by=["timestamp", "X", "Y", "Z", "timestamp2"], as_index=False).count()["esito_positivo"] - events1dfgrouped["esito_positivo"]).values
    # events1dfgrouped.to_csv("data/events/events1.csv", index=False)
