import pandas as pd


def event2():
    ifile = pd.read_csv("simpy_dataset.csv")
    finestra = 9000
    events2 = []

    ifile.sort_values(by=["timestamp"], inplace=True)
    ifile.reset_index(inplace=True, drop=True)

    for i1 in range(len(ifile)):
        row = ifile.iloc[i1]
        a = row["mittente"]
        x = row["destinatario"]
        timestamp1 = row["timestamp"]

        if row["is_mittente_intercettato"] == "N" and row["is_destinatario_intercettato"] == "S":
            for i2 in range(i1 + 1, len(ifile)):

                row2 = ifile.iloc[i2]
                if row2["timestamp"] > (timestamp1 + finestra):
                    break
                elif row2["mittente"] == x and row2["is_destinatario_intercettato"] == "S":
                    y = row2["destinatario"]
                    timestamp2 = row2["timestamp"]
                    esito = 2

                    events2.append((timestamp1, a, x, y, timestamp2, esito))

    return events2


if __name__ == "__main__":
    events2 = event2()
    events2df = pd.DataFrame(events2, columns=["timestamp", "A", "X", "Y", "timestamp2", "esito_positivo"])
    events2df.to_csv("data/events/events2raw.csv", index=False)
    # events2dfgrouped = events2df.groupby(by=["timestamp", "A", "X", "Y"], as_index=False).sum()
    # events2dfgrouped["esito_negativo"] = (events2df.groupby(by=["timestamp", "A", "X", "Y"], as_index=False).count()["esito_positivo"] - events2dfgrouped["esito_positivo"]).values
    # events2dfgrouped.to_csv("data/events/events2.csv", index=False)
