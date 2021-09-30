import pandas as pd


def event4():
    finestra = 9000
    events4 = []
    ifile = pd.read_csv("simpy_dataset.csv")

    ifile.sort_values(by=["timestamp"], inplace=True)
    ifile.reset_index(inplace=True, drop=True)

    for i1 in range(len(ifile)):

        row = ifile.iloc[i1]

        if row["is_mittente_intercettato"] == "N" and row["is_destinatario_intercettato"] == "S":
            a = row["mittente"]
            x = row["destinatario"]
            timestamp1 = row["timestamp"]

            for i2 in range(i1 + 1, len(ifile)):
                row2 = ifile.iloc[i2]
                if row2["timestamp"] > (timestamp1 + finestra):
                    break
                elif row2["is_destinatario_intercettato"] == "N" and row2["mittente"] == x:
                    b = row2["destinatario"]
                    for i3 in range(i2 + 1, len(ifile)):
                        row3 = ifile.iloc[i3]
                        if row3["timestamp"] > (row2["timestamp"] + finestra):
                            break
                        elif row3["is_destinatario_intercettato"] == "S" and row3["mittente"] == b:
                            esito = 3
                            y = row3["destinatario"]
                            events4.append((timestamp1, a, x, b, y, esito))

    return events4


if __name__ == "__main__":
    events4 = event4()
    events4df = pd.DataFrame(events4, columns=["timestamp", "A", "X", "B", "Y", "esito_positivo"])
    events4df.to_csv("data/events/events4raw.csv", index=False)
    # events4dfgrouped = events4df.groupby(by=["timestamp", "A", "X", "B", "Y"], as_index=False).sum()
    # events4dfgrouped["esito_negativo"] = (
    #         events4df.groupby(by=["timestamp", "A", "Y", "Z"], as_index=False).count()["esito_positivo"] -
    #         events4dfgrouped["esito_positivo"]).values
    #
    # events4dfgrouped.to_csv("data/events/events4.csv", index=False)
