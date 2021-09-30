import pandas as pd


def event3():
    finestra = 9000
    events3 = []
    ifile = pd.read_csv("simpy_dataset.csv")

    ifile.sort_values(by=["timestamp"], inplace=True)
    ifile.reset_index(inplace=True, drop=True)

    for i1 in range(len(ifile)):

        row = ifile.iloc[i1]

        if row["is_mittente_intercettato"] == "S" and row["is_destinatario_intercettato"] == "S":
            x = row["mittente"]
            y = row["destinatario"]
            timestamp1 = row["timestamp"]

            for i2 in range(i1 + 1, len(ifile)):
                row2 = ifile.iloc[i2]
                if row2["timestamp"] > (timestamp1 + finestra):
                    break
                elif row2["is_destinatario_intercettato"] == "N" and row2["mittente"] == y:
                    a = row2["destinatario"]
                    for i3 in range(i2 + 1, len(ifile)):
                        row3 = ifile.iloc[i3]
                        if row3["timestamp"] > (row2["timestamp"] + finestra):
                            break
                        elif row3["is_destinatario_intercettato"] == "S" and row3["mittente"] == a:
                            esito = 3

                            z = row3["destinatario"]
                            events3.append((timestamp1, x, y, a, z, esito))

    return events3


if __name__ == "__main__":
    events3 = event3()
    events3df = pd.DataFrame(events3, columns=["timestamp", "X", "Y", "A", "Z", "esito_positivo"])
    events3df.to_csv("data/events/events3raw.csv", index=False)
    # events3dfgrouped = events3df.groupby(by=["timestamp", "X", "Y", "A", "Z"], as_index=False).sum()
    # events3dfgrouped["esito_negativo"] = (
    #         events3df.groupby(by=["timestamp", "A", "Y", "Z"], as_index=False).count()["esito_positivo"] -
    #         events3dfgrouped["esito_positivo"]).values
    #
    # events3dfgrouped.to_csv("data/events/events3.csv", index=False)
