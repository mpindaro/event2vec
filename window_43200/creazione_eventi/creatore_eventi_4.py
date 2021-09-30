import pandas as pd


def event4():
    finestra = 43200
    events4 = []
    ifile = pd.read_csv("../../dati_pre_eventi/data/tabulato_semplificato.csv")

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
                            esito = 0
                            if (row["esito_chiamata"] == 0 and row["tipo"] == "V") or not (row["tipo"] == "V"):
                                esito += 1
                            if (row2["esito_chiamata"] == 0 and row2["tipo"] == "V") or not (row2["tipo"] == "V"):
                                esito += 1
                            if (row3["esito_chiamata"] == 0 and row3["tipo"] == "V") or not (row3["tipo"] == "V"):
                                esito += 1
                            y = row3["destinatario"]
                            events4.append((timestamp1, a, x, b, y, esito))

    return events4


if __name__ == "__main__":
    events4 = event4()
    events4df = pd.DataFrame(events4, columns=["timestamp", "A", "X", "B", "Y", "esito_positivo"])
    events4df.to_csv("data/events/events4raw.csv", index=False)

