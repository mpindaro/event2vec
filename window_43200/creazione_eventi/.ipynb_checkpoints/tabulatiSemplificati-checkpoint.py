import pandas as pd
import os
import csv


def crea_tabulati():
    phones = pd.read_csv("fixed_phones.csv", dtype=str)
    lista = os.listdir('extracted_data')
    with open('celle.csv', newline='') as f:
        reader = csv.reader(f, delimiter='\n')
        celle_unflatted = list(reader)
    celle = [item for sublist in celle_unflatted for item in sublist]

    def get_index_and_flag(number):
        return (
            list(phones[phones['telefono'] == number].values[0])[1],
            phones.index[phones['telefono'] == number].tolist()[0])

    def get_cella_index(cella):
        return celle.index(cella) if (cella in celle) else ""

    print("Iniziato")
    

    for data in lista:
        tabulato_ridotto = []
        df = pd.read_csv(f"extracted_data/{data}", delimiter=";")
        for index, row in df.iterrows():
            try:
                if str(row["da_numero"])[0:1] == "+":
                    search = get_index_and_flag("+" + str(row["da_numero"][1:]).replace("+", "00"))
                else:
                    search = get_index_and_flag("+" + str(row["da_numero"]).replace("+", "00"))

                da_numero = search[1]
                da_numero_flag = search[0]
                if str(row["a_numero"])[0:1] == "+":
                    search = get_index_and_flag("+" + str(row["a_numero"][1:]).replace("+", "00"))
                else:
                    search = get_index_and_flag("+" + str(row["a_numero"]).replace("+", "00"))
                a_numero = search[1]
                a_numero_flag = search[0]

                mittente_cella_start = get_cella_index(row["da_torre_cell_inizio"]) if not str(
                    row["da_torre_cell_inizio"]) == "nan" else ""
                mittente_cella_end = get_cella_index(row["da_torre_cell_fine"]) if not str(
                    row["da_torre_cell_fine"]) == "nan" else ""
                destinatario_cella_start = get_cella_index(row["a_torre_cell_inizio"]) if not str(
                    row["a_torre_cell_inizio"]) == "nan" else ""
                destinatario_cella_end = get_cella_index(row["a_torre_cell_fine"]) if not str(
                    row["a_torre_cell_fine"]) == "nan" else ""

                tabulato_ridotto.append(
                    (row["data_ora"], row["durata"], da_numero, da_numero_flag, mittente_cella_start,
                     mittente_cella_end, a_numero, a_numero_flag, destinatario_cella_start,
                     destinatario_cella_end))
            except:
                print(f'Errore: non ho trovato {row["da_numero"]} o {row["a_numero"]}. Tabulato: {data}')
        tabulati_ridotti_df = pd.DataFrame(tabulato_ridotto,
                                           columns=["timestamp", "durata", "mittente", "is_mittente_intercettato",
                                                    "mittente_cella_start", "mittente_cella_end", "destinatario",
                                                    "is_destinatario_intercettato", "destinatario_cella_start",
                                                    "destinatario_cella_end"])
        tabulati_ridotti_df.to_csv(f"semplified/{data}", index=False)

        print("Finito")


if __name__ == "__main__":
    crea_tabulati()
