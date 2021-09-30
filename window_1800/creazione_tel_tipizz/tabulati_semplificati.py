import pandas as pd
import os
import csv
import re
from datetime import datetime

def crea_tabulati():
    phones = pd.read_csv("data/fixed_phones.csv", dtype=str)
    lista = os.listdir('extracted_data')
    with open('data/celle.csv', newline='') as f:
        reader = csv.reader(f, delimiter='\n')
        celle_unflatted = list(reader)
    celle = [item for sublist in celle_unflatted for item in sublist]

    def isInTelefoni(number):
        return len(phones[phones['telefono']== number])>0

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
        print(f"Iniziato con {data}")
        for index, row in df.iterrows():

            mit = str(row["da_numero"])

            if mit[0] == "+":
                mit = "+" + mit[1:].replace("+", "00")
            else:
                mit = mit.replace("+", "00")
            search = ()
            #recupero dell'indice del numero mittente
            if(isInTelefoni(mit)):
                search = get_index_and_flag(mit)
            elif(isInTelefoni("+" + mit)):
                search = get_index_and_flag("+" + mit)
            elif(isInTelefoni("+0" +mit)):
                search = get_index_and_flag("+0" + mit)
            elif(isInTelefoni("+390" + mit)):
                search = get_index_and_flag("+390" + mit)
            elif (isInTelefoni("+39" + mit)):
                search = get_index_and_flag("+39" + mit)
            else:
                print(f'Errore: non ho trovato il mittente {row["da_numero"]}. Tabulato: {data}')
                continue

            da_numero = search[1]
            da_numero_flag = search[0]

            dest = str(row["a_numero"])

            if dest[0] == "+":
                dest = "+" + dest[1:].replace("+", "00")
            else:
                dest = dest.replace("+", "00")
            search = ()
            # recupero dell'indice del numero mittente
            if (isInTelefoni(dest)):
                search = get_index_and_flag(dest)
            elif (isInTelefoni("+" + dest)):
                search = get_index_and_flag("+" + dest)
            elif (isInTelefoni("+0" + dest)):
                search = get_index_and_flag("+0" + dest)
            elif (isInTelefoni("+390" + dest)):
                search = get_index_and_flag("+390" + dest)
            elif (isInTelefoni("+39" + dest)):
                search = get_index_and_flag("+39" + dest)
            else:
                print(f'Errore: non ho trovato il destinatario {row["da_numero"]}. Tabulato: {data}')
                continue

            a_numero = search[1]
            a_numero_flag = search[0]

            #conversione della data in secondi a distanza dell'epoca
            result = re.findall(r"([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})",
                                row["data_ora"])
            date = datetime
            if len(result) == 0:
                result = re.findall(r"([0-9]{2})/([0-9]{2})/([0-9]{4}) ([0-9]{2}):([0-9]{2}):([0-9]{2})",
                                    row["data_ora"])
                result = [int(x) for x in result[0] if x]
                date = datetime(result[2], result[1], result[0], result[3], result[4], result[5], 0)
            else:
                result = [int(x) for x in result[0] if x]
                date = datetime(result[0], result[1], result[2], result[3], result[4], result[5], 0)
            timestamp = (date - datetime(1970,1,1)).total_seconds()

            #recupero delle celle mittente destinatario
            mittente_cella_start = get_cella_index(row["da_torre_cell_inizio"]) if not str(
                row["da_torre_cell_inizio"]) == "nan" else ""
            mittente_cella_end = get_cella_index(row["da_torre_cell_fine"]) if not str(
                row["da_torre_cell_fine"]) == "nan" else ""
            destinatario_cella_start = get_cella_index(row["a_torre_cell_inizio"]) if not str(
                row["a_torre_cell_inizio"]) == "nan" else ""
            destinatario_cella_end = get_cella_index(row["a_torre_cell_fine"]) if not str(
                row["a_torre_cell_fine"]) == "nan" else ""

            tabulato_ridotto.append(
                (timestamp, row["durata"], da_numero, da_numero_flag, mittente_cella_start,
                 mittente_cella_end, a_numero, a_numero_flag, destinatario_cella_start,
                 destinatario_cella_end, row["esito_chiamata"], row["tipo"]))


        tabulati_ridotti_df = pd.DataFrame(tabulato_ridotto,
                                           columns=["timestamp", "durata", "mittente", "is_mittente_intercettato",
                                                    "mittente_cella_start", "mittente_cella_end", "destinatario",
                                                    "is_destinatario_intercettato", "destinatario_cella_start",
                                                    "destinatario_cella_end", "esito_chiamata", "tipo"])
        tabulati_ridotti_df.to_csv(f"semplified/{data}", index=False)
        print(f"Finito con {data}")

    print("Finito")


if __name__ == "__main__":
    crea_tabulati()
