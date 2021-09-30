import pandas as pd
import os


def concat_tabulati():

    frames = []

    lista = os.listdir('semplified')
    frames = []
    for tabulato_path in lista:
        frames.append(pd.read_csv(f"../../dati_pre_eventi/semplified/{tabulato_path}"))
    tabulato = pd.concat(frames)
    tabulato = tabulato.sort_values(by=['timestamp','mittente', 'destinatario'])
    tabulato = tabulato.astype({'timestamp':'int32'})
    print(len(tabulato))
    tabulato.to_csv("data/tabulato_semplificato.csv", index=False)



if __name__=="__main__":
    concat_tabulati()