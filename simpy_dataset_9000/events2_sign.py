import pandas as pd
import math
from scipy.stats import norm
import numpy as np

tabulato = pd.read_csv("creazione_tel_tipizz/simpy_dataset.csv")
tabulato.sort_values(by=["timestamp"], inplace=True)
tabulato.reset_index(inplace=True, drop=True)
eventi2 = pd.read_csv("creazione_tel_tipizz/data/events/events2.csv")
partsAX = {}
finestra = 9000


def group_e2():
    global eventi2
    eventi2 = eventi2.drop(columns=["timestamp2"])
    eventi2["chiamate"] = eventi2["esito_positivo"] + eventi2["esito_negativo"]
    eventi2 = eventi2.drop(columns=["esito_positivo", "esito_negativo"])
    events2_df_grouped = eventi2.groupby(by=["timestamp", "A", "X", "Y"], as_index=False).sum()
    eventi2 = events2_df_grouped


def get_part_a_ax(a, x):
    events_xy = eventi2[eventi2["A"] == a]
    events_xy = events_xy[events_xy["X"] == x]
    intervals = []
    if len(events_xy) != 0:
        start = 0
        start_finestra = True
        prec = 0
        end = 0
        for i, row in events_xy.iterrows():
            if start_finestra:
                start = row["timestamp"]  # start = 2800
                start_finestra = False
            if row["timestamp"] > start + finestra:  # 3200 > 2800 + 1000
                intervals.append((start, prec + finestra))
                start = row["timestamp"]
            if i == len(events_xy) - 1:
                end = row["timestamp"]
            prec = row["timestamp"]

        intervals.append((start, end + finestra))
    return intervals


def get_part_b_ax(partA, minT, maxT):
    intervals = []
    if len(partA) != 0:
        start = minT
        for item in partA:
            end = item[0] - 1
            intervals.append((start, end))
            start = item[1] + 1
        intervals.append((start, maxT))
        return intervals
    else:
        return [(minT, maxT)]


def get_p0(a, x, y):
    partB = partsAX[(a, x)]["B"]
    tabulato_xy = tabulato[tabulato["mittente"] == x]
    tabulato_xy = tabulato_xy[tabulato_xy["destinatario"] == y]
    tabulato_xy.reset_index(inplace=True, drop=True)
    c = 0
    for _, row in tabulato_xy.iterrows():
        timestamp = row["timestamp"]
        for item in partB:
            if item[0] <= timestamp <= item[1]:
                c += 1
                break
            if timestamp < item[0]:
                break
    return c / len(tabulato)


def get_p_cappuccio(a, x, y):
    events_ax = eventi2[eventi2["A"] == a]
    events_ax = events_ax[events_ax["X"] == x]
    events_ax.reset_index(inplace=True, drop=True)
    events_axy = events_ax[events_ax["Y"] == y]
    events_axy.reset_index(inplace=True, drop=True)
    return events_axy["chiamate"].sum() / eventi2["chiamate"].sum()


def calcolo_p0_p_hat():
    global tabulato
    minT = tabulato["timestamp"].min()
    maxT = tabulato["timestamp"].max()
    tabulato_coppie = tabulato
    tabulato_coppie = tabulato_coppie.drop(
        columns=["timestamp", "durata", "is_mittente_intercettato",
                 "is_destinatario_intercettato", "esito_chiamata",
                 "tipo"])
    tabulato_coppie.drop_duplicates(inplace=True)
    tabulato_coppie.reset_index(inplace=True, drop=True)
    for i, row in tabulato_coppie.iterrows():
        a = row["mittente"]
        x = row["destinatario"]
        partsAX[(a, x)] = {}
        partsAX[(a, x)]["A"] = get_part_a_ax(a, x)
        partsAX[(a, x)]["B"] = get_part_b_ax(partsAX[(a, x)]["A"], minT, maxT)
    p0 = []
    p_cappuccio = []
    for i, row in eventi2.iterrows():
        a = row["A"]
        x = row["X"]
        y = row["Y"]
        p0.append(get_p0(a, x, y))
        p_cappuccio.append(get_p_cappuccio(a, x, y))
    eventi2["p0"] = p0
    eventi2["p_cappuccio"] = p_cappuccio
    eventi2.to_csv("data/events2_sign.csv", index=False)


def n_chiamate_xy():
    global tabulato
    tabulato_coppie = tabulato
    tabulato_coppie = tabulato_coppie.drop(
        columns=["timestamp", "durata", "is_mittente_intercettato",
                 "is_destinatario_intercettato", "esito_chiamata",
                 "tipo"])
    tabulatof = tabulato_coppie.groupby(tabulato_coppie.columns.tolist()).size().reset_index().rename(
        columns={0: 'n_telefonate'})
    for i, row in tabulatof.iterrows():
        partsAX[(row["mittente"], row["destinatario"])]["n_telefonate"] = row['n_telefonate']
    return


def calcolo_z():
    eventi_p = pd.read_csv("data/events2_sign.csv")
    z = []
    for i, row in eventi_p.iterrows():
        p_hat = row["p_cappuccio"]
        p0 = row["p0"]  # if row["p0"] != 0 else zero_probability_factor
        n = partsAX[(row["A"], row["X"])]["n_telefonate"]
        root = math.sqrt((p0 * (1 - p0)) / n)
        z.append((p_hat - p0) / root)
    eventi_p["z"] = z
    eventi_p.to_csv("data/events2_sign.csv", index=False)

    return


def calcolo_sign():
    eventi_z = pd.read_csv("data/events2_sign.csv")
    sign = []
    for i, row in eventi_z.iterrows():
        p = 1 - norm.cdf(row["z"])
        sign.append(p)
    eventi_z["significativo"] = sign
    eventi_z.to_csv("data/events2_sign.csv", index=False)
    return


if __name__ == "__main__":
    print("Inizio")
    group_e2()
    calcolo_p0_p_hat()
    n_chiamate_xy()
    calcolo_z()
    calcolo_sign()
    print("Fine")
