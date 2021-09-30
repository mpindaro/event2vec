import pandas as pd
import math
from scipy.stats import norm
import numpy as np

tabulato = pd.read_csv("creazione_tel_tipizz/simpy_dataset.csv")
tabulato.sort_values(by=["timestamp"], inplace=True)
tabulato.reset_index(inplace=True, drop=True)
eventi1 = pd.read_csv("creazione_tel_tipizz/data/events/events1.csv")
partsXY = {}
finestra = 9000


def group_e1():
    global eventi1
    eventi1 = eventi1.drop(columns=["timestamp2"])
    eventi1["chiamate"] = eventi1["esito_positivo"] + eventi1["esito_negativo"]
    eventi1 = eventi1.drop(columns=["esito_positivo", "esito_negativo"])
    events1dfgrouped = eventi1.groupby(by=["timestamp", "X", "Y", "Z"], as_index=False).sum()
    eventi1 = events1dfgrouped


def get_part_a_xy(x, y):
    eventsxy = eventi1[eventi1["X"] == x]
    eventsxy = eventsxy[eventsxy["Y"] == y]
    intervals = []

    if len(eventsxy) != 0:
        start = 0
        start_finestra = True
        prec = 0
        end = 0
        for i, row in eventsxy.iterrows():
            if start_finestra:
                start = row["timestamp"]
                start_finestra = False
            if row["timestamp"] > start + finestra:
                intervals.append((start, prec + finestra))
                start = row["timestamp"]
            if i == len(eventsxy) - 1:
                end = row["timestamp"]
            prec = row["timestamp"]

        intervals.append((start, end + finestra))
    return intervals


def get_part_b_xy(partA, minT, maxT):
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


def get_p0(x, y, z):
    partB = partsXY[(x, y)]["B"]
    tabulato_yz = tabulato[tabulato["mittente"] == y]
    tabulato_yz = tabulato_yz[tabulato_yz["destinatario"] == z]
    tabulato_yz.reset_index(inplace=True, drop=True)
    c = 0
    for _, row in tabulato_yz.iterrows():
        timestamp = row["timestamp"]
        for item in partB:
            if item[0] <= timestamp <= item[1]:
                c += 1
                break
            if timestamp < item[0]:
                break
    return c / len(tabulato)  # TODO: togliere filtro


def get_p_cappuccio(x, y, z):
    eventsxy = eventi1[eventi1["X"] == x]
    eventsxy = eventsxy[eventsxy["Y"] == y]
    eventsxy.reset_index(inplace=True, drop=True)
    eventsxyz = eventsxy[eventsxy["Z"] == z]
    eventsxyz.reset_index(inplace=True, drop=True)
    return eventsxyz["chiamate"].sum() / eventi1["chiamate"].sum()  # TODO? su tutti gli eventi


def calcolo_p0_p_hat():
    global tabulato
    minT = tabulato["timestamp"].min()
    maxT = tabulato["timestamp"].max()
    tabulato = tabulato[tabulato["is_mittente_intercettato"] == "S"]
    tabulato = tabulato[tabulato["is_destinatario_intercettato"] == "S"]
    tabulato_coppie = tabulato
    tabulato_coppie = tabulato_coppie.drop(
        columns=["timestamp", "durata", "is_mittente_intercettato",
                 "is_destinatario_intercettato",  "esito_chiamata",
                 "tipo"])
    tabulato_coppie.drop_duplicates(inplace=True)
    tabulato_coppie.reset_index(inplace=True, drop=True)
    for i, row in tabulato_coppie.iterrows():
        x = row["mittente"]
        y = row["destinatario"]
        partsXY[(x, y)] = {}
        partsXY[(x, y)]["A"] = get_part_a_xy(x, y)
        partsXY[(x, y)]["B"] = get_part_b_xy(partsXY[(x, y)]["A"], minT, maxT)
    p0 = []
    p_cappuccio = []
    for i, row in eventi1.iterrows():
        x = row["X"]
        y = row["Y"]
        z = row["Z"]
        p0.append(get_p0(x, y, z))
        p_cappuccio.append(get_p_cappuccio(x, y, z))
    eventi1["p0"] = p0
    eventi1["p_cappuccio"] = p_cappuccio
    eventi1.to_csv("data/events1_sign.csv", index=False)


def n_chiamate_xy():
    global tabulato
    tabulato_coppie = tabulato
    tabulato_coppie = tabulato_coppie.drop(
        columns=["timestamp", "durata", "is_mittente_intercettato", "is_destinatario_intercettato",  "esito_chiamata","tipo"])
    tabulatof = tabulato_coppie.groupby(tabulato_coppie.columns.tolist()).size().reset_index().rename(
        columns={0: 'n_telefonate'})
    for i, row in tabulatof.iterrows():
        # partsXY[(row["mittente"], row["destinatario"])] = {}
        partsXY[(row["mittente"], row["destinatario"])]["n_telefonate"] = row['n_telefonate']
    return


def calcolo_z():
    eventi_p = pd.read_csv("data/events1_sign.csv")
    z = []
    for i, row in eventi_p.iterrows():
        p_hat = row["p_cappuccio"]
        p0 = row["p0"]  # if row["p0"] != 0 else zero_probability_factor
        n = partsXY[(row["X"], row["Y"])]["n_telefonate"]
        root = math.sqrt((p0 * (1 - p0)) / n)
        z.append((p_hat - p0) / root)
    eventi_p["z"] = z
    eventi_p.to_csv("data/events1_sign.csv", index=False)

    return


def calcolo_sign():
    eventi_z = pd.read_csv("data/events1_sign.csv")
    sign = []
    for i, row in eventi_z.iterrows():
        p = 1 - norm.cdf(row["z"])
        sign.append(p)
    eventi_z["significativo"] = sign
    eventi_z.to_csv("data/events1_sign.csv", index=False)
    return


if __name__ == "__main__":
    print("Inizio")
    group_e1()
    calcolo_p0_p_hat()
    n_chiamate_xy()
    calcolo_z()
    calcolo_sign()
    print("Fine")
