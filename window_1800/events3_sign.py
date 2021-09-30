import pandas as pd
import math
from scipy.stats import norm
import numpy as np

tabulato = pd.read_csv("creazione_tel_tipizz/data/tabulato_semplificato.csv")
eventi3 = pd.read_csv("creazione_tel_tipizz/data/events/events3.csv")
parts = {}
finestra = 1800
minT = tabulato["timestamp"].min()
maxT = tabulato["timestamp"].max()


def group():
    global eventi3
    eventi3["chiamate"] = eventi3["esito_positivo"] + eventi3["esito_negativo"]
    eventi3 = eventi3.drop(columns=["esito_positivo", "esito_negativo"])
    events3_df_grouped = eventi3.groupby(by=["timestamp", "X", "Y", "A", "Z"], as_index=False).sum()
    eventi3 = events3_df_grouped


def get_part_a(x, y):
    eventsxy = eventi3[eventi3["X"] == x]
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


def merge_parts(part1, part2):
    part1 = [item for sublist in part1 for item in sublist]
    part2 = [item for sublist in part2 for item in sublist]
    timestamps = part1 + part2
    timestamps.sort()
    part = []
    if len(timestamps) != 0:
        start = 0
        start_finestra = True
        prec = 0
        end = 0
        for i, item in enumerate(timestamps):
            if start_finestra:
                start = item
                start_finestra = False
            if item > start + finestra:
                part.append((start, prec + finestra))
                start = item
            if i == len(timestamps) - 1:
                end = item
            prec = item
        part.append((start, end + finestra))
    return part


def get_part_b(partA1, partA2):
    partA = merge_parts(partA1, partA2)
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


def get_p0(x, y, a, z):
    partB = get_part_b(parts[(x, y)]["A"], parts[(y, a)]["A"])
    tabulato_az = tabulato[tabulato["mittente"] == a]
    tabulato_az = tabulato_az[tabulato_az["destinatario"] == z]
    tabulato_az.reset_index(inplace=True, drop=True)
    c = 0
    for _, row in tabulato_az.iterrows():
        timestamp = row["timestamp"]
        for item in partB:
            if item[0] <= timestamp <= item[1]:
                c += 1
                break
            if timestamp < item[0]:
                break
    return c / len(tabulato)


def get_p_cappuccio(x, y, a, z):
    events_xya = eventi3[eventi3["X"] == x]
    events_xya = events_xya[events_xya["Y"] == y]
    events_xya = events_xya[events_xya["A"] == a]
    events_xya.reset_index(inplace=True, drop=True)
    events_xyaz = events_xya[events_xya["Z"] == z]
    events_xyaz.reset_index(inplace=True, drop=True)
    return events_xyaz["chiamate"].sum() / eventi3["chiamate"].sum()  # TODO? su tutti gli eventi


def calcolo_p0_p_hat():
    global tabulato

    tabulato_coppie = tabulato
    tabulato_coppie = tabulato_coppie.drop(
        columns=["timestamp", "durata", "is_mittente_intercettato", "mittente_cella_start", "mittente_cella_end",
                 "is_destinatario_intercettato", "destinatario_cella_start", "destinatario_cella_end", "esito_chiamata",
                 "tipo"])
    tabulato_coppie.drop_duplicates(inplace=True)
    tabulato_coppie.reset_index(inplace=True, drop=True)
    for i, row in tabulato_coppie.iterrows():
        x = row["mittente"]
        y = row["destinatario"]
        parts[(x, y)] = {}
        parts[(x, y)]["A"] = get_part_a(x, y)

    p0 = []
    p_cappuccio = []
    for i, row in eventi3.iterrows():
        x = row["X"]
        y = row["Y"]
        a = row["A"]
        z = row["Z"]
        p0.append(get_p0(x, y, a, z))
        p_cappuccio.append(get_p_cappuccio(x, y, a, z))
    eventi3["p0"] = p0
    eventi3["p_cappuccio"] = p_cappuccio
    eventi3.to_csv("data/events3_sign.csv", index=False)


def n_chiamate():
    global tabulato
    tabulato_coppie = tabulato
    tabulato_coppie = tabulato_coppie.drop(
        columns=["timestamp", "durata", "is_mittente_intercettato", "mittente_cella_start", "mittente_cella_end",
                 "is_destinatario_intercettato", "destinatario_cella_start", "destinatario_cella_end", "esito_chiamata",
                 "tipo"])
    tabulatof = tabulato_coppie.groupby(tabulato_coppie.columns.tolist()).size().reset_index().rename(
        columns={0: 'n_telefonate'})
    for i, row in tabulatof.iterrows():
        parts[(row["mittente"], row["destinatario"])]["n_telefonate"] = row['n_telefonate']
    return


def calcolo_z():
    eventi_p = pd.read_csv("data/events3_sign.csv")
    z = []
    for i, row in eventi_p.iterrows():
        p_hat = row["p_cappuccio"]
        p0 = row["p0"]  # if row["p0"] != 0 else zero_probability_factor
        n = parts[(row["X"], row["Y"])]["n_telefonate"]
        root = math.sqrt((p0 * (1 - p0)) / n)
        z.append((p_hat - p0) / root)
    eventi_p["z"] = z
    eventi_p.to_csv("data/events3_sign.csv", index=False)

    return


def calcolo_sign():
    eventi_z = pd.read_csv("data/events3_sign.csv")
    sign = []
    for i, row in eventi_z.iterrows():
        p = 1 - norm.cdf(row["z"])
        sign.append(p)
    eventi_z["significativo"] = sign
    eventi_z.to_csv("data/events3_sign.csv", index=False)
    return


if __name__ == "__main__":
    print("Inizio")
    #group()
    #calcolo_p0_p_hat()
    #n_chiamate()
    #calcolo_z()
    calcolo_sign()
    print("Fine")
