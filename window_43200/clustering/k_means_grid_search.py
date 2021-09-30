import pandas as pd
from sklearn.cluster import KMeans, MiniBatchKMeans, DBSCAN
import numpy as np
from sklearn import metrics
from datetime import datetime

def get_filtered_events(path):
    date = datetime(2017, 4, 1)
    inizio = (date - datetime(1970, 1, 1)).total_seconds()
    date = datetime(2017, 7, 15)
    fine = (date - datetime(1970, 1, 1)).total_seconds()
    events = pd.read_csv(path)
    filtered = events[events["timestamp"] >= inizio]
    filtered = filtered[filtered["timestamp"] <= fine]
    return filtered

def grid_search():
    events = get_filtered_events("../data/vector_events.csv")
    events_no_timestamp = filtered[
        ["event_type", "n_1a", "n_1b", "n_2", "n_3a", "n_3b", "n_3c", "n_4a", "n_4b", "n_4c", "n_4d", "n_1_comune",
         "n_2_comune"]]
    events_no_timestamp_and_optional = filtered[["event_type", "n_1a", "n_1b", "n_2", "n_3a", "n_3b", "n_3c", "n_4a", "n_4b", "n_4c", "n_4d"]]

    #timestamps = filtered[["timestamp"]]

    print("Grid search eventi senza \"optional\"")
    k_set = np.arange(3, 23, 2)
    for k in k_set:
        km = KMeans(n_clusters=k, init='k-means++', max_iter=100, n_init=1)
        km.fit(events_no_timestamp_and_optional)
        print(k, metrics.silhouette_score(events_no_timestamp_and_optional, km.labels_))

if __name__ == "__main__":
    grid_search()
