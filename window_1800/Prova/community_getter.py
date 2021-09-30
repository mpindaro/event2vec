import argparse
import pandas as pd

def read_txt(overlapped):
    if overlapped:
        f = open("overlapping_communities.txt", "r")
    else:
        f = open("communities.txt", "r")
    return eval(f.read())
def generate_df(n_community, overlapped):

    communities = read_txt(overlapped)
    if n_community > len(communities):
        print("Indice troppo grande")
        return
    asked_community = list(communities[n_community])
    events_sign = pd.read_csv("../data/events_sign.csv")
    events = []
    for event_index in asked_community:
        event = tuple(events_sign.iloc[event_index,: ].to_list())
        events.append(event)
    events_df = pd.DataFrame(events, columns=["timestamp","u1","u2","u3","chiamate","p0","p_cappuccio","z","significativo","evento","u4"])
    events_df.to_csv("temp.csv", index=False)

    return


def read_inputs():
    parser = argparse.ArgumentParser()

    parser.add_argument('community', type=int,help='Numero della community richiesta, sono in ordine decrescente per size')
    parser.add_argument('-o', '--overlapped', action="store_true", default=False,
                        help='Per avere le community overlapped')
    args = parser.parse_args()
    generate_df(args.community, args.overlapped)


if __name__ == "__main__":
    read_inputs()