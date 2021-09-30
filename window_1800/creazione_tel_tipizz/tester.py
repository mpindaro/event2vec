import pandas as pd


if __name__ == "__main__":
    events1 = pd.read_csv("data/events/events1.csv", dtype=str)
    events2 = pd.read_csv("data/events/events2.csv", dtype=str)

    print(f"Numero eventi 1 : {len(events1)}")
    print(f"Numero eventi 2 : {len(events2)}")