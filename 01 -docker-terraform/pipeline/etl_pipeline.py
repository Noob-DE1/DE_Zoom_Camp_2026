import sys
import pandas as pd 
print("arguments",sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({"day":[1,2], "number_passengers" : [3,6]})
df["month"] = sys.argv[1]
print(df.head())

df.to_parquet(f"out_put{month}.parquet")

print(f"hellow pipeline month={month}")
