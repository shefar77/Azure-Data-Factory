print("Hello from the deployed notebook!")

data = [("North", 100), ("South", 150), ("East", 120), ("West", 90)]
df = spark.createDataFrame(data, ["region", "sales"])
df.show()
print("Total rows:", df.count())