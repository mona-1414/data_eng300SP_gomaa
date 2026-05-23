from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import subprocess

spark = (
    SparkSession.builder
    .appName("homework 3")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("Spark started successfully!")
print(f"Spark version: {spark.version}")

yellow_path = "/home/ec2-user/data/nyctlc/yellow_tripdata_2026-01.parquet"
green_path  = "/home/ec2-user/data/nyctlc/green_tripdata_2026-01.parquet"

yellow_raw = spark.read.parquet(yellow_path)
green_raw  = spark.read.parquet(green_path)

# print("\n Yellow Taxi Schema: \n")
#yellow_raw.printSchema()

# print("\n Green Taxi Schema: \n")
#green_raw.printSchema()

# print("\n Yellow Taxi Sample: \n")
#yellow_raw.show(5, truncate=False)

# print("\n Green Taxi Sample: \n")
#green_raw.show(5, truncate=False)

yellow_std = yellow_raw \
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
    .withColumn("taxi_type", F.lit("yellow"))

green_std = green_raw \
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
    .withColumn("taxi_type", F.lit("green"))

common_cols = [
    "taxi_type",
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "cbd_congestion_fee"
]

yellow_std = yellow_std.select(common_cols)
green_std  = green_std.select(common_cols)

print("\n Standardized Yellow Schema: \n")
yellow_std.printSchema()

print("\n Standardized Green Schema: \n")
green_std.printSchema()

def clean_taxi_data(df):
    return df.filter(
        F.col("pickup_datetime").isNotNull() &
        F.col("dropoff_datetime").isNotNull() &
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") >= 0) &
        (F.col("total_amount") >= 0) &
        (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
        ((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) <= 86400)
    )

yellow_clean = clean_taxi_data(yellow_std)
green_clean  = clean_taxi_data(green_std)

print(f"Yellow: {yellow_std.count()} -> {yellow_clean.count()} rows ({yellow_std.count() - yellow_clean.count()} removed)")
print(f"Green:  {green_std.count()} -> {green_clean.count()} rows ({green_std.count() - green_clean.count()} removed)")


combined_df = yellow_clean.unionByName(green_clean)
print(f"Total rows: {combined_df.count()}")
combined_df.printSchema()

# questions 

q1 = combined_df.groupBy("taxi_type").count().orderBy("count", ascending=False).collect()
print("Q1: Which taxi type had more trips?")
print(f"  Yellow: {q1[0]['count']:,}, Green: {q1[1]['count']:,}")

q2 = combined_df.groupBy("taxi_type").agg(F.round(F.avg("fare_amount"), 2).alias("avg_fare")).collect()
print("Q2: Average fare by taxi type")
print(f"  Yellow: ${[r for r in q2 if r['taxi_type']=='yellow'][0]['avg_fare']}, Green: ${[r for r in q2 if r['taxi_type']=='green'][0]['avg_fare']}")

q3 = combined_df.groupBy("taxi_type").agg(F.round(F.avg("trip_distance"), 2).alias("avg_dist")).collect()
print("Q3: Average trip distance by taxi type")
print(f"  Yellow: {[r for r in q3 if r['taxi_type']=='yellow'][0]['avg_dist']} miles, Green: {[r for r in q3 if r['taxi_type']=='green'][0]['avg_dist']} miles")

q4 = combined_df.withColumn("hour", F.hour("pickup_datetime")).groupBy("hour").count().orderBy("count", ascending=False).first()
print("Q4: Hour of day with most pickups")
print(f"  Hour {q4['hour']} (6 PM) with {q4['count']:,} pickups")

total = combined_df.count()
under_2 = combined_df.filter(F.col("trip_distance") < 2).count()
print("Q5: Percentage of trips under 2 miles")
print(f"  {round(under_2 / total * 100, 2)}%")


feature_cols = [
    "trip_distance",
    "passenger_count",
    "PULocationID",
    "DOLocationID",
    "RatecodeID",
    "congestion_surcharge",
    "cbd_congestion_fee",
    "improvement_surcharge",
    "mta_tax"
]

model_df = combined_df.select(feature_cols + ["fare_amount"]).dropna(subset=feature_cols)

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
model_df = assembler.transform(model_df).select("features", "fare_amount")

train_df, test_df = model_df.randomSplit([0.8, 0.2], seed=42)

lr = LinearRegression(featuresCol="features", labelCol="fare_amount")
lr_model = lr.fit(train_df)

train_preds = lr_model.transform(train_df)
test_preds  = lr_model.transform(test_df)

evaluator = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction", metricName="rmse")
train_rmse = evaluator.evaluate(train_preds)
test_rmse  = evaluator.evaluate(test_preds)

print("Linear Regression Model for Fare Amount Prediction:")
print(f"Training RMSE: {train_rmse}")
print(f" Testing RMSE: {test_rmse}")

print("\n  Predictor Coefficients:")
for name, coef in zip(feature_cols, lr_model.coefficients):
    print(f"    {name}: {coef}")

sample = test_preds.select("fare_amount", "prediction").limit(2000).toPandas()

plt.figure(figsize=(8, 6))
plt.scatter(sample["fare_amount"], sample["prediction"], alpha=0.3, s=10, color="steelblue")
plt.plot([0, 100], [0, 100], color="red", linestyle="--", label="Perfect Prediction")
plt.xlabel("Actual Fare Amount in USD")
plt.ylabel("Predicted Fare Amount in USD")
plt.title("Predicted vs Actual Fare Amount (Linear Regression)")
plt.legend()
plt.tight_layout()
plt.savefig("/home/ec2-user/fare_prediction.png", dpi=150)
plt.close()


subprocess.run([
    "aws", "s3", "cp",
    "/home/ec2-user/fare_prediction.png",
    "s3://gomaa-hw3/fare_prediction.png"
])

trips_by_type = combined_df.groupBy("taxi_type").count().orderBy("count", ascending=False)
avg_fare_by_type = combined_df.groupBy("taxi_type").agg(F.round(F.avg("fare_amount"), 2).alias("avg_fare"))
pickups_by_hour = combined_df.withColumn("hour", F.hour("pickup_datetime")).groupBy("hour").count().orderBy("hour")


output_local = "/home/ec2-user/results"
output_s3 = "s3://gomaa-hw3/results"

import os
os.makedirs(output_local, exist_ok=True)

trips_by_type.write.mode("overwrite").parquet(f"{output_local}/trips_by_type")
avg_fare_by_type.write.mode("overwrite").parquet(f"{output_local}/avg_fare_by_type")
pickups_by_hour.write.mode("overwrite").parquet(f"{output_local}/pickups_by_hour")

subprocess.run(["aws", "s3", "cp", f"{output_local}/trips_by_type", f"{output_s3}/trips_by_type", "--recursive"])
subprocess.run(["aws", "s3", "cp", f"{output_local}/avg_fare_by_type", f"{output_s3}/avg_fare_by_type", "--recursive"])
subprocess.run(["aws", "s3", "cp", f"{output_local}/pickups_by_hour", f"{output_s3}/pickups_by_hour", "--recursive"])
