## UserMetricsJob Documentation

### Overview
The `UserMetricsJob` program processes user event data and generates a Parquet dataset with aggregated metrics such as revenue, event count, and country rank. It demonstrates common Spark patterns, including:
- SparkSession configuration
- Reading CSV files with explicit schemas
- Filtering data with edge-case handling
- Using UDFs and built-in column expressions
- Performing joins with broadcast hints
- Ranking data using window functions
- Error handling and deterministic output ordering

### Input and Output
#### Inputs
1. `events.csv` with columns: `user_id`, `event_type`, `score`, `amount`, `ts` (timestamp)
2. `users.csv` with columns: `user_id`, `country`

#### Output
- Parquet dataset with columns: `country`, `user_id`, `revenue`, `event_count`, `score_bucket`, `country_rank`

### Execution Flow
1. **SparkSession Configuration**: The program initializes a SparkSession with Adaptive Query Execution (AQE) and shuffle partition settings.
2. **Input Reading**: Reads `events.csv` and `users.csv` into Spark DataFrames with explicit schemas.
3. **Data Transformation**:
   - Filters events by type (`click` or `purchase`) and timestamp range.
   - Buckets scores into categories (`high`, `medium`, `low`, `unknown`) using either a UDF or built-in expressions.
   - Aggregates user revenue and event counts.
   - Joins user data with event metrics using a broadcast join.
   - Ranks users by revenue within each country.
   - Orders the output deterministically for validation.
4. **Output Writing**: Writes the final DataFrame as a Parquet dataset.
5. **Error Handling**: Catches and logs exceptions, ensuring the Spark session is stopped.

### Detailed Logic Explanation
#### 1. **SparkSession Configuration**
```java
SparkSession spark = SparkSession.builder()
    .appName("UserMetricsJob")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate();
```
- Enables AQE to optimize query execution dynamically.
- Sets shuffle partitions to 8 for performance tuning.

#### 2. **Input Reading**
- Reads `events.csv` with a schema defining columns for `user_id`, `event_type`, `score`, `amount`, and `ts`.
- Reads `users.csv` with a schema defining columns for `user_id` and `country`.

#### 3. **Data Transformation**
##### a. Filtering Events
Filters events by type (`click` or `purchase`) and timestamp range:
```java
Column inWindow = col("ts").geq(to_timestamp(lit(minDateInclusive)))
    .and(col("ts").lt(to_timestamp(lit(maxDateExclusive))));

Dataset<Row> filtered = events
    .filter(col("event_type").isin("click", "purchase"))
    .filter(inWindow);
```

##### b. Score Bucketing
Uses either a UDF or built-in expressions to bucket scores:
```java
if (useUdfBucket) {
    sparkRegisterBucketUdf(filtered.sparkSession());
    filtered = filtered.withColumn("score_bucket", callUDF("bucketScore", col("score")));
} else {
    filtered = filtered.withColumn(
        "score_bucket",
        when(col("score").isNull(), lit("unknown"))
            .when(col("score").geq(lit(80)), lit("high"))
            .when(col("score").geq(lit(50)), lit("medium"))
            .otherwise(lit("low"))
    );
}
```

##### c. Aggregating Metrics
Aggregates user revenue and event counts:
```java
Dataset<Row> aggregated = filtered
    .groupBy("user_id", "score_bucket")
    .agg(
        sum("amount").as("revenue"),
        count("*").as("event_count")
    );
```

##### d. Joining User Data
Joins user data with event metrics using a broadcast join:
```java
Dataset<Row> joined = aggregated
    .join(broadcast(users), "user_id");
```

##### e. Ranking Users
Ranks users by revenue within each country:
```java
WindowSpec rankSpec = Window.partitionBy("country").orderBy(desc("revenue"));
Dataset<Row> ranked = joined.withColumn("country_rank", rank().over(rankSpec));
```

##### f. Ordering Output
Orders the output deterministically:
```java
return ranked.orderBy("country", "country_rank", "user_id");
```

### Visual Representations
#### Loop and Nested Loop Analysis
- **No explicit loops**: The program relies on Spark's distributed processing.

#### Flowchart
```plaintext
[Start] --> [Initialize SparkSession] --> [Read events.csv] --> [Read users.csv] -->
[Filter events] --> [Bucket scores] --> [Aggregate metrics] --> [Join user data] -->
[Rank users] --> [Order output] --> [Write Parquet] --> [End]
```

### Error Handling
The program handles exceptions during execution and logs errors:
```java
try {
    // Main execution logic
} catch (AnalysisException ae) {
    log.error("Spark analysis error: {}", ae.getMessage(), ae);
    throw new RuntimeException("Analysis exception during job run", ae);
} catch (Exception e) {
    log.error("Unexpected error: {}", e.getMessage(), e);
    throw new RuntimeException("Unhandled exception", e);
} finally {
    spark.stop();
}
```

### Conclusion
The `UserMetricsJob` program demonstrates efficient ETL processing using Apache Spark. Its modular design and use of Spark's features make it a robust solution for large-scale data processing.
