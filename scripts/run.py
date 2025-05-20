#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

# Add here your team number teamx
team = "team5"

# location of your Hive database in HDFS
warehouse = "project/hive/warehouse"

spark = SparkSession.builder        .appName("{} - spark ML".format(team))        .master("yarn")        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")        .config("spark.sql.warehouse.dir", warehouse)        .config("spark.sql.avro.compression.codec", "snappy")        .enableHiveSupport()        .getOrCreate()

#We can also add
# .config("spark.sql.catalogImplementation","hive")\ 
# But this is the default configuration
# You can switch to Spark Catalog by setting "in-memory" for "spark.sql.catalogImplementation"


# In[2]:


spark.sql("SHOW DATABASES").show()
spark.sql("USE team5_projectdb").show()
spark.sql("SHOW TABLES").show()
# spark.sql("SELECT * FROM <db_name>.<table_name>").show()


# In[3]:


tables = [row['tableName'] for row in spark.sql("SHOW TABLES").select('tableName').collect()]
tables = [table for table in tables if "_results" not in table]

for table in tables:
    print(table)
    spark.sql(f"SHOW COLUMNS IN team5_projectdb.{table}").show()


# In[4]:


for table in tables:
    print(table)
    spark.sql(f"SELECT * FROM team5_projectdb.{table}").show(1)


# In[5]:


data = {}
for table in tables:
    data[table] = spark.read.format("avro").table(f"team5_projectdb.{table}")


# In[6]:


data.keys()


# In[7]:


from pyspark.sql.functions import col, count, when, isnull

# Calculate missing value percentages
total_count = data['locations_part'].count()

missing_stats = data['locations_part'].agg(
    (count(when(isnull(col("start_lat")), col("id"))) / total_count).alias("start_lat_missing"),
    (count(when(isnull(col("start_lng")), col("id"))) / total_count).alias("start_lng_missing"),
    (count(when(isnull(col("end_lat")), col("id"))) / total_count).alias("end_lat_missing"),
    (count(when(isnull(col("end_lng")), col("id"))) / total_count).alias("end_lng_missing")
)

missing_stats.show()


# From the table we can see that we always have starting coordinates of the accident and about 33.35% of the times we don't have ending coordinates. We will impute missing coordinates with starting coordinates, as it's most likely that those accidents are single point, i.e., accident did not involve movement.

# In[8]:


from pyspark.sql.functions import coalesce, col

# Impute missing end locations with start locations
data['locations_part'] = data['locations_part'].withColumn(
    "end_lat", coalesce(col("end_lat"), col("start_lat")))
    
data['locations_part'] = data['locations_part'].withColumn(
    "end_lng", coalesce(col("end_lng"), col("start_lng")))

# Add flag indicating imputation
data['locations_part'] = data['locations_part'].withColumn(
    "end_loc_imputed", 
    (col("end_lat") == col("start_lat")) & (col("end_lng") == col("start_lng")))


# In[9]:


data['locations_part'].show(5)


# In[10]:


from pyspark.sql.functions import from_unixtime, unix_timestamp, col

# Define the desired format (optional for from_unixtime, default is 'yyyy-MM-dd HH:mm:ss')
datetime_format = "yyyy-MM-dd HH:mm:ss"

data['accidents_part'] = data['accidents_part'].withColumn(
    "start_datetime",
    from_unixtime(unix_timestamp(col("start_time")), datetime_format)
).withColumn(
    "end_datetime",
    from_unixtime(unix_timestamp(col("end_time")), datetime_format)
)

data['weather_buck'] = data['weather_buck'].withColumn(
    "weather_datetime",
    from_unixtime(unix_timestamp(col("weather_timestamp")), datetime_format)
)


# In[11]:


from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second

# Extract components for accidents
data['accidents_part'] = data['accidents_part'].withColumn("start_year", year("start_datetime"))     .withColumn("start_month", month("start_datetime"))     .withColumn("start_day", dayofmonth("start_datetime"))     .withColumn("start_hour", hour("start_datetime"))     .withColumn("start_minute", minute("start_datetime"))     .withColumn("start_second", second("start_datetime"))
data['accidents_part'] = data['accidents_part'].withColumn("end_year", year("end_datetime"))     .withColumn("end_month", month("end_datetime"))     .withColumn("end_day", dayofmonth("end_datetime"))     .withColumn("end_hour", hour("end_datetime"))     .withColumn("end_minute", minute("end_datetime"))     .withColumn("end_second", second("end_datetime"))

# Extract components for weather
data['weather_buck'] = data['weather_buck'].withColumn("weather_year", year("weather_datetime"))     .withColumn("weather_month", month("weather_datetime"))     .withColumn("weather_day", dayofmonth("weather_datetime"))     .withColumn("weather_hour", hour("weather_datetime"))     .withColumn("weather_minute", minute("weather_datetime"))     .withColumn("weather_second", second("weather_datetime"))


# In[12]:


from pyspark.sql.functions import col, datediff, unix_timestamp, date_format, when, dayofweek

# Duration of accident in minutes
data['accidents_part'] = data['accidents_part'].withColumn(
    "duration_minutes",
    (unix_timestamp(col("end_time")) - unix_timestamp(col("start_time"))) / 60.0
)

# Day of week (1=Sunday, 7=Saturday)
data['accidents_part'] = data['accidents_part'].withColumn(
    "day_of_week",
    dayofweek(col("start_datetime"))  # Returns 1 (Sunday) to 7 (Saturday)
)

# Weekend flag (Saturday=7, Sunday=1)
data['accidents_part'] = data['accidents_part'].withColumn(
    "is_weekend",
    ((col("day_of_week") == 1) | (col("day_of_week") == 7)).cast("integer")
)


# Season (could be useful for weather patterns)
data['accidents_part'] = data['accidents_part'].withColumn(
    "season",
    when((col("start_month") >= 3) & (col("start_month") <= 5), "spring")
    .when((col("start_month") >= 6) & (col("start_month") <= 8), "summer")
    .when((col("start_month") >= 9) & (col("start_month") <= 11), "fall")
    .otherwise("winter")
)


# In[13]:


data


# In[14]:


from pyspark.sql.functions import col

# Join all tables
df = data['accidents_part']     .join(data['locations_part'], data['accidents_part'].location_id == data['locations_part'].id, 'left')     .join(data['weather_buck'], data['accidents_part'].weather_id == data['weather_buck'].id, 'left')     .join(data['twilight'], data['accidents_part'].twilight_id == data['twilight'].id, 'left')     .join(data['road_features'], data['accidents_part'].road_feat_id == data['road_features'].id, 'left')

# Drop redundant ID columns
df = df.drop('location_id', 'weather_id', 'twilight_id', 'road_feat_id')


# In[15]:


data['accidents_part'].select('id', 'description').show(10, truncate=False)


# As we can see, description is mainly giving us information about the location of the accidents - we already have that, so that information is not really useful and we can safely drop it.

# In[16]:


from pyspark.sql.functions import col

# First, let's select only the needed columns from each table before joining
accidents_selected = data['accidents_part'].select(
    [c for c in data['accidents_part'].columns]
)

locations_selected = data['locations_part'].select(
    'id', 'start_lat', 'start_lng', 'end_lat', 'end_lng',
    'street', 'city', 'county', 'state', 'zipcode', 
    'timezone'
)

weather_selected = data['weather_buck'].select(
    'id', 'airport_code', 'temperature_f', 'wind_chill_f', 'humidity_pct', 
    'pressure_in', 'visibility_mi', 'wind_direction', 'wind_speed_mph', 
    'precipitation_in', 'weather_condition', 'weather_year', 'weather_month', 
    'weather_day', 'weather_hour', 'weather_minute', 'weather_second'
)

twilight_selected = data['twilight'].select(
    [c for c in data['twilight'].columns]
)

road_features_selected = data['road_features'].select(
    [c for c in data['road_features'].columns]
)

# Now perform the join with explicit column selection
df = accidents_selected     .join(locations_selected, accidents_selected.location_id == locations_selected.id, 'left')     .join(weather_selected, accidents_selected.weather_id == weather_selected.id, 'left')     .join(twilight_selected, accidents_selected.twilight_id == twilight_selected.id, 'left')     .join(road_features_selected, accidents_selected.road_feat_id == road_features_selected.id, 'left')

# Drop the redundant ID columns from joined tables
df = df.drop(
    locations_selected.id, 
    weather_selected.id, 
    twilight_selected.id, 
    road_features_selected.id,
    'location_id', 
    'weather_id', 
    'twilight_id', 
    'road_feat_id'
)

# Also drop any other redundant columns that might have been carried over
redundant_cols = [
    'start_datetime', 'end_datetime',  # We have the decomposed time features
    'weather_datetime',                # Same as above
    'description'                      # Text field not useful for ML
]

df = df.drop(*[c for c in redundant_cols if c in df.columns])

# Verify the remaining columns
print("Final columns after joining:")
df.printSchema()


# In[17]:


from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import math

class CyclicalTimeTransformer(Transformer, HasInputCol, HasOutputCol, 
                            DefaultParamsReadable, DefaultParamsWritable):
    """
    A custom transformer that converts cyclical time features (like hour, month, etc.)
    into sin/cos components to preserve their cyclical nature.
    """
    
    period = Param(Params._dummy(), "period", "The period of the cyclical feature (e.g., 12 for months, 24 for hours)",
                  typeConverter=TypeConverters.toFloat)
    
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, period=None):
        """
        Initialize the transformer.
        
        :param inputCol: The name of the input column (time component to transform)
        :param outputCol: The base name for output columns (will append _sin and _cos)
        :param period: The period of the cyclical feature (e.g., 12 for months)
        """
        super(CyclicalTimeTransformer, self).__init__()
        self._setDefault(period=12.0)  # Default to monthly cycle
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
    
    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, period=None):
        """
        Set the params for this CyclicalTimeTransformer.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)
    
    def getPeriod(self):
        """
        Gets the value of period or its default value.
        """
        return self.getOrDefault(self.period)
    
    def _transform(self, dataset: DataFrame):
        """
        Transform the input dataset by adding sin/cos components of the cyclical feature.
        """
        input_col = self.getInputCol()
        output_col = self.getOutputCol()
        period = self.getPeriod()
        
        # Calculate the sin and cos components
        angle = 2 * math.pi * F.col(input_col) / period
        
        return dataset.withColumn(f"{output_col}_sin", F.sin(angle))                      .withColumn(f"{output_col}_cos", F.cos(angle))


# In[18]:


cols = ['start', 'end', 'weather']
cyclical_time_transformers = []
for col in cols:
    # Transform months (period=12)
    cyclical_time_transformers.append(
        CyclicalTimeTransformer(
            inputCol=f"{col}_month", 
            outputCol=f"{col}_month_encoded",
            period=12.0
        )
    )

    # Transform days (period=31)
    cyclical_time_transformers.append(
        CyclicalTimeTransformer(
            inputCol=f"{col}_day", 
            outputCol=f"{col}_day_encoded",
            period=31.0
        )
    )

    # Transform hours (period=24)
    cyclical_time_transformers.append(
        CyclicalTimeTransformer(
            inputCol=f"{col}_hour",
            outputCol=f"{col}_hour_encoded",
            period=24.0
        )
    )

    # Transform minutes (period=60)
    cyclical_time_transformers.append(
        CyclicalTimeTransformer(
            inputCol=f"{col}_minute",
            outputCol=f"{col}_minute_encoded",
            period=60.0
        )
    )

    # Transform seconds (period=60)
    cyclical_time_transformers.append(
        CyclicalTimeTransformer(
            inputCol=f"{col}_second",
            outputCol=f"{col}_second_encoded",
            period=60.0
        )
    )
len(cyclical_time_transformers) # Should be 3*5=15


# In[19]:


from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCols, HasOutputCols
import pyspark.sql.functions as F
import math

class GeodeticToECEFTransformer(Transformer, HasInputCols, HasOutputCols):
    """
    Converts geodetic coordinates (lat, lng, alt) to ECEF coordinates (x, y, z)
    WGS84 ellipsoid parameters used by default
    """
    
    @keyword_only
    def __init__(self, inputCols=None, outputCols=None):
        super(GeodeticToECEFTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
    
    @keyword_only
    def setParams(self, inputCols=None, outputCols=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)
    
    def _transform(self, dataset):
        # WGS84 parameters
        a = 6378137.0  # semi-major axis in meters
        f = 1/298.257223563  # flattening
        b = a * (1 - f)  # semi-minor axis
        
        lat_col, lng_col, alt_col = self.getInputCols()
        x_col, y_col, z_col = self.getOutputCols()
        
        # Convert to radians
        lat_rad = F.radians(F.col(lat_col))
        lng_rad = F.radians(F.col(lng_col))
        
        # Compute eccentricity
        e_sq = 2*f - f*f
        
        # Compute N (prime vertical radius of curvature)
        N = a / F.sqrt(1 - e_sq * F.sin(lat_rad)**2)
        
        # Compute ECEF coordinates
        x = (N + F.col(alt_col)) * F.cos(lat_rad) * F.cos(lng_rad)
        y = (N + F.col(alt_col)) * F.cos(lat_rad) * F.sin(lng_rad)
        z = ((1 - e_sq) * N + F.col(alt_col)) * F.sin(lat_rad)
        
        return dataset.withColumn(x_col, x)                      .withColumn(y_col, y)                      .withColumn(z_col, z)


# In[20]:


geodetic_transformers = []
for pref in ['start', 'end']:
    if f'{pref}_alt' not in df.columns:
        df = df.withColumn(f'{pref}_alt', F.lit(0.0))
    geodetic_transformers.append(
        GeodeticToECEFTransformer(
            inputCols=[f'{pref}_lat', f'{pref}_lng', f'{pref}_alt'],
            outputCols=[f'{pref}_x', f'{pref}_y', f'{pref}_z']
        )
    )

len(geodetic_transformers) # Should be 2


# In[21]:


from pyspark.ml.feature import StringIndexer, OneHotEncoder, FeatureHasher
from pyspark.sql.functions import concat_ws

# df = df.withColumn("address", concat_ws(", ", "street", "city", "county", "state", "zipcode"))

address_hasher = FeatureHasher(
    inputCols=["street", "city", "county", "state", "zipcode"],
    outputCol="address_hashed",
    numFeatures=64  # Reduced from default 2^18 to save memory
)
zipcode_indexer = StringIndexer(inputCol="zipcode", outputCol="zipcode_encoded", handleInvalid="keep")
timezone_indexer = StringIndexer(inputCol="timezone", outputCol="timezone_encoded", handleInvalid="keep")


# In[22]:


for transformer in cyclical_time_transformers+geodetic_transformers:
    df = transformer.transform(df)
df = address_hasher.transform(df)
df = zipcode_indexer.fit(df).transform(df)
df = timezone_indexer.fit(df).transform(df)


# In[23]:


df.head(2)


# In[24]:


from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

# Select numerical features to scale
numerical_cols = [
    'distance_mi', 'duration_minutes', 'temperature_f', 'wind_chill_f',
    'humidity_pct', 'pressure_in', 'visibility_mi', 'wind_speed_mph',
    'precipitation_in', 'start_x', 'start_y', 'start_z', 'end_x', 'end_y', 'end_z'
]

# Convert string columns to double
for col_name in numerical_cols:
    if col_name in df.columns and str(df.schema[col_name].dataType) == 'StringType':
        df = df.withColumn(col_name, F.col(col_name).cast('double'))

# Remove any null values (or impute)
df = df.na.fill(0, subset=numerical_cols)

# Assemble and scale features
assembler = VectorAssembler(inputCols=numerical_cols, outputCol="numerical_features")
scaler = StandardScaler(inputCol="numerical_features", outputCol="scaled_features")

# Create pipeline
pipeline = Pipeline(stages=[assembler, scaler])
scaler_model = pipeline.fit(df)
df = scaler_model.transform(df)


# In[26]:


from pyspark.ml.feature import StringIndexer, OneHotEncoder

# List of categorical columns
# source add later
categorical_cols = [
     'weather_condition', 'sunrise_sunset', 'civil_twilight',
    'nautical_twilight', 'astronomical_twilight', 'season','day_of_week'
]

# Add boolean road features
road_bool_cols = [c for c in df.columns if c in [
    'amenity', 'bump', 'crossing', 'give_way', 'junction', 'no_exit',
    'railway', 'roundabout', 'station', 'stop', 'traffic_calming', 'traffic_signal', 'is_weekend'
]]

categorical_cols.extend(road_bool_cols)

for col in road_bool_cols:
    df = df.withColumn(col, F.col(col).cast("string"))

# String index and one-hot encode
indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep") for c in categorical_cols]
encoder = OneHotEncoder(
    inputCols=[f"{c}_index" for c in categorical_cols],
    outputCols=[f"{c}_encoded" for c in categorical_cols]
)

# Create pipeline
pipeline = Pipeline(stages=indexers + [encoder])
encoder_model = pipeline.fit(df)
df = encoder_model.transform(df)


# In[27]:


from pyspark.ml.feature import VectorAssembler

# Get all feature columns
feature_cols = [
    'scaled_features',
    *[f"{c}_encoded" for c in categorical_cols],
    *[c for c in df.columns if '_encoded_sin' in c or '_encoded_cos' in c]
]

# Assemble final feature vector
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df = assembler.transform(df)


# In[28]:


from pyspark.ml.feature import VarianceThresholdSelector

selector = VarianceThresholdSelector(
    varianceThreshold=0.01,
    featuresCol="features",
    outputCol="selected_features"
)

selector_model = selector.fit(df)
df = selector_model.transform(df)

# Show which features were selected
selected_features = selector_model.selectedFeatures
print(f"Selected {len(selected_features)} out of {len(feature_cols)} features")


# In[29]:


df.head(2)


# In[30]:


# df is the DataFrame after VarianceThresholdSelector, containing:
# 'id', 'severity', 'duration_minutes', 'selected_features', and other original columns.

# --- Prepare data for CLASSIFICATION (Severity) ---
df_cls = df.select(
    F.col("id"),
    F.col("severity").alias("label"),  # 'severity' is the target
    F.col("selected_features").alias("features")
)
# .na.drop(subset=["label", "features"]) # Important: drop rows where label or features are null

# --- Prepare data for REGRESSION (Duration) ---
df_reg = df.select(
    F.col("id"),
    F.col("duration_minutes").alias("label"), # 'duration_minutes' is the target
    F.col("selected_features").alias("features")
)
# .na.drop(subset=["label", "features"]) # Important: drop rows where label or features are null

# Now, you will split df_classification_input for your classification models
# And you will split df_regression_input for your regression models.


# In[31]:


#  split the data into 60% training and 40% test (it is not stratified)
(train_data_cls, test_data_cls) = df_cls.randomSplit([0.6, 0.4], seed = 10)
(train_data_reg, test_data_reg) = df_reg.randomSplit([0.6, 0.4], seed = 10)


# In[32]:


# A function to run commands

# save to json file
import os
def run(command):
    return os.popen(command).read()

def saveDF(dataframe,name):
    dataframe.select("id", "label", "features")    .coalesce(1)    .write    .mode("overwrite")    .format("json")    .save(f"project/data/{name}")
    
    run(f"hdfs dfs -cat project/data/{name}/*.json > data/{name}.json")
    
saveDF(train_data_cls,'train_data_cls')

saveDF(test_data_cls,'test_data_cls')

saveDF(train_data_reg,'train_data_reg')

saveDF(test_data_reg,'test_data_reg')


# Imoprt libs

# In[34]:


from pyspark.sql.functions import col
import os
import numpy as np
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.regression import LinearRegression, GBTRegressor


# Helper function to save model and predictions

# In[35]:


def save_model_and_predictions(model, model_name_prefix, task_label, test_data_df, base_path="project"):
    model_full_name = f"{model_name_prefix}_{task_label}"
    
    # Save model
    model_path_hdfs = f"{base_path}/models/{model_full_name}"
    model_path_local_repo = f"models/{model_full_name}" # Assuming 'models' folder in repo root
    
    print(f"Saving model {model_full_name} to HDFS: {model_path_hdfs}")
    model.write().overwrite().save(model_path_hdfs)
    
    # Ensure local directory exists
    os.makedirs(model_path_local_repo, exist_ok=True)
    run(f"hdfs dfs -get -f {model_path_hdfs}/* {model_path_local_repo}/") # Copy contents
    # For some models, -get needs the directory, not wildcard, and it copies the dir itself.
    # If the above fails, try: run(f"hdfs dfs -get -f {model_path_hdfs} models/") to get the folder into local 'models/'
    print(f"Model {model_full_name} copied to local repository: {model_path_local_repo}")

    # Predict
    print(f"Making predictions with {model_full_name} on test data...")
    predictions_df = model.transform(test_data_df)

    # Save predictions
    predictions_path_hdfs = f"{base_path}/output/{model_full_name}_predictions" # Full path for HDFS save
    predictions_path_local_csv_repo = f"output/{model_full_name}_predictions.csv" # Path for final CSV in repo

    print(f"Saving predictions for {model_full_name} to HDFS: {predictions_path_hdfs}")
    predictions_df.select("label", "prediction")         .coalesce(1)         .write         .mode("overwrite")         .format("csv")         .option("sep", ",")         .option("header", "true")         .save(predictions_path_hdfs) # Saves as a directory with part-files

    # Ensure local output directory exists
    os.makedirs(os.path.dirname(predictions_path_local_csv_repo), exist_ok=True)
    run(f"hdfs dfs -cat {predictions_path_hdfs}/part*.csv > {predictions_path_local_csv_repo}")
    print(f"Predictions for {model_full_name} copied to local CSV: {predictions_path_local_csv_repo}")
    
    return predictions_df


# TASK 1: SEVERITY PREDICTION (CLASSIFICATION)

# In[36]:


train_data_cls.cache()
test_data_cls.cache()

# Evaluators for Classification
cls_evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
cls_evaluator_accuracy = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")


#  Model 1.1: Logistic Regression for Classification

# In[37]:


print("\n--- Training Classification Model 1: Logistic Regression ---")
lr_cls = LogisticRegression(featuresCol="features", labelCol="label")

lr_cls_param_grid = ParamGridBuilder()     .addGrid(lr_cls.regParam, [0.01, 0.1])     .addGrid(lr_cls.elasticNetParam, [0.0, 0.8])     .build()

lr_cls_cv = CrossValidator(estimator=lr_cls,
                           estimatorParamMaps=lr_cls_param_grid,
                           evaluator=cls_evaluator_f1, # Optimize for F1-score
                           numFolds=3, # Use 2 for faster run on large data if needed
                           parallelism=2)


# In[38]:


print("Fitting Logistic Regression (Classification) CV model...")
lr_cls_cv_model = lr_cls_cv.fit(train_data_cls)
best_lr_cls_model = lr_cls_cv_model.bestModel
print("Best Logistic Regression (Classification) Hyperparameters:")
print(best_lr_cls_model.extractParamMap())


# In[39]:


lr_cls_predictions = save_model_and_predictions(best_lr_cls_model, "LogisticRegression", "cls", test_data_cls)

lr_cls_f1 = cls_evaluator_f1.evaluate(lr_cls_predictions)
lr_cls_accuracy = cls_evaluator_accuracy.evaluate(lr_cls_predictions)
print(f"Logistic Regression (Classification) - F1-Score on Test Data: {lr_cls_f1}")
print(f"Logistic Regression (Classification) - Accuracy on Test Data: {lr_cls_accuracy}")


# Model 1.2: Random Forest Classifier

# In[40]:


rf_cls = RandomForestClassifier(featuresCol="features", labelCol="label", seed=42)

rf_cls_param_grid = ParamGridBuilder()     .addGrid(rf_cls.numTrees, [10, 20])     .addGrid(rf_cls.maxDepth, [5, 10])     .build()

rf_cls_cv = CrossValidator(estimator=rf_cls,
                           estimatorParamMaps=rf_cls_param_grid,
                           evaluator=cls_evaluator_f1, # Optimize for F1-score
                           numFolds=3, # Use 2 for faster run
                           parallelism=2)


# Fitting Random Forest (Classification) CV model

# In[41]:


print("...")
rf_cls_cv_model = rf_cls_cv.fit(train_data_cls)
best_rf_cls_model = rf_cls_cv_model.bestModel
print("Best Random Forest (Classification) Hyperparameters:")
print(best_rf_cls_model.extractParamMap())

rf_cls_predictions = save_model_and_predictions(best_rf_cls_model, "RandomForestClassifier", "cls", test_data_cls)

rf_cls_f1 = cls_evaluator_f1.evaluate(rf_cls_predictions)
rf_cls_accuracy = cls_evaluator_accuracy.evaluate(rf_cls_predictions)
print(f"Random Forest (Classification) - F1-Score on Test Data: {rf_cls_f1}")
print(f"Random Forest (Classification) - Accuracy on Test Data: {rf_cls_accuracy}")


# Compare Classification Models

# In[42]:


cls_models_summary_data = [
    (str(best_lr_cls_model), lr_cls_f1, lr_cls_accuracy),
    (str(best_rf_cls_model), rf_cls_f1, rf_cls_accuracy)
]
cls_summary_df = spark.createDataFrame(cls_models_summary_data, ["model", "F1_Score", "Accuracy"])
print("\nClassification Models Comparison:")
cls_summary_df.show(truncate=False)

cls_summary_path_hdfs = "project/output/classification_evaluation" # Directory for CSV parts
cls_summary_path_local_csv = "output/classification_evaluation.csv"
cls_summary_df.coalesce(1)     .write.mode("overwrite").format("csv").option("sep", ",").option("header", "true")     .save(cls_summary_path_hdfs)
os.makedirs(os.path.dirname(cls_summary_path_local_csv), exist_ok=True)
run(f"hdfs dfs -cat {cls_summary_path_hdfs}/part*.csv > {cls_summary_path_local_csv}")
print(f"Classification evaluation summary saved to HDFS dir: {cls_summary_path_hdfs} and concatenated to local: {cls_summary_path_local_csv}")

train_data_cls.unpersist()
test_data_cls.unpersist()


# TASK 2: DURATION PREDICTION (REGRESSION)

# In[43]:


# Evaluators for Regression
reg_evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
reg_evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")


# In[44]:


# Model 2.1: Linear Regression for Regression
lr_reg = LinearRegression(featuresCol="features", labelCol="label")

lr_reg_param_grid = ParamGridBuilder()     .addGrid(lr_reg.regParam, [0.01, 0.1])     .addGrid(lr_reg.elasticNetParam, [0.0, 0.8])     .addGrid(lr_reg.aggregationDepth, [2, 3])     .build()

lr_reg_cv = CrossValidator(estimator=lr_reg,
                           estimatorParamMaps=lr_reg_param_grid,
                           evaluator=reg_evaluator_rmse, # Optimize for RMSE
                           numFolds=3, # Use 2 for faster run
                           parallelism=2)


# In[45]:


print("Fitting Linear Regression (Regression) CV model...")
lr_reg_cv_model = lr_reg_cv.fit(train_data_reg)
best_lr_reg_model = lr_reg_cv_model.bestModel
print("Best Linear Regression (Regression) Hyperparameters:")
print(best_lr_reg_model.extractParamMap())

lr_reg_predictions = save_model_and_predictions(best_lr_reg_model, "LinearRegression", "reg", test_data_reg)

lr_reg_rmse = reg_evaluator_rmse.evaluate(lr_reg_predictions)
lr_reg_r2 = reg_evaluator_r2.evaluate(lr_reg_predictions)
print(f"Linear Regression (Regression) - RMSE on Test Data: {lr_reg_rmse}")
print(f"Linear Regression (Regression) - R2 on Test Data: {lr_reg_r2}")


#  Model 2.2: Gradient-Boosted Tree Regressor

# In[46]:


gbt_reg = GBTRegressor(featuresCol="features", labelCol="label", seed=42)

gbt_reg_param_grid = ParamGridBuilder()     .addGrid(gbt_reg.maxIter, [10, 20])     .addGrid(gbt_reg.maxDepth, [3, 5])     .build()

gbt_reg_cv = CrossValidator(estimator=gbt_reg,
                            estimatorParamMaps=gbt_reg_param_grid,
                            evaluator=reg_evaluator_rmse, # Optimize for RMSE
                            numFolds=3, # Use 2 for faster run
                            parallelism=2)

print("Fitting GBT Regressor CV model...")
gbt_reg_cv_model = gbt_reg_cv.fit(train_data_reg)
best_gbt_reg_model = gbt_reg_cv_model.bestModel
print("Best GBT Regressor Hyperparameters:")
print(best_gbt_reg_model.extractParamMap())

gbt_reg_predictions = save_model_and_predictions(best_gbt_reg_model, "GBTRegressor", "reg", test_data_reg)


# In[47]:


gbt_reg_rmse = reg_evaluator_rmse.evaluate(gbt_reg_predictions)
gbt_reg_r2 = reg_evaluator_r2.evaluate(gbt_reg_predictions)
print(f"GBT Regressor - RMSE on Test Data: {gbt_reg_rmse}")
print(f"GBT Regressor - R2 on Test Data: {gbt_reg_r2}")


# Compare Regression Models

# In[48]:



reg_models_summary_data = [
    (str(best_lr_reg_model), lr_reg_rmse, lr_reg_r2),
    (str(best_gbt_reg_model), gbt_reg_rmse, gbt_reg_r2)
]
reg_summary_df = spark.createDataFrame(reg_models_summary_data, ["model", "RMSE", "R2"])
print("\nRegression Models Comparison:")
reg_summary_df.show(truncate=False)


# In[49]:


reg_summary_path_hdfs = "project/output/regression_evaluation" # Directory for CSV parts
reg_summary_path_local_csv = "output/regression_evaluation.csv"
reg_summary_df.coalesce(1)     .write.mode("overwrite").format("csv").option("sep", ",").option("header", "true")     .save(reg_summary_path_hdfs)
os.makedirs(os.path.dirname(reg_summary_path_local_csv), exist_ok=True)
run(f"hdfs dfs -cat {reg_summary_path_hdfs}/part*.csv > {reg_summary_path_local_csv}")
print(f"Regression evaluation summary saved to HDFS dir: {reg_summary_path_hdfs} and concatenated to local: {reg_summary_path_local_csv}")

train_data_reg.unpersist()
test_data_reg.unpersist()

print("\n--- All tasks completed. Stopping Spark session. ---")
spark.stop()

