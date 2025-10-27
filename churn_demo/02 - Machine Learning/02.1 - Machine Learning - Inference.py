# Databricks notebook source
# MAGIC %md
# MAGIC # Churn Prediction Inference - Batch or serverless real-time
# MAGIC
# MAGIC
# MAGIC After running AutoML we saved our best model our MLflow registry.
# MAGIC
# MAGIC All we need to do now is use this model to run Inferences. A simple solution is to share the model name to our Data Engineering team and they'll be able to call this model within the pipeline they maintained.
# MAGIC
# MAGIC This can be done as part of a DLT pipeline or a Workflow in a separate job.
# MAGIC Here is an example to show you how MLflow can be directly used to retrieve the model and run inferences.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##Deploying the model for batch inferences
# MAGIC
# MAGIC <img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" />
# MAGIC
# MAGIC Now that our model is available in the Registry, we can load it to compute our inferences and save them in a table to start building dashboards.
# MAGIC
# MAGIC We will use MLFlow function to load a pyspark UDF and distribute our inference in the entire cluster. If the data is small, we can also load the model with plain python and use a pandas Dataframe.
# MAGIC
# MAGIC If you don't know how to start, Databricks can generate a batch inference notebook in just one click from the model registry: Open MLFlow model registry and click the "User model for inference" button!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 5/ Enriching the gold data with a ML model
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-4.png"/>
# MAGIC </div>
# MAGIC
# MAGIC Our Data scientist team has build a churn prediction model using Auto ML and saved it into Databricks Model registry. 
# MAGIC
# MAGIC One of the key value of the Lakehouse is that we can easily load this model and predict our churn right into our pipeline. 
# MAGIC
# MAGIC Note that we don't have to worry about the model framework (sklearn or other), MLflow abstracts all that for us.

# COMMAND ----------

# DBTITLE 1,Install necessary libraries
# MAGIC %pip install --quiet databricks-feature-engineering databricks-automl-runtime holidays scikit-learn==1.0.2 category_encoders
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Run setup
# MAGIC %run ../includes/SetupLab $CATALOG="main"

# COMMAND ----------

# DBTITLE 1,Use commands for catalog and db
spark.sql("use catalog " + catalogName)
spark.sql("use database " + databaseName)

# COMMAND ----------

# DBTITLE 1,Verify values
print("Catalog name:  " + catalogName)
print("Database name: " + databaseName)
print("User name:     " + userName)
print("Model name:    " + modelName)

# COMMAND ----------

# DBTITLE 1,Download our ML model from UC registry
from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository

requirements_path = ModelsArtifactRepository(f"models:/{catalogName}.{databaseName}.{modelName}@production").download_artifacts(artifact_path="requirements.txt") # download model from remote registry

# COMMAND ----------

# DBTITLE 1,Install model libraries
# MAGIC %pip install --quiet -r $requirements_path
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Re-setup (previous command cleared everything)
# MAGIC %run ../includes/SetupLab

# COMMAND ----------

# DBTITLE 1,Use commands for catalog and db
spark.sql("use catalog " + catalogName)
spark.sql("use database " + databaseName)

# COMMAND ----------

# DBTITLE 1,Register ML model as UDF
import mlflow

production_model = mlflow.pyfunc.spark_udf(spark, model_uri=f"models:/{catalogName}.{databaseName}.{modelName}@production")

# COMMAND ----------

# DBTITLE 1,Create prediction column
inference_df = spark.read.table(f"churn_features")

preds_df = inference_df.withColumn('churn_prediction', production_model(*production_model.metadata.get_input_schema().input_names()))
display(preds_df)

# COMMAND ----------

# DBTITLE 1,Save as a new table
preds_df.write.mode("overwrite").saveAsTable("v_churn_prediction")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next up
# MAGIC [Explore the data with SQL and create visualisations]($./03 - BI and Data Warehousing)
