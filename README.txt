Requirements: Python 3.10, Spark 3.5.4, Azure Blob Storage with Access Keys, MongoDB details, Jupyter Notebook with al libraries installed.

Follow step-by-step to run the code
Note: Make sure the environment is setup up correctly

1. Open the .ipynb file. Check the comments to see where to start the code run.
2. Make sure the .py file is saved.
3. Once the data is in the Azure Storage, run the .py file on command prompt using the command
-spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 "path" -#provide correct path of .py file.
4. Once the data is in MongoDB, run the code in .ipynb file.
5. Visualizations are followed with data loading.