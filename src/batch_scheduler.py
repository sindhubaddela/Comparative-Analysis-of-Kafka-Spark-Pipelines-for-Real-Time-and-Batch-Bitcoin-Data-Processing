# src/batch_scheduler.py
import time
import os

# Get all .jar files in /app (current working dir in container)
jars_dir = "/app"
spark_jars_list = [os.path.join(jars_dir, f) for f in os.listdir(jars_dir) if f.endswith(".jar")]

if not spark_jars_list:
    print("Error: No JAR files found in /app directory.")
    exit(1)

spark_jars = ",".join(spark_jars_list)
print(f"Using SPARK_JARS: {spark_jars}")

# Create the log directory if it doesn't exist
log_dir = "/app/data"
os.makedirs(log_dir, exist_ok=True)
batch_log_file = os.path.join(log_dir, "batch_log.txt")

while True:
    print("Running batch job...")
    # Construct the spark-submit command
    command = f"spark-submit --master local[*] --jars {spark_jars} /app/src/batch_processor.py"
    print(f"Executing: {command}")

    start_time = time.time()
    exit_code = os.system(command)
    end_time = time.time()
    duration = end_time - start_time

    if exit_code == 0:
        print(f"Batch job completed successfully in {duration:.2f} seconds.")
        # Append execution time to the log file for the dashboard
        try:
            with open(batch_log_file, "a") as f:
                f.write(f"{duration:.2f}\n")
        except Exception as e:
            print(f"Error writing to batch log file: {e}")
    else:
        print(f"Batch job failed with exit code {exit_code} after {duration:.2f} seconds.")

    print("Waiting for 10 minutes...\n")
    time.sleep(300)  # 600 seconds = 10 minutes
