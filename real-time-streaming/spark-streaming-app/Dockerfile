FROM bde2020/spark-base:3.1.1-hadoop3.2

# Copy the application code into the container
COPY spark-streaming-app.py /app/spark-streaming-app.py

# Set the command to run the Spark application
CMD ["spark-submit", "--master", "spark://spark-master:7077", "/app/spark-streaming-app.py"]
