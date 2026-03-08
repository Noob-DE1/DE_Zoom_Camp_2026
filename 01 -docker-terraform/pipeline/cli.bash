-- For Running ingestion pipeline from command line use the following command:

# Run Docker Compose to start the PostgreSQL database container:

docker-compose up -d

# once the database container is up and running, you can execute the following command to run the ingestion pipeline:

# Build the Docker image for the ingestion pipeline

docker build -t taxi_ingest:v001 .

# Run the Docker container for the ingestion pipeline, connecting it to the same network as the PostgreSQL database container(Docker Compose creates a default network for the services defined in the docker-compose.yml file, and the containers can communicate with each other using their service names as hostnames):

docker run -it \
  --network=pipeline_default \
  taxi_ingest:v001 \
  --pg-user=root \
  --pg-password=root \
  --pg-host=pgdatabase \
  --pg-port=5432 \
  --db-name=ny_taxi \
  --target-table=yellow_taxi_trips_2021_2 \
  --year=2021 \
  --month=2 \
  --chunksize=100000

