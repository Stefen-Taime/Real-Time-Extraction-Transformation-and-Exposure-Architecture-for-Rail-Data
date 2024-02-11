# Startup Guide for Real-Time Data Architecture for Rail Data

This guide provides the necessary steps to set up and run the real-time data architecture for rail data.

## Clone the Project

1. Open a terminal.
2. Run the following command to clone the repository:
   ```
   git clone https://github.com/Stefen-Taime/Real-Time-Extraction-Transformation-and-Exposure-Architecture-for-Rail-Data.git
   ```
3. Navigate to the cloned project:
   ```
   cd Real-Time-Extraction-Transformation-and-Exposure-Architecture-for-Rail-Data
   ```

## Launch Services with Docker Compose

In the project's root directory, launch the services using Docker Compose:

```
docker-compose up --build -d
```

## Service Configuration

After starting the services, configure and start the different project components in four separate terminals.

### Terminal 1: Flink SQL Client

1. Obtain the Docker container ID running Apache Flink with `docker ps`.
2. Execute the Flink SQL client:
   ```
   docker exec -it <container_id> /opt/flink/bin/sql-client.sh
   ```
3. Execute the SQL queries located in `jobs/job.sql`.

### Terminal 2: API Microservice 1

1. Navigate to the first API microservice:
   ```
   cd apiMicroservice1
   ```
2. Launch the microservice:
   ```
   python main.py
   ```

### Terminal 3: API Microservice 2

1. Navigate to the second API microservice:
   ```
   cd apiMicroservice2
   ```
2. Launch the microservice:
   ```
   python app.py
   ```

### Terminal 4: Next.js Application

1. Navigate to the metro-status service:
   ```
   cd metro-status
   ```
2. Clean the project and launch the application in development mode:
   ```
   npm run clean
   npm run dev
   ```

## Accessing the Services

- **Flink Dashboard**: Typically at `http://localhost:8085`.
- **API Microservices**: `http://localhost:<port>`.
- **Next.js Application**: Typically at `http://localhost:3000`.

