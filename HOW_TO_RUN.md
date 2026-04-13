# How to Run

**Prerequisites:** Docker and Docker Compose. Nothing else.

---

## Option 1 — Pull image from Docker Hub (recommended)

Direct Link to download docker-compose file from repository
```bash
https://raw.githubusercontent.com/AnirudhV213/DataSorter/main/docker-compose.yaml
```
No source code needed. Just download the docker-compose.yaml from the repo and run:
```bash
docker compose up
```

Compose pulls `apache/kafka:3.7.0` and `aavv4/datasorter-app:latest` from Docker Hub, starts Kafka, waits for it to be healthy, then runs the full pipeline automatically.

**Stop:**
```bash
docker compose down
```

---

## Option 2 — Build from source locally

Use this if you have cloned the repo and want to build the image yourself.

```bash
git clone https://github.com/AnirudhV16/DataSorter.git
cd DataSorter
docker compose up --build
```

The `--build` flag forces Docker to build the app image from the local source instead of pulling it from Docker Hub.

**Stop:**
```bash
docker compose down
```

---

## Wait for completion

Both options print a step-by-step summary when the pipeline finishes:

```
═══ Step 1: Generating 50000000 records → data.csv ═══
═══ Step 2: Producing data.csv → Kafka topic "source" ═══
═══ Step 3: Running sorters (id / name / continent) ═══
✓ Full pipeline complete. Total wall-clock: ~800s
```

---

## Verify correctness (optional)

On Linux/macOS:
```bash
./scripts/verify.sh
```
On Windows:
```bat
scripts\verify.bat
```

Prints the first 10 records from each output topic. Check that:
- **id** — ascending numeric order by the first column.
- **name** — ascending alphabetical order by the second column.
- **continent** — ascending alphabetical order by the fourth column.

---


## Local Development (no Docker)

**Prerequisites:** Go 1.26.1+, Kafka running on `localhost:9092`.

```bash
export KAFKA_BROKERS=localhost:9092
go run ./cmd/main.go
```

**CLI flags:**

| Flag | Default | Description |
|---|---|---|
| `-brokers` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `-csv` | `data.csv` | Path for the generated CSV file |
| `-count` | `50000000` | Number of records to generate |
| `-skip-gen` | `false` | Skip CSV generation — use an existing file |
| `-skip-produce` | `false` | Skip producing to Kafka — run sorters only |

**Run tests:**
```bash
go test ./...
```

---

## Topics

| Topic | Partitions | Contents |
|---|---|---|
| `source` | 3 | Raw records (round-robin) |
| `id` | 1 | Records sorted ascending by id |
| `name` | 1 | Records sorted ascending by name |
| `continent` | 1 | Records sorted ascending by continent |