# How to Run

---

## Quick Start with Docker Compose (build from source)

Use this if you have the source code and want to build and run the pipeline locally.

**Prerequisites:** Docker and Docker Compose. No Go or Kafka installation required.

**1. Get the project files.** Clone the repo — you need `docker-compose.yaml` and the full source tree.

**2. Clean any previous run** (required before each run):
```bash
docker-compose down
```

**3. Build and start the pipeline:**
```bash
docker-compose up --build
```

This will:
- Start Kafka in KRaft mode (no Zookeeper) with a pinned 512 MB JVM heap.
- Wait for Kafka to be healthy, then start the `app` container.
- Run the full pipeline inside `app`: generate 50M CSV records → produce to `source` topic → sort in parallel by `id`, `name`, and `continent` → write to three output topics.

**4. Wait for completion.** The console prints a step-by-step wall-clock summary when done:

```
═══ Step 1: Generating 50000000 records → data.csv ═══
═══ Step 2: Producing data.csv → Kafka topic "source" ═══
═══ Step 3: Running sorters (id / name / continent) ═══
✓ Full pipeline complete. Total wall-clock: ~800s
```

**5. Verify correctness** (optional). On Windows:
```bat
scripts\verify.bat
```
On Linux/macOS:
```bash
./scripts/verify.sh
```
This prints the first 10 records from each output topic (`id`, `name`, `continent`). Check that:
- **id**: records are in ascending numeric order by the first column.
- **name**: records are in ascending alphabetical order by the second column.
- **continent**: records are grouped in ascending alphabetical order by the fourth column.

**6. Stop and remove containers:**
```bash
docker-compose down
```

---

## Local Development (optional)

If you want to run the pipeline directly on your machine without Docker:

### Prerequisites
- Go 1.26.1+
- Kafka running on `localhost:9092`

### Run the pipeline
```bash
export KAFKA_BROKERS=localhost:9092
go run ./cmd/main.go
```

### CLI flags

| Flag | Default | Description |
|---|---|---|
| `-brokers` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `-csv` | `data.csv` | Path for the generated CSV file |
| `-count` | `50000000` | Number of records to generate |
| `-skip-gen` | `false` | Skip CSV generation — use an existing file |
| `-skip-produce` | `false` | Skip producing to Kafka — run sorters only |

Example — rerun just the sorters on an already-produced topic:
```bash
go run ./cmd/main.go -skip-gen -skip-produce=false -skip-gen=true
# or more simply:
go run ./cmd/main.go -skip-gen
```

### Run tests
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

---

## Stop
```bash
docker-compose down
```