# Telemachus CLI

Command-line tool for validating and converting **Telemachus Core** data.

## Quickstart

### Install locally (editable)
```bash
pip install -e .
```

### Commands

#### Validate
Validate a JSON/JSONL file or directory:
```bash
telemachus validate examples/geotab.json
```

#### Convert to Parquet
Convert a JSON/JSONL file or directory into one Parquet file:
```bash
telemachus to-parquet examples/ -o out.parquet
```

#### Compute Completeness Score (TCS)
Compute the Telemahus Completeness Score on JSON or Parquet:
```bash
telemachus tcs out.parquet
```

### Docker Usage
```bash
docker build -t telemachus-cli .
docker run -v $(pwd):/data telemachus-cli validate /data/examples/geotab.json
docker run -v $(pwd):/data telemachus-cli to-parquet /data/examples -o /data/out.parquet
docker run -v $(pwd):/data telemachus-cli tcs /data/out.parquet
```

## License
AGPL-3.0