# Telemachus CLI

Command-line tool for validating and converting **Telemachus Core** data.

## Quickstart

```bash
python -m cli.main validate examples/geotab.jsonl
```

Or via Docker:
```bash
docker build -t telemachus-cli .
docker run -v $(pwd):/data telemachus-cli validate /data/examples/geotab.jsonl
```

## License
AGPL-3.0