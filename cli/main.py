import argparse, sys, json, glob, os
from telemachus.validate import validate as _validate, to_parquet as _to_parquet
from telemachus.validate import from_parquet as _from_parquet
from telemachus.validate import score_completeness as _tcs
from telemachus.io import load_jsonl

def _iter_input_files(path: str):
    if os.path.isdir(path):
        for p in glob.glob(os.path.join(path, "*.json")) + glob.glob(os.path.join(path, "*.jsonl")):
            yield p
    else:
        yield path

def cmd_validate(args):
    any_error = False
    for f in _iter_input_files(args.input):
        res = _validate(f, schema=args.schema)
        if not res["ok"]:
            any_error = True
            print(f"❌ {f}:")
            print(json.dumps(res, indent=2, ensure_ascii=False))
        else:
            print(f"✅ {f}: valid")
    sys.exit(1 if any_error else 0)

def cmd_to_parquet(args):
    # if dir → concatenate rows of all files into one parquet
    records = []
    if os.path.isdir(args.input):
        for f in _iter_input_files(args.input):
            # validate each file before ingest
            res = _validate(f, schema=args.schema)
            if not res["ok"]:
                print(f"❌ {f} invalid, aborting.", file=sys.stderr)
                print(json.dumps(res, indent=2, ensure_ascii=False))
                sys.exit(1)
            df = load_jsonl(f)
            records.append(df)
        import pandas as pd
        if not records:
            print("No JSON/JSONL files found.", file=sys.stderr)
            sys.exit(1)
        df = pd.concat(records, ignore_index=True)
        df.to_parquet(args.output, index=False)
    else:
        _ = _to_parquet(args.input, args.output, schema=args.schema)
    print(f"💾 wrote {args.output}")

def cmd_tcs(args):
    import pandas as pd
    if args.input.lower().endswith(".parquet"):
        df = _from_parquet(args.input)
    else:
        df = load_jsonl(args.input)
    res = _tcs(df)
    if args.json:
        print(json.dumps(res, indent=2, ensure_ascii=False))
    else:
        print(f"TCS: {res['score_pct']:.1f}%")
        # show top missing
        cov = res["coverage"]
        missing = sorted((k, v) for k, v in cov.items() if v == 0.0)
        if missing:
            print("Missing fields (0% coverage):")
            for k, _ in missing[:20]:
                print(f" - {k}")

def main():
    p = argparse.ArgumentParser(prog="telemachus")
    sub = p.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("validate", help="Validate JSON/JSONL file or directory")
    v.add_argument("input", help="Path to file or directory")
    v.add_argument("--schema", default=None, help="Schema path or URL (optional)")
    v.set_defaults(func=cmd_validate)

    tp = sub.add_parser("to-parquet", help="Convert JSON/JSONL (or directory) to Parquet")
    tp.add_argument("input", help="Path to file or directory")
    tp.add_argument("-o", "--output", required=True, help="Output Parquet path")
    tp.add_argument("--schema", default=None, help="Schema path or URL (optional)")
    tp.set_defaults(func=cmd_to_parquet)

    t = sub.add_parser("tcs", help="Compute Telemachus Completeness Score (JSON/Parquet)")
    t.add_argument("input", help="Path to file (JSON/JSONL/Parquet)")
    t.add_argument("--json", action="store_true", help="Output JSON")
    t.set_defaults(func=cmd_tcs)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()