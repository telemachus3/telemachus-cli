import argparse, sys, json
from telemachus.validate import validate

def main():
    p = argparse.ArgumentParser(prog="telemachus")
    sub = p.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("validate", help="Validate a JSONL file against Telemachus schema")
    v.add_argument("file", help="Path to JSONL file")
    v.add_argument("--schema", help="Schema path or URL (optional)", default=None)

    args = p.parse_args()
    if args.cmd == "validate":
        res = validate(args.file, schema=args.schema)
        if res["ok"]:
            print("✅ All records valid!")
            sys.exit(0)
        print("❌ Validation errors:")
        print(json.dumps(res, indent=2, ensure_ascii=False))
        sys.exit(1)

if __name__ == "__main__":
    main()