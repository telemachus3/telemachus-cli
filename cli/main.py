import argparse
import json
from telemachus.validate import validate

def main():
    parser = argparse.ArgumentParser(prog="telemachus")
    sub = parser.add_subparsers(dest="command")

    val = sub.add_parser("validate", help="Validate a JSONL file")
    val.add_argument("file", help="Path to JSONL file")

    args = parser.parse_args()

    if args.command == "validate":
        errors = validate(args.file)
        if any(errors):
            print("Validation errors found:", errors)
        else:
            print("All records valid!")

if __name__ == "__main__":
    main()