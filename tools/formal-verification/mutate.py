"""Applies one textual mutation to a file, failing loudly when the pattern is absent.

Usage: python3 mutate.py <file> <old> <new>
"""
import sys

path, old, new = sys.argv[1], sys.argv[2], sys.argv[3]
source = open(path).read()
if old not in source:
    raise SystemExit(f"mutation pattern not found in {path}: {old!r}")
open(path, "w").write(source.replace(old, new, 1))
