#!/usr/bin/env python3
"""Deterministic mock "external optimizer" for testing
ExternalProcessOptimizer's protocol handling. Not a real optimization
algorithm -- reads the search space, evaluates every combination of the
"x" parameter in ascending order, and reports whichever had the highest
score as the winner. Speaks newline-delimited JSON over stdin/stdout,
per docs/superpowers/specs/2026-07-30-apexes-optimize-design.md §七.
"""
import json
import sys


def read_line():
    line = sys.stdin.readline()
    if not line:
        return None
    return json.loads(line)


def write_line(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def main():
    initial = read_line()
    search_space = initial["search_space"]
    x_values = search_space["x"]

    best_params = None
    best_score = None
    for x in sorted(x_values):
        write_line({"evaluate": {"x": x}})
        response = read_line()
        if "error" in response:
            continue
        score = response["score"]
        if best_score is None or score > best_score:
            best_score = score
            best_params = {"x": x}

    if best_params is None:
        write_line({"done": None})
    else:
        write_line({"done": {"best_params": best_params, "best_score": best_score}})


if __name__ == "__main__":
    main()
