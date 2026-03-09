#!/usr/bin/env python3
import argparse
import math
from pathlib import Path
from typing import Iterable, List, Set


ALPHABET = "abcdefghijklmnopqrstuvwxyz"


def to_base26(n: int, width: int) -> str:
    if n < 0:
        raise ValueError("n must be >= 0")
    out: List[str] = []
    x = n
    for _ in range(width):
        out.append(ALPHABET[x % 26])
        x //= 26
    return "".join(reversed(out))


def code_width(n_variants: int) -> int:
    if n_variants <= 1:
        return 1
    return max(1, math.ceil(math.log(n_variants, 26)))


def valid_word(w: str) -> bool:
    return bool(w) and all("a" <= c <= "z" for c in w)


def load_base_words(path: Path) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            w = line.strip().lower()
            if not valid_word(w):
                continue
            if w in seen:
                continue
            seen.add(w)
            out.append(w)
    return out


def choose_marker(base_word: str, code: str, marker_seed: str, original_words: Set[str]) -> str:
    marker = marker_seed
    # Rare fallback for collisions with existing original words.
    # Example collision: base="app", marker="zzq", code="aa" -> "appzzqaa" may already exist.
    i = 0
    while f"{base_word}{marker}{code}" in original_words:
        marker = f"{marker_seed}{to_base26(i, 2)}"
        i += 1
    return marker


def generate_scaled_words(base_words: Iterable[str], factor: int, marker_seed: str) -> Iterable[str]:
    base_words = list(base_words)
    original_set = set(base_words)
    n_extra = factor - 1
    if n_extra <= 0:
        for w in base_words:
            yield w
        return

    width = code_width(n_extra)
    for w in base_words:
        yield w
        for i in range(n_extra):
            code = to_base26(i, width)
            marker = choose_marker(w, code, marker_seed, original_set)
            yield f"{w}{marker}{code}"


def main() -> None:
    ap = argparse.ArgumentParser(
        prog="generate_scaled_wordlist.py",
        description="Generate a larger unique lowercase wordlist by deterministic suffix expansion.",
    )
    ap.add_argument("--input", required=True, help="input wordlist path")
    ap.add_argument("--output", required=True, help="output wordlist path")
    ap.add_argument("--factor", type=int, default=10, help="scale factor (default: 10)")
    ap.add_argument(
        "--marker",
        default="zzq",
        help="marker inserted before generated suffix code (letters only, default: zzq)",
    )
    args = ap.parse_args()

    if args.factor < 1:
        raise SystemExit("--factor must be >= 1")
    marker = args.marker.strip().lower()
    if not marker or any(c < "a" or c > "z" for c in marker):
        raise SystemExit("--marker must contain only a-z")

    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    base_words = load_base_words(in_path)
    if not base_words:
        raise SystemExit(f"no valid words loaded from {in_path}")

    total = len(base_words) * args.factor
    with out_path.open("w", encoding="utf-8") as f:
        for w in generate_scaled_words(base_words, args.factor, marker):
            f.write(w)
            f.write("\n")

    print(f"input_words={len(base_words)}")
    print(f"factor={args.factor}")
    print(f"output_words={total}")
    print(f"output={out_path}")


if __name__ == "__main__":
    main()
