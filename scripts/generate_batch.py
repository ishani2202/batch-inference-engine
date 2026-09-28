"""Generate data/sample_batch.json: N varied prompts plus a few deliberately broken items.

The broken items prove error isolation in a real run: each lands in errors.jsonl
with a clear reason while the rest of the batch completes.

    python scripts/generate_batch.py                 # 1,000 items -> data/sample_batch.json
    python scripts/generate_batch.py -n 500000 -o data/big_batch.json --no-broken
"""

import argparse
import itertools
import json
import random
from collections.abc import Iterator
from pathlib import Path

TOPICS = [
    "photosynthesis", "black holes", "the French Revolution", "machine learning", "the water cycle",
    "compound interest", "DNA replication", "plate tectonics", "the Roman Empire", "quantum computing",
    "supply and demand", "the immune system", "climate change", "the printing press", "neural networks",
    "volcanoes", "the stock market", "vaccines", "renewable energy", "the Internet", "gravity",
    "the Renaissance", "blockchain", "ocean currents", "the human brain",
]

TEMPLATES = [
    "Explain {t} in one sentence.",
    "Give one surprising fact about {t}.",
    "Describe {t} to a ten-year-old in two sentences.",
    "What is a common misconception about {t}? Answer briefly.",
    "Write a one-line analogy that explains {t}.",
    "Name one real-world application of {t} in a single sentence.",
    "Summarize the history of {t} in two sentences.",
    "What question would an expert ask about {t}? Reply with just the question.",
]

# (description, item) pairs. Each is a different kind of corrupt input.
BROKEN = [
    ("empty prompt", {"id": "broken-empty", "prompt": ""}),
    ("whitespace prompt", {"id": "broken-blank", "prompt": "   "}),
    ("null item", None),
    ("number item", 42),
    ("missing prompt field", {"id": "broken-missing", "text": "wrong field name"}),
    ("non-string prompt", {"id": "broken-type", "prompt": ["not", "a", "string"]}),
    ("prompt too long", {"id": "broken-too-long", "prompt": "lorem ipsum " * 5000}),
]


def generate(n: int, include_broken: bool, seed: int = 7) -> Iterator:
    """Yield n items. A generator, so even 500k items never sit in memory at once."""
    rng = random.Random(seed)
    combos = [tpl.format(t=topic) for topic, tpl in itertools.product(TOPICS, TEMPLATES)]
    broken = [item for _, item in BROKEN] if include_broken and n >= len(BROKEN) else []
    # Scatter the broken items through the batch rather than bunching them at the end.
    broken_at = dict(zip(sorted(rng.sample(range(n), len(broken))), broken))
    good = 0
    for i in range(n):
        if i in broken_at:
            yield broken_at[i]
        else:
            yield {"id": f"p-{good:06d}", "prompt": combos[good % len(combos)]}
            good += 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-n", type=int, default=1000, help="total number of items (default 1000)")
    parser.add_argument("-o", "--output", type=Path, default=Path("data/sample_batch.json"))
    parser.add_argument("--no-broken", action="store_true", help="omit the deliberately broken items")
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, item in enumerate(generate(args.n, include_broken=not args.no_broken)):
            f.write(("  " if i == 0 else ",\n  ") + json.dumps(item))
        f.write("\n]\n")
    print(f"wrote {args.n} items to {args.output}")


if __name__ == "__main__":
    main()
