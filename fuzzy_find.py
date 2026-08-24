from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlsplit

from rapidfuzz import fuzz, process


def split_prefix_postfix(uri: str) -> tuple[str, str]:
    split = urlsplit(uri)
    if split.fragment == "":
        prefix = f"{split.scheme}://{split.netloc}{split.path.rsplit('/', 1)[0]}"
        postfix = split.path.rsplit("/", 1)[1]
        return (prefix, postfix)
    else:
        prefix = f"{split.scheme}://{split.netloc}{split.path}"
        postfix = split.fragment
        return (prefix, postfix)


def uri_score(query: str, candidate: str) -> float:
    q = split_prefix_postfix(query)
    c = split_prefix_postfix(candidate)
    prefix_score = fuzz.ratio(q[0], c[0])
    postfix_score = fuzz.ratio(q[1], c[1])

    return 0.2 * prefix_score + 0.8 * postfix_score


def find_closest(
    query: str, candidates: set[str], limit=3, cutoff=85
) -> list[tuple[str, float]]:
    if not candidates:
        return []

    # result = process.extractOne(query, candidates, scorer=fuzz.ratio)
    result = process.extract(
        query=query,
        choices=candidates,
        scorer=fuzz.ratio,
        limit=limit,
        score_cutoff=cutoff,
    )

    if len(result) == 0:
        return []

    # match, score, _ = result
    return [(match, score) for (match, score, _) in result]


def find_closest_uri(
    query: str, candidates: set[str], limit, cutoff
) -> list[tuple[str, float]]:
    if not candidates:
        return []
    result: list[tuple[str, float]] = []
    for candidate in candidates:
        score = uri_score(query, candidate)
        if score >= cutoff:
            result.append((candidate, score))
        elif score == 100.0:
            return [(candidate, 100.0)]
        else:
            continue
    result.sort(key=lambda x: x[1], reverse=True)
    return result[0:limit]


def fuzzy_match_uris(
    queries: list[str], candidates: set[str], limit=3, cutoff=50
) -> dict[str, list[tuple[str, float]]]:
    return {
        query: find_closest_uri(query, candidates, limit=limit, cutoff=cutoff)
        for query in queries
    }


def get_vocabulary_dict() -> dict[str, set[str]]:
    vocab_json_path = Path(
        os.environ.get(
            "VOCABULARIES_JSON", Path(__file__).resolve().parent / "vocabularies.json"
        )
    )

    if not vocab_json_path.exists() or vocab_json_path.stat().st_size == 0:
        print(
            f"{vocab_json_path.name} not found. Generating from source vocabularies..."
        )
        try:
            from generate_vocabularies import generate_vocabulary_dict

            raw_dict = generate_vocabulary_dict(output_path=vocab_json_path)
            return {k: set(v) for k, v in raw_dict.items()}
        except Exception as e:
            print(f"Error generating vocabulary dictionary: {e}")
            return {}

    print(f"Loading controlled vocabularies from {vocab_json_path.name}...")
    try:
        with open(vocab_json_path, "r", encoding="utf-8") as f:
            cached_dict = json.load(f)
        return {k: set(v) for k, v in cached_dict.items()}
    except Exception as e:
        print(f"Error loading {vocab_json_path}: {e}")
        return {}


VOC_DICT = get_vocabulary_dict()
vocab = VOC_DICT.get("https://w3id.org/mobilitydcat-ap#transportMode")
if not vocab:
    print("no vocab")
else:
    res1 = find_closest(
        "https://w3id.org/mobilitydcat-ap/transport-mode/bike",
        vocab,
    )
    res2 = find_closest_uri(
        "https://w3id.com/mobilitydcat-ap/transport-mode#bike",
        vocab,
        limit=5,
        cutoff=40,
    )
    # print(res1)
    print(res2)
