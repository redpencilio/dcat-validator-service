from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import numpy as np
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def split_prefix_postfix(uri: str) -> tuple[str, str]:
    uri = uri.strip()
    split = urlsplit(uri)
    if split.fragment == "":
        path = split.path.rstrip("/")
        if "/" in path:
            prefix = f"{split.scheme}://{split.netloc}{path.rsplit('/', 1)[0]}"
            postfix = path.rsplit("/", 1)[1]
        else:
            prefix = f"{split.scheme}://{split.netloc}"
            postfix = path
        return (prefix, postfix)
    else:
        prefix = f"{split.scheme}://{split.netloc}{split.path.rstrip('/')}"
        postfix = split.fragment.rstrip("/")
        return (prefix, postfix)


def _cosine_sim(
    s1: str,
    s2: str,
    analyzer: str = "char_wb",
    ngram_range: tuple[int, int] = (2, 3),
) -> float:
    """Calculate character n-gram cosine similarity (0.0 to 100.0) between two strings."""
    if s1 == s2:
        return 100.0
    if not s1 or not s2:
        return 0.0
    try:
        vec = TfidfVectorizer(analyzer=analyzer, ngram_range=ngram_range)
        matrix = vec.fit_transform([s1, s2])
        sim = float(cosine_similarity(matrix[0:1], matrix[1:2])[0, 0])
        return sim * 100.0
    except ValueError:
        return 0.0


def uri_score_fuzzy_split(query: str, candidate: str) -> float:
    q = split_prefix_postfix(query)
    c = split_prefix_postfix(candidate)
    prefix_score = fuzz.ratio(q[0], c[0])
    postfix_score = fuzz.ratio(q[1], c[1])

    return 0.2 * prefix_score + 0.8 * postfix_score


def uri_score_cosine(query: str, candidate: str) -> float:
    """Calculate cosine similarity score (0.0 to 100.0) between two URIs (pairwise fallback)."""
    if query.strip().rstrip("/") == candidate.strip().rstrip("/"):
        return 100.0
    q = split_prefix_postfix(query)
    c = split_prefix_postfix(candidate)

    prefix_score = _cosine_sim(q[0], c[0], analyzer="char_wb", ngram_range=(2, 4))
    postfix_score = _cosine_sim(q[1], c[1], analyzer="char_wb", ngram_range=(2, 3))
    return 0.2 * prefix_score + 0.8 * postfix_score


@dataclass
class VectorizedVocabulary:
    property_uri: str
    candidates: list[str]
    vec_prefix: TfidfVectorizer
    matrix_prefix: Any
    vec_postfix: TfidfVectorizer
    matrix_postfix: Any


def vectorize_vocabulary(
    vocab_dict: Mapping[str, Any] | None = None,
) -> tuple[dict[str, VectorizedVocabulary], dict[frozenset[str], VectorizedVocabulary]]:
    """Pre-vectorize controlled vocabularies at startup for ultra-fast matrix cosine distance."""
    vocab_map = get_vocabulary_dict() if vocab_dict is None else vocab_dict
    by_uri: dict[str, VectorizedVocabulary] = {}
    by_candidates: dict[frozenset[str], VectorizedVocabulary] = {}

    for prop_uri, candidates in vocab_map.items():
        cand_list = list(candidates)
        if not cand_list:
            continue
        try:
            cand_splits = [split_prefix_postfix(c) for c in cand_list]
            prefixes = [p[0] for p in cand_splits]
            postfixes = [p[1] for p in cand_splits]

            vec_p = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4))
            mat_p = vec_p.fit_transform(prefixes)

            vec_post = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 3))
            mat_post = vec_post.fit_transform(postfixes)

            v_vocab = VectorizedVocabulary(
                property_uri=prop_uri,
                candidates=cand_list,
                vec_prefix=vec_p,
                matrix_prefix=mat_p,
                vec_postfix=vec_post,
                matrix_postfix=mat_post,
            )
            by_uri[prop_uri] = v_vocab
            by_candidates[frozenset(cand_list)] = v_vocab
        except Exception as e:
            print(f"Failed to vectorize vocabulary for {prop_uri}: {e}")

    return by_uri, by_candidates


def _batch_matrix_cosine(
    queries: list[str],
    vec_vocab: VectorizedVocabulary,
    limit: int = 3,
    cutoff: float = 30.0,
) -> dict[str, list[tuple[str, float]]]:
    if not queries or not vec_vocab.candidates:
        return {q: [] for q in queries}

    q_splits = [split_prefix_postfix(q) for q in queries]
    q_prefixes = [p[0] for p in q_splits]
    q_postfixes = [p[1] for p in q_splits]

    q_mat_p = vec_vocab.vec_prefix.transform(q_prefixes)
    q_mat_post = vec_vocab.vec_postfix.transform(q_postfixes)

    sims_p = (q_mat_p @ vec_vocab.matrix_prefix.T).toarray() * 100.0
    sims_post = (q_mat_post @ vec_vocab.matrix_postfix.T).toarray() * 100.0
    total_sims = 0.2 * sims_p + 0.8 * sims_post

    results: dict[str, list[tuple[str, float]]] = {}
    candidates = vec_vocab.candidates

    for row_idx, query in enumerate(queries):
        row_scores = total_sims[row_idx]

        # Fast exact match check
        exact_found = False
        norm_query = query.strip().rstrip("/")
        for c in candidates:
            if norm_query == c.strip().rstrip("/"):
                results[query] = [(c, 100.0)]
                exact_found = True
                break
        if exact_found:
            continue

        if len(candidates) <= limit:
            top_indices = np.argsort(row_scores)[::-1]
        else:
            partition_idx = np.argpartition(row_scores, -limit)[-limit:]
            top_indices = partition_idx[np.argsort(row_scores[partition_idx])[::-1]]

        matched: list[tuple[str, float]] = []
        for idx in top_indices:
            score = float(row_scores[idx])
            if score >= cutoff:
                matched.append((candidates[idx], score))
        results[query] = matched[:limit]

    return results


def get_similar_uris(
    queries: list[str],
    candidates: set[str] | list[str],
    scorer: Callable[[str, str], float],
    limit: int = 3,
    cutoff: float = 50.0,
) -> dict[str, list[tuple[str, float]]]:
    """Match a list of query URIs against candidates using a configurable scorer function.

    Supports:
      - RapidFuzz scorers: uri_score_fuzzy_split, fuzz.WRatio, etc.
      - Cosine distance: uri_score_cosine (automatically uses ultra-fast matrix acceleration)
    """
    if scorer == uri_score_cosine:
        cand_key = frozenset(candidates)
        if cand_key in VECTORIZED_BY_CANDIDATES:
            return _batch_matrix_cosine(
                queries,
                VECTORIZED_BY_CANDIDATES[cand_key],
                limit=limit,
                cutoff=cutoff,
            )

    results: dict[str, list[tuple[str, float]]] = {}
    for query in queries:
        matched: list[tuple[str, float]] = []
        for candidate in candidates:
            score = scorer(query, candidate)
            if score >= cutoff:
                matched.append((candidate, score))
            elif score == 100.0:
                matched = [(candidate, 100.0)]
                break
        matched.sort(key=lambda x: x[1], reverse=True)
        results[query] = matched[:limit]

    return results


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
VECTORIZED_VOCABULARIES, VECTORIZED_BY_CANDIDATES = vectorize_vocabulary(VOC_DICT)
