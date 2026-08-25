from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import numpy as np
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


@dataclass
class VectorizedVocabulary:
    property_uri: str
    candidates: list[str]
    vec_prefix: TfidfVectorizer
    matrix_prefix: Any
    vec_postfix: TfidfVectorizer
    matrix_postfix: Any


class SuggestionsEngine:
    def __init__(self, vocab_dict: dict[str, set[str]]):
        self.vocab_dict = vocab_dict
        self.by_uri: dict[str, VectorizedVocabulary] = {}
        self.by_cand: dict[frozenset[str], VectorizedVocabulary] = {}
        self.vectorized = False

    def vectorize(self):
        for prop_uri, candidates in self.vocab_dict.items():
            cand_list = list(candidates)
            if not cand_list:
                continue
            try:
                cand_splits = [self.__split_prefix_postfix(c) for c in cand_list]
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
                self.by_uri[prop_uri] = v_vocab
                self.by_cand[frozenset(cand_list)] = v_vocab
            except Exception as e:
                print(f"Failed to vectorize vocabulary for {prop_uri}: {e}")
        self.vectorized = True

    def __split_prefix_postfix(self, uri: str) -> tuple[str, str]:
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

    def __cosine_sim(
        self,
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

    def uri_score_fuzzy_split(self, query: str, candidate: str) -> float:
        q = self.__split_prefix_postfix(query)
        c = self.__split_prefix_postfix(candidate)
        prefix_score = fuzz.ratio(q[0], c[0])
        postfix_score = fuzz.ratio(q[1], c[1])

        return 0.2 * prefix_score + 0.8 * postfix_score

    def uri_score_cosine(self, query: str, candidate: str) -> float:
        """Calculate cosine similarity score (0.0 to 100.0) between two URIs (pairwise fallback)."""
        if not self.vectorized:
            raise Exception(
                "The vocabulary must be vectorized before calculating cosine similarity"
            )
        if query.strip().rstrip("/") == candidate.strip().rstrip("/"):
            return 100.0
        q = self.__split_prefix_postfix(query)
        c = self.__split_prefix_postfix(candidate)

        prefix_score = self.__cosine_sim(
            q[0], c[0], analyzer="char_wb", ngram_range=(2, 4)
        )
        postfix_score = self.__cosine_sim(
            q[1], c[1], analyzer="char_wb", ngram_range=(2, 3)
        )
        return 0.2 * prefix_score + 0.8 * postfix_score

    def __batch_matrix_cosine(
        self,
        queries: list[str],
        vec_vocab: VectorizedVocabulary,
        limit: int = 3,
        cutoff: float = 50.0,
    ) -> dict[str, list[tuple[str, float]]]:
        if not queries or not vec_vocab.candidates:
            return {q: [] for q in queries}

        q_splits = [self.__split_prefix_postfix(q) for q in queries]
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
        self,
        queries: list[str],
        candidates: set[str] | list[str],
        scorer: Callable[[str, str], float],
        limit: int = 3,
        cutoff: float = 50.0,
    ) -> dict[str, list[tuple[str, float]]]:
        """Match a list of query URIs against candidates using a configurable scorer function.

        Supports:
        - RapidFuzz scorers: self.uri_score_fuzzy_split, fuzz.WRatio, etc.
        - Cosine distance: self.uri_score_cosine (automatically uses ultra-fast matrix acceleration)
        """
        if scorer == self.uri_score_cosine:
            cand_key = frozenset(candidates)
            if cand_key in self.by_cand:
                return self.__batch_matrix_cosine(
                    queries,
                    self.by_cand[cand_key],
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
