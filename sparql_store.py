import rdflib
from rdflib.plugins.stores import sparqlstore
from rdflib import Graph, URIRef, IdentifiedNode, Variable, Literal

# Defines some SPARQL keywords
LIMIT = "LIMIT"
OFFSET = "OFFSET"
ORDERBY = "ORDER BY"

def cast(cls, o):
    return o

class SPARQLStore(sparqlstore.SPARQLStore):

    # type error: Return type "Iterator[tuple[tuple[Node, Node, Node], None]]" of "triples" incompatible with return type "Iterator[tuple[tuple[Node, Node, Node], Iterator[Optional[Graph]]]]"
    def triples(  # type: ignore[override]
        self, spo, context = None
    ):
        """
        - tuple **(s, o, p)**
          the triple used as filter for the SPARQL select.
          (None, None, None) means anything.
        - context **context**
          the graph effectively calling this method.

        Returns a tuple of triples executing essentially a SPARQL like
        SELECT ?subj ?pred ?obj WHERE { ?subj ?pred ?obj }

        **context** may include three parameter
        to refine the underlying query:

        * LIMIT: an integer to limit the number of results
        * OFFSET: an integer to enable paging of results
        * ORDERBY: an instance of Variable('s'), Variable('o') or Variable('p') or, by default, the first 'None' from the given triple

        !!! warning "Limit and offset

            - Using LIMIT or OFFSET automatically include ORDERBY otherwise this is
              because the results are retrieved in a not deterministic way (depends on
              the walking path on the graph)
            - Using OFFSET without defining LIMIT will discard the first OFFSET - 1 results

        ```python
        a_graph.LIMIT = limit
        a_graph.OFFSET = offset
        triple_generator = a_graph.triples(mytriple):
        # do something
        # Removes LIMIT and OFFSET if not required for the next triple() calls
        del a_graph.LIMIT
        del a_graph.OFFSET
        ```
        """

        p: IdentifiedNode | Variable
        s: IdentifiedNode | Literal | Variable
        o: IdentifiedNode | Literal | Variable
        _s, _p, _o = spo

        vars: list[Variable] = []
        if _s is None:
            s = Variable("s")
            vars.append(s)
        elif isinstance(_s, Variable):
            s = _s
            vars.append(s)
        # Technically we should check for QuotedGraph here, to make MyPy happy
        elif isinstance(_s, Graph):  # type: ignore[unreachable]
            raise ValueError("Cannot use a Graph as subject in SPARQLStore.")
        else:
            s = _s

        if _p is None:
            p = Variable("p")
            vars.append(p)
        else:
            p = _p

        if _o is None:
            o = Variable("o")
            vars.append(o)
        elif isinstance(_o, Variable):
            o = _o
            vars.append(o)
        # Technically we should check for QuotedGraph here, to make MyPy happy
        elif isinstance(_o, Graph):  # type: ignore[unreachable]
            raise ValueError("Cannot use a Graph as object in SPARQLStore.")
        else:
            o = _o
        if vars:
            v = " ".join([term.n3() for term in vars])
            verb = "SELECT %s " % v
        else:
            verb = "ASK"

        target_graph = context.identifier if self._is_contextual(context) else None  # type: ignore[union-attr]

        nts = self.node_to_sparql
        if target_graph:
            query = "%s { GRAPH %s { %s %s %s } }" % (verb, target_graph.n3(), nts(s), nts(p), nts(o))
        else:
            query = "%s { %s %s %s }" % (verb, nts(s), nts(p), nts(o))

        # The ORDER BY is necessary
        if (
            hasattr(context, LIMIT)
            or hasattr(context, OFFSET)
            or hasattr(context, ORDERBY)
        ):
            var = None
            if isinstance(s, Variable):
                var = s
            elif isinstance(p, Variable):
                var = p
            elif isinstance(o, Variable):
                var = o
            elif hasattr(context, ORDERBY) and isinstance(
                getattr(context, ORDERBY), Variable
            ):
                var = getattr(context, ORDERBY)
            # type error: Item "None" of "Optional[Variable]" has no attribute "n3"
            query = query + " %s %s" % (ORDERBY, var.n3())  # type: ignore[union-attr]

        try:
            query = query + " LIMIT %s" % int(getattr(context, LIMIT))
        except (ValueError, TypeError, AttributeError):
            pass
        try:
            query = query + " OFFSET %s" % int(getattr(context, OFFSET))
        except (ValueError, TypeError, AttributeError):
            pass

        print("Executing query:")
        print(query)

        result = self._query(
            query,
            # type error: Item "None" of "Optional[Graph]" has no attribute "identifier"
            default_graph=target_graph
        )

        if vars:
            if type(result) is tuple:
                if result[0] == 401:
                    raise ValueError(
                        "It looks like you need to authenticate with this SPARQL Store. HTTP unauthorized"
                    )
            for row in result:
                yield (
                    (
                        row.get(s, URIRef(f"urn:undef:{s}"))
                        if isinstance(s, Variable)
                        else row.get(s, s)
                    ),
                    # TODO: getting value of ?p variable can return a Literal,
                    #  but literal cannot be yielded in the predicate slot.
                    cast(
                        IdentifiedNode,
                        (
                            row.get(p, URIRef(f"urn:undef:{p}"))
                            if isinstance(p, Variable)
                            else row.get(p, p)
                        ),
                    ),
                    (
                        row.get(o, URIRef(f"urn:undef:{o}"))
                        if isinstance(o, Variable)
                        else row.get(o, o)
                    ),
                ), None  # why is the context here not the passed in graph 'context'?
        else:
            if result.askAnswer:
                yield (s, cast(IdentifiedNode, p), o), None

    def __len__(self, context = None) -> int:
        if not self.sparql11:
            raise NotImplementedError(
                "For performance reasons, this is not"
                + "supported for sparql1.0 endpoints"
            )
        else:
            default_graph = (
                context.identifier  # type: ignore[union-attr]
                if self._is_contextual(context)
                else None
            )
            if default_graph:
                q = "SELECT (count(*) as ?c) WHERE { GRAPH %s { ?s ?p ?o . } }" % (default_graph.n3(),)
            else:
                q = "SELECT (count(*) as ?c) WHERE {?s ?p ?o .}"

            result = self._query(
                q,
                default_graph=default_graph
            )
            # type error: Item "tuple[Node, ...]" of "Union[tuple[Node, Node, Node], bool, ResultRow]" has no attribute "c"
            return int(next(iter(result)).c)  # type: ignore[union-attr]
