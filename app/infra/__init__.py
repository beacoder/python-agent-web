"""Infrastructure: stateless, cross-cutting mechanism.

Primitives with no domain entities and no request semantics: ``config``
(settings), ``db`` (engine/session, Alembic migration runner),
``security`` (JWT + password hashing), ``secrets_store`` (Fernet
encrypt/decrypt), ``logging`` (the ``paw.*`` logger tree + request-id
context), and ``ratelimit`` (the sliding-window limiter).

This layer imports nothing from ``app.controllers`` or ``app.models`` —
it holds *how*, never *what a caller may do*.  Policy that takes a
``Session`` or can refuse a request lives in ``app.controllers``.
"""
