"""Routes: the HTTP view layer.

One FastAPI router per area — ``auth``, ``account``, ``conversations``,
``billing``, ``secrets`` — plus shared dependencies in ``limits``
(rate-limit guards).  These modules are deliberately thin: they wire
HTTP (path/verb, dependencies, status codes, response schemas) and
delegate every decision to ``app.controllers``.  A controller refuses
work by raising a ``DomainError``, which ``app.main`` renders as the
matching HTTP response, so routes never contain business rules.
"""
