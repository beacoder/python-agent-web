"""Request/response validation.

Pydantic models (``schemas``) that define the shapes crossing the HTTP
boundary — request bodies FastAPI validates on the way in, and the
response models routes serialize on the way out.  Kept apart from the
ORM ``app.models`` so the wire contract and the persisted schema can
evolve independently.
"""
