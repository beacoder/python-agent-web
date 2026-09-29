"""Cost/budget enforcement: the spend kill-switch before a run starts."""

from __future__ import annotations

import pytest

from app.controllers import usage
from app.controllers.errors import PaymentRequired
from app.infra.config import get_settings
from app.infra.db import get_session_factory, reset_engine_for_tests
from app.models import Conversation, UsageEvent, User, new_id


def _session():
    reset_engine_for_tests(get_settings().db_url)
    return get_session_factory()()


def _user(db, *, plan_points=0, pack_points=0) -> User:
    user = User(
        id=new_id("usr"),
        email=f"{new_id('u')}@example.com",
        password_hash="x",
        plan_points=plan_points,
        pack_points=pack_points,
    )
    db.add(user)
    db.flush()
    return user


def _spend(db, user, tokens: int) -> None:
    conv = Conversation(id=new_id("cnv"), user_id=user.id, title="t")
    db.add(conv)
    db.flush()
    db.add(
        UsageEvent(
            id=new_id("use"),
            user_id=user.id,
            conversation_id=conv.id,
            run_id=new_id("run"),
            input_tokens=tokens,
            output_tokens=0,
            rounds=1,
        )
    )
    db.flush()


class TestTokenBudget:
    def test_free_tier_budget(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PAW_BUDGET_FREE_TOKENS", "500000")
        monkeypatch.setenv("PAW_BUDGET_TOKENS_PER_POINT", "1000")
        get_settings.cache_clear()
        try:
            with _session() as db:
                user = _user(db)
                assert usage.token_budget(user) == 500_000
        finally:
            get_settings.cache_clear()

    def test_points_raise_the_ceiling(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PAW_BUDGET_FREE_TOKENS", "100")
        monkeypatch.setenv("PAW_BUDGET_TOKENS_PER_POINT", "10")
        get_settings.cache_clear()
        try:
            with _session() as db:
                user = _user(db, plan_points=5, pack_points=3)
                # 100 free + (5+3)*10 = 180
                assert usage.token_budget(user) == 180
        finally:
            get_settings.cache_clear()

    def test_consumed_tokens_sums_ledger(self) -> None:
        with _session() as db:
            user = _user(db)
            _spend(db, user, 1200)
            _spend(db, user, 800)
            assert usage.consumed_tokens(db, user) == 2000


class TestEnforcement:
    def test_disabled_by_default_is_noop(self) -> None:
        with _session() as db:
            user = _user(db)
            _spend(db, user, 10_000_000_000)  # way over any budget
            # enforcement off by default: must not raise
            usage.enforce_budget(db, user)

    def test_blocks_when_exhausted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PAW_BUDGET_ENFORCE", "true")
        monkeypatch.setenv("PAW_BUDGET_FREE_TOKENS", "1000")
        monkeypatch.setenv("PAW_BUDGET_TOKENS_PER_POINT", "1000")
        get_settings.cache_clear()
        try:
            with _session() as db:
                user = _user(db)
                _spend(db, user, 1000)  # exactly at budget -> exhausted
                with pytest.raises(PaymentRequired, match="budget exhausted"):
                    usage.enforce_budget(db, user)
        finally:
            get_settings.cache_clear()

    def test_allows_when_under_budget(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PAW_BUDGET_ENFORCE", "true")
        monkeypatch.setenv("PAW_BUDGET_FREE_TOKENS", "1000")
        get_settings.cache_clear()
        try:
            with _session() as db:
                user = _user(db)
                _spend(db, user, 999)
                usage.enforce_budget(db, user)  # under budget: ok
                status = usage.budget_status(db, user)
                assert status.remaining == 1
                assert not status.exhausted
        finally:
            get_settings.cache_clear()

    def test_points_extend_budget_past_free_tier(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PAW_BUDGET_ENFORCE", "true")
        monkeypatch.setenv("PAW_BUDGET_FREE_TOKENS", "1000")
        monkeypatch.setenv("PAW_BUDGET_TOKENS_PER_POINT", "1000")
        get_settings.cache_clear()
        try:
            with _session() as db:
                # free tier alone (1000) would be exhausted by 1500 spent,
                # but 1 point adds 1000 -> budget 2000, so it's allowed
                user = _user(db, pack_points=1)
                _spend(db, user, 1500)
                usage.enforce_budget(db, user)  # must not raise
        finally:
            get_settings.cache_clear()


class TestRunEndpointBudget:
    def test_run_returns_402_when_exhausted(
        self, client, auth_headers, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # create a conversation, then exhaust the budget and try to run
        conv = client.post("/conversations", json={"title": "c"}, headers=auth_headers).json()

        # enable enforcement with a tiny budget and record over-limit usage
        from app.controllers.usage import consumed_tokens  # noqa: F401
        from app.infra.db import get_session_factory
        from app.models import Conversation as Conv
        from app.models import UsageEvent, new_id

        db = get_session_factory()()
        try:
            owner_id = db.query(Conv).filter(Conv.id == conv["id"]).one().user_id
            db.add(
                UsageEvent(
                    id=new_id("use"),
                    user_id=owner_id,
                    conversation_id=conv["id"],
                    run_id=new_id("run"),
                    input_tokens=10_000,
                    output_tokens=0,
                    rounds=1,
                )
            )
            db.commit()
        finally:
            db.close()

        monkeypatch.setenv("PAW_BUDGET_ENFORCE", "true")
        monkeypatch.setenv("PAW_BUDGET_FREE_TOKENS", "1000")
        get_settings.cache_clear()
        try:
            res = client.post(
                f"/conversations/{conv['id']}/runs",
                json={"prompt": "hello"},
                headers=auth_headers,
            )
            assert res.status_code == 402, res.text
            assert "budget" in res.json()["detail"].lower()
        finally:
            get_settings.cache_clear()
