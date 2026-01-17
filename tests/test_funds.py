"""
Unit tests for india_stocks_api.funds module.
Checks logic for fetching and processing margin/funds data.
"""

import pytest
import types

import india_stocks_api.funds as funds


BROKER = "angelone"
FAKE_TOKEN = "auth-token-123"


# ------------------------------------------------------------------
# GLOBAL FIXTURE: mock auth layer (NON-NEGOTIABLE)
# ------------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_auth_layer(monkeypatch):
    monkeypatch.setattr(
        funds,
        "get_supported_brokers",
        lambda: [BROKER],
    )

    monkeypatch.setattr(
        funds,
        "is_authenticated",
        lambda broker: broker == BROKER,
    )

    monkeypatch.setattr(
        funds,
        "get_broker_token",
        lambda broker: {"auth_token": FAKE_TOKEN} if broker == BROKER else None,
    )

    yield


# ------------------------------------------------------------------
# HELPER: fake funds module
# ------------------------------------------------------------------

def make_fake_funds_module(return_value=None, raise_exc=None):
    module = types.SimpleNamespace()

    if raise_exc:
        def get_margin_data(_):
            raise raise_exc
    else:
        def get_margin_data(_):
            return return_value

    module.get_margin_data = get_margin_data
    return module


# ------------------------------------------------------------------
# _resolve_funds_module
# ------------------------------------------------------------------

def test_resolve_funds_module_success(monkeypatch):
    fake_module = make_fake_funds_module({})

    monkeypatch.setattr(
        "importlib.import_module",
        lambda _: fake_module,
    )

    module = funds._resolve_funds_module(BROKER)
    assert hasattr(module, "get_margin_data")


def test_resolve_funds_module_missing(monkeypatch):
    def fake_import(_):
        raise ModuleNotFoundError

    monkeypatch.setattr("importlib.import_module", fake_import)

    with pytest.raises(ModuleNotFoundError):
        funds._resolve_funds_module(BROKER)


def test_resolve_funds_module_missing_method(monkeypatch):
    monkeypatch.setattr(
        "importlib.import_module",
        lambda _: types.SimpleNamespace(),
    )

    with pytest.raises(AttributeError):
        funds._resolve_funds_module(BROKER)


# ------------------------------------------------------------------
# get_margin
# ------------------------------------------------------------------

def test_get_margin_success(monkeypatch):
    fake_margin = {
        "availablecash": "10000",
        "collateral": "0",
    }

    fake_module = make_fake_funds_module(fake_margin)

    monkeypatch.setattr(
        "importlib.import_module",
        lambda _: fake_module,
    )

    margin = funds.get_margin(BROKER)
    assert margin == fake_margin


def test_get_margin_unsupported_broker(monkeypatch):
    monkeypatch.setattr(
        funds,
        "get_supported_brokers",
        lambda: [],
    )

    margin = funds.get_margin(BROKER)
    assert margin == {}


def test_get_margin_not_authenticated(monkeypatch):
    monkeypatch.setattr(
        funds,
        "is_authenticated",
        lambda _: False,
    )

    margin = funds.get_margin(BROKER)
    assert margin == {}


def test_get_margin_empty_token(monkeypatch):
    monkeypatch.setattr(
        funds,
        "get_broker_token",
        lambda _: {"auth_token": ""},
    )

    margin = funds.get_margin(BROKER)
    assert margin == {}


def test_get_margin_broker_exception(monkeypatch):
    fake_module = make_fake_funds_module(raise_exc=RuntimeError("API down"))

    monkeypatch.setattr(
        "importlib.import_module",
        lambda _: fake_module,
    )

    margin = funds.get_margin(BROKER)
    assert margin == {}


# ------------------------------------------------------------------
# get_all_margins
# ------------------------------------------------------------------

def test_get_all_margins_success(monkeypatch):
    fake_margin = {"availablecash": "5000"}

    fake_module = make_fake_funds_module(fake_margin)

    monkeypatch.setattr(
        "importlib.import_module",
        lambda _: fake_module,
    )

    all_margins = funds.get_all_margins()
    assert all_margins == {BROKER: fake_margin}


def test_get_all_margins_none_authenticated(monkeypatch):
    monkeypatch.setattr(
        funds,
        "is_authenticated",
        lambda _: False,
    )

    assert funds.get_all_margins() == {}


# ------------------------------------------------------------------
# get_available_cash
# ------------------------------------------------------------------

def test_get_available_cash_success(monkeypatch):
    fake_margin = {"availablecash": "7500"}

    fake_module = make_fake_funds_module(fake_margin)

    monkeypatch.setattr(
        "importlib.import_module",
        lambda _: fake_module,
    )

    cash = funds.get_available_cash(BROKER)
    assert cash == "7500"


def test_get_available_cash_no_margin(monkeypatch):
    monkeypatch.setattr(
        funds,
        "get_margin",
        lambda _: {},
    )

    with pytest.raises(ValueError):
        funds.get_available_cash(BROKER)


def test_get_available_cash_missing_key(monkeypatch):
    monkeypatch.setattr(
        funds,
        "get_margin",
        lambda _: {"foo": "bar"},
    )

    with pytest.raises(KeyError):
        funds.get_available_cash(BROKER)
