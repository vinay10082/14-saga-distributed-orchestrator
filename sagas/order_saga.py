"""Example saga: a distributed order-processing workflow.

Each step simulates a call to a separate microservice (inventory, payment,
shipping, notifications). Every forward action and compensation is keyed by
the saga's idempotency key so repeated invocations (e.g. after crash
recovery) never double-reserve stock, double-charge a card, or double-refund.

This module has no dependency on any real service - it exists to exercise the
orchestrator end-to-end without external infrastructure. Swap the bodies of
`_inventory`, `_payment` and `_shipping` for real HTTP/gRPC/message calls to
adopt this in a real system.
"""

from __future__ import annotations

import logging

from saga_orchestrator.models import Saga, SagaContext

logger = logging.getLogger(__name__)


class _InMemoryLedger:
    """Stand-in for an idempotent microservice: records one entry per
    idempotency key so retried calls are no-ops.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._entries: dict[str, dict] = {}

    def apply(self, key: str, payload: dict) -> dict:
        if key in self._entries:
            logger.info("[%s] idempotent replay for key=%s, skipping", self.name, key)
            return self._entries[key]
        self._entries[key] = payload
        logger.info("[%s] applied key=%s payload=%s", self.name, key, payload)
        return payload

    def undo(self, key: str) -> None:
        if key not in self._entries:
            logger.info("[%s] idempotent undo for key=%s: nothing to reverse", self.name, key)
            return
        del self._entries[key]
        logger.info("[%s] reversed key=%s", self.name, key)


_inventory = _InMemoryLedger("inventory-service")
_payment = _InMemoryLedger("payment-service")
_shipping = _InMemoryLedger("shipping-service")


def build_order_saga() -> Saga:
    saga = Saga(name="order_processing")

    @saga.step(name="reserve_inventory", compensation=_release_inventory)
    def reserve_inventory(ctx: SagaContext) -> dict:
        key = ctx.idempotency_key("reserve_inventory")
        sku = ctx.data["sku"]
        quantity = ctx.data["quantity"]
        if ctx.data.get("simulate_failure") == "reserve_inventory":
            raise RuntimeError("inventory-service: out of stock")
        _inventory.apply(key, {"sku": sku, "quantity": quantity})
        return {"inventory_reserved": True}

    @saga.step(name="charge_payment", compensation=_refund_payment)
    def charge_payment(ctx: SagaContext) -> dict:
        key = ctx.idempotency_key("charge_payment")
        amount = ctx.data["amount"]
        if ctx.data.get("simulate_failure") == "charge_payment":
            raise RuntimeError("payment-service: card declined")
        _payment.apply(key, {"amount": amount, "customer_id": ctx.data["customer_id"]})
        return {"payment_charged": True}

    @saga.step(name="ship_order", compensation=_cancel_shipment)
    def ship_order(ctx: SagaContext) -> dict:
        key = ctx.idempotency_key("ship_order")
        if ctx.data.get("simulate_failure") == "ship_order":
            raise RuntimeError("shipping-service: no carrier available")
        _shipping.apply(key, {"address": ctx.data["address"]})
        return {"order_shipped": True}

    return saga


def _release_inventory(ctx: SagaContext) -> None:
    key = ctx.idempotency_key("reserve_inventory")
    _inventory.undo(key)


def _refund_payment(ctx: SagaContext) -> None:
    key = ctx.idempotency_key("charge_payment")
    _payment.undo(key)


def _cancel_shipment(ctx: SagaContext) -> None:
    key = ctx.idempotency_key("ship_order")
    _shipping.undo(key)
