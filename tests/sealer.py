#!/usr/bin/env python3
"""
Sealer integration test (PUB-driven GPIO)

Scenario 1 (ACCEPT):
- pusher3 and pusher4 MUST activate at least once

Scenario 2 (REJECT):
- pusher3 and pusher4 MUST NEVER activate
"""

from __future__ import annotations

import json
import sys
import time
from typing import Dict

import zmq


# =============================================================================
# CONFIG
# =============================================================================

GPIO_PUB_ADDR = "tcp://127.0.0.1:5556"
GPIO_REP_ADDR = "tcp://127.0.0.1:5557"
INTENT_PUSH_ADDR = "tcp://127.0.0.1:5570"

STATUS_TOPIC = b"gpio.status"
RECV_TIMEOUT_MS = 2000

SENSOR4 = "sensor4"
SENSOR5 = "sensor5"
PUSHER3 = "pusher3"
PUSHER4 = "pusher4"


# =============================================================================
# ZMQ SETUP
# =============================================================================

ctx = zmq.Context.instance()

# REP (commands only)
rep = ctx.socket(zmq.REQ)
rep.connect(GPIO_REP_ADDR)
rep.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)

# SUB (authoritative state)
sub = ctx.socket(zmq.SUB)
sub.connect(GPIO_PUB_ADDR)
sub.setsockopt(zmq.SUBSCRIBE, STATUS_TOPIC)
sub.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)

# PUSH (intent)
push = ctx.socket(zmq.PUSH)
push.connect(INTENT_PUSH_ADDR)


# =============================================================================
# STATE TRACKING (from PUB)
# =============================================================================

pin_state: Dict[str, int] = {}


def drain_gpio_pub(timeout_s: float = 0.0) -> None:
    """
    Drain PUB socket and update pin_state.
    If timeout_s == 0, drain non-blocking.
    """
    end = time.time() + timeout_s

    while True:
        try:
            topic, raw = sub.recv_multipart(
                flags=zmq.NOBLOCK if timeout_s == 0 else 0
            )
        except zmq.Again:
            return

        msg = json.loads(raw.decode("utf-8"))
        pins = msg.get("pins", {})

        for name, info in pins.items():
            pin_state[name] = info["value"]

        if timeout_s == 0 or time.time() >= end:
            return


def read_pin(pin: str) -> bool:
    return bool(pin_state.get(pin, 0))


# =============================================================================
# COMMAND HELPERS
# =============================================================================


def gpio_set(pin: str, value: bool) -> None:
    rep.send_json({"cmd": "set", "pin": pin, "value": value})
    reply = rep.recv_json()
    assert reply.get("ok") is True, reply


def push_intent(action: str) -> None:
    payload = {
        "action": action,
        "ts_ms": int(time.time() * 1000),
    }
    push.send(json.dumps(payload).encode("utf-8"))


def wait(duration_s: float, on_tick=None) -> None:
    end = time.time() + duration_s
    while time.time() < end:
        drain_gpio_pub()
        if on_tick:
            on_tick()
        time.sleep(0.05)


# =============================================================================
# SCENARIOS
# =============================================================================


def scenario_1() -> None:
    """
    ACCEPT:
    - pusher3 and pusher4 must both activate at least once
    """

    print("\n=== Scenario 1: ACCEPT ===")

    p3_seen = False
    p4_seen = False

    def observe() -> None:
        nonlocal p3_seen, p4_seen
        if read_pin(PUSHER3):
            p3_seen = True
        if read_pin(PUSHER4):
            p4_seen = True

    # baseline
    gpio_set(SENSOR4, False)
    gpio_set(SENSOR5, False)
    wait(0.5)

    push_intent("accept")
    print("✓ intent ACCEPT pushed")

    gpio_set(SENSOR4, True)
    print("✓ sensor4 ↑")
    wait(1.0, observe)

    gpio_set(SENSOR4, False)
    print("✓ sensor4 ↓")
    wait(1.0, observe)

    gpio_set(SENSOR5, True)
    print("✓ sensor5 ↑")
    wait(1.0, observe)

    gpio_set(SENSOR5, False)
    print("✓ sensor5 ↓")
    wait(0.5, observe)

    if not p3_seen:
        raise AssertionError("FAIL: pusher3 never activated (ACCEPT)")
    if not p4_seen:
        raise AssertionError("FAIL: pusher4 never activated (ACCEPT)")

    print("✓ pusher3 activation observed")
    print("✓ pusher4 activation observed")
    print("✓ Scenario 1 PASS")


def scenario_2() -> None:
    """
    REJECT:
    - pushers must NEVER activate
    """

    print("\n=== Scenario 2: REJECT ===")

    def assert_off(phase: str) -> None:
        if read_pin(PUSHER3):
            raise AssertionError(f"FAIL: pusher3 ON during {phase}")
        if read_pin(PUSHER4):
            raise AssertionError(f"FAIL: pusher4 ON during {phase}")

    gpio_set(SENSOR4, False)
    gpio_set(SENSOR5, False)
    wait(0.5)
    assert_off("baseline")

    push_intent("reject")
    print("✓ intent REJECT pushed")
    assert_off("after intent")

    gpio_set(SENSOR4, True)
    print("✓ sensor4 ↑")
    wait(1.0, lambda: assert_off("sensor4 ↑"))

    gpio_set(SENSOR4, False)
    print("✓ sensor4 ↓")
    wait(1.0, lambda: assert_off("sensor4 ↓"))

    gpio_set(SENSOR5, True)
    print("✓ sensor5 ↑")
    wait(1.0, lambda: assert_off("sensor5 ↑"))

    gpio_set(SENSOR5, False)
    print("✓ sensor5 ↓")
    wait(0.5, lambda: assert_off("sensor5 ↓"))

    print("✓ Scenario 2 PASS (no pusher activation)")


def scenario_3() -> None:
    """
    Scenario 3: ACCEPT followed by REJECT (queue integrity)

    Sequence:
    - Push ACCEPT
    - Push REJECT
    - Box 1:
        sensor4 ↑  -> pusher4 MUST activate
        sensor4 ↓
        sensor5 ↑  -> pusher3 MUST activate
        pushers must go OFF
    - Box 2:
        sensor4 ↑
        sensor4 ↓
        sensor5 ↑
        sensor5 ↓
        NO pusher may activate
    """

    print("\n=== Scenario 3: ACCEPT then REJECT ===")

    # ------------------------------------------------------------------
    # State tracking
    # ------------------------------------------------------------------
    box = 1
    p4_seen_box1 = False
    p3_seen_box1 = False

    def observe_box1() -> None:
        nonlocal p4_seen_box1, p3_seen_box1
        if read_pin(PUSHER4):
            p4_seen_box1 = True
        if read_pin(PUSHER3):
            p3_seen_box1 = True

    def assert_no_pushers(phase: str) -> None:
        if read_pin(PUSHER3):
            raise AssertionError(
                f"FAIL: pusher3 ON during {phase} (box {box})"
            )
        if read_pin(PUSHER4):
            raise AssertionError(
                f"FAIL: pusher4 ON during {phase} (box {box})"
            )

    # ------------------------------------------------------------------
    # Baseline
    # ------------------------------------------------------------------
    gpio_set(SENSOR4, False)
    gpio_set(SENSOR5, False)
    wait(0.5)
    assert_no_pushers("baseline")

    # ------------------------------------------------------------------
    # Push intents (back-to-back)
    # ------------------------------------------------------------------
    push_intent("accept")
    push_intent("reject")
    print("✓ intents pushed: ACCEPT, then REJECT")

    # =========================
    # BOX 1 — ACCEPT
    # =========================
    box = 1

    gpio_set(SENSOR4, True)
    print("✓ box1 sensor4 ↑")
    wait(1.0, observe_box1)

    gpio_set(SENSOR4, False)
    print("✓ box1 sensor4 ↓")
    wait(1.0, observe_box1)

    gpio_set(SENSOR5, True)
    print("✓ box1 sensor5 ↑")
    wait(1.0, observe_box1)

    # pushers should retract
    wait(0.5, observe_box1)
    assert not read_pin(PUSHER3)
    assert not read_pin(PUSHER4)

    gpio_set(SENSOR5, False)
    print("✓ box1 sensor5 ↓")

    if not p4_seen_box1:
        raise AssertionError("FAIL: pusher4 never activated for box1 (ACCEPT)")
    if not p3_seen_box1:
        raise AssertionError("FAIL: pusher3 never activated for box1 (ACCEPT)")

    print("✓ box1 ACCEPT behavior verified")

    # =========================
    # BOX 2 — REJECT
    # =========================
    box = 2

    gpio_set(SENSOR4, True)
    print("✓ box2 sensor4 ↑")
    wait(1.0, lambda: assert_no_pushers("box2 sensor4 ↑"))

    gpio_set(SENSOR4, False)
    print("✓ box2 sensor4 ↓")
    wait(1.0, lambda: assert_no_pushers("box2 sensor4 ↓"))

    gpio_set(SENSOR5, True)
    print("✓ box2 sensor5 ↑")
    wait(1.0, lambda: assert_no_pushers("box2 sensor5 ↑"))

    gpio_set(SENSOR5, False)
    print("✓ box2 sensor5 ↓")
    wait(0.5, lambda: assert_no_pushers("box2 sensor5 ↓"))

    print("✓ Scenario 3 PASS (ACCEPT then REJECT, no leakage)")


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    # prime PUB
    drain_gpio_pub(0.5)

    scenario_1()
    scenario_2()
    scenario_3()

    sys.exit(0)


if __name__ == "__main__":
    main()
