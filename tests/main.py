#!/usr/bin/env python3
"""
Integration tests for main sequencer

Scenarios:
- Scenario 1: single box, repeated
- Scenario 2: queued boxes with upstream interference
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict, Callable

import zmq
import paho.mqtt.client as mqtt


# =============================================================================
# CONFIG
# =============================================================================

GPIO_REP_ADDR = "tcp://127.0.0.1:5557"

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC_STATE = "lucid/boxturner/state"

RECV_TIMEOUT_MS = 2000

# Pins
SENSOR1 = "sensor1"
SENSOR2 = "sensor2"
SENSOR3 = "sensor3"

PUSHER1 = "pusher1"
PUSHER2 = "pusher2"
ARM = "arm"
VACUUM = "vacuum"

ALL_PINS = [
    SENSOR1,
    SENSOR2,
    SENSOR3,
    PUSHER1,
    PUSHER2,
    ARM,
    VACUUM,
]

# Timing
STATE_TIMEOUT_SEC = 5.0
BOX_EXIT_SEC = 1.2


# =============================================================================
# ZMQ HELPERS
# =============================================================================

ctx = zmq.Context.instance()


def make_req() -> zmq.Socket:
    s = ctx.socket(zmq.REQ)
    s.connect(GPIO_REP_ADDR)
    s.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)
    return s


def gpio_set(pin: str, value: bool) -> None:
    req = make_req()
    req.send_json({"cmd": "set", "pin": pin, "value": value})
    reply = req.recv_json()
    assert reply["ok"] is True, reply


def reset_all_pins() -> None:
    for pin in ALL_PINS:
        gpio_set(pin, False)
    time.sleep(0.3)


# =============================================================================
# MQTT STATE TRACKING
# =============================================================================

mqtt_state: Dict[str, Any] | None = None


def on_mqtt_message(_client, _userdata, msg) -> None:
    global mqtt_state
    mqtt_state = json.loads(msg.payload.decode("utf-8"))


def wait_for_state(
    predicate: Callable[[Dict[str, Any]], bool],
    timeout: float = STATE_TIMEOUT_SEC,
) -> Dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if mqtt_state and predicate(mqtt_state):
            return mqtt_state
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for state condition")


# =============================================================================
# SCENARIO 1
# =============================================================================


def scenario_1() -> None:
    print("\n=== Scenario 1: single box + sealer path ===")

    reset_all_pins()
    wait_for_state(lambda s: s["running"] is False)
    print("✓ baseline idle")

    # --------------------------------------------------------------
    # sensor1 noise
    # --------------------------------------------------------------
    gpio_set(SENSOR1, True)
    print("✓ sensor1 ↑ (noise)")
    time.sleep(1.0)

    state = mqtt_state
    assert state["gpio"][PUSHER1] == 0
    assert state["gpio"][PUSHER2] == 0
    assert state["gpio"].get("pusher3", 0) == 0
    assert state["gpio"].get("pusher4", 0) == 0
    print("✓ no pushers actuated")

    gpio_set(SENSOR1, False)
    time.sleep(1.0)

    # --------------------------------------------------------------
    # sensor2 enters → pusher2 actuates
    # --------------------------------------------------------------
    gpio_set(SENSOR2, True)
    print("✓ sensor2 ↑")

    wait_for_state(lambda s: s["running"] is True)
    print("✓ running == True")

    gpio_set(SENSOR2, False)
    print("✓ sensor2 ↓")
    time.sleep(2.0)

    # --------------------------------------------------------------
    # sensor3 → arm open
    # --------------------------------------------------------------
    gpio_set(SENSOR3, True)
    print("✓ sensor3 ↑")
    time.sleep(1.0)

    wait_for_state(lambda s: s["gpio"][ARM] == 0)
    print("✓ arm opened")

    gpio_set(SENSOR3, False)
    time.sleep(1.0)

    # --------------------------------------------------------------
    # sealer path
    # --------------------------------------------------------------
    wait_for_state(lambda s: s["gpio"][ARM] == 0)
    time.sleep(0.5)  # allow intent propagation
    gpio_set("sensor4", True)
    print("✓ sensor4 ↑")
    time.sleep(1.0)

    wait_for_state(lambda s: s["gpio"].get("pusher4", 0) == 1)
    print("✓ pusher4 ON")

    gpio_set("sensor4", False)
    time.sleep(1.0)

    gpio_set("sensor5", True)
    print("✓ sensor5 ↑")
    time.sleep(0.5)

    wait_for_state(lambda s: s["gpio"].get("pusher3", 0) == 1)
    print("✓ pusher3 ON")

    gpio_set("sensor5", False)
    time.sleep(1.0)

    # --------------------------------------------------------------
    # final state
    # --------------------------------------------------------------
    state = mqtt_state
    assert state["gpio"][PUSHER1] == 0
    assert state["gpio"][PUSHER2] == 0
    assert state["gpio"].get("pusher3", 0) == 0
    assert state["gpio"].get("pusher4", 0) == 0
    assert state["running"] is False

    print("✓ all pushers OFF, running=False")
    print("✓ Scenario 1 PASSED")


# =============================================================================
# SCENARIO 2
# =============================================================================


def scenario_2() -> None:
    print("\n=== Scenario 2: queued boxes with upstream interference ===")

    # ------------------------------------------------------------------
    # Baseline
    # ------------------------------------------------------------------
    reset_all_pins()
    wait_for_state(lambda s: s["running"] is False)
    print("✓ baseline idle")

    # ------------------------------------------------------------------
    # Phase A: upstream noise
    # ------------------------------------------------------------------
    gpio_set(SENSOR1, True)
    print("✓ sensor1 ↑ (box2 upstream noise)")

    time.sleep(3.0)

    state = mqtt_state
    assert state["running"] is False, "running triggered prematurely"
    assert state["gpio"][PUSHER1] == 0, "pusher1 activated prematurely"
    assert state["gpio"][PUSHER2] == 0, "pusher2 activated prematurely"
    print("✓ no reaction to upstream-only box")

    gpio_set(SENSOR1, False)
    time.sleep(1.0)

    # ------------------------------------------------------------------
    # Phase B: box 1 enters pusher2
    # ------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    print("✓ sensor2 ↑ (box1)")
    wait_for_state(lambda s: s["running"] is True, timeout=5)
    print("✓ box1 held at pusher2")

    # ------------------------------------------------------------------
    # Phase C: box 2 arrives upstream during processing
    # ------------------------------------------------------------------
    gpio_set(SENSOR1, True)
    print("✓ sensor1 ↑ (box2 upstream)")

    wait_for_state(lambda s: s["gpio"][PUSHER1] == 1)
    print("✓ pusher1 holding box2")

    # ------------------------------------------------------------------
    # Phase D: box 1 released into measurement
    # ------------------------------------------------------------------
    gpio_set(SENSOR2, False)
    print("✓ sensor2 ↓ (box1 released)")

    wait_for_state(lambda s: s["gpio"][PUSHER1] == 0)
    print("✓ pusher1 released box2")

    gpio_set(SENSOR1, False)
    time.sleep(1.0)

    # ------------------------------------------------------------------
    # Phase E: box 2 queues at pusher2
    # ------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    print("✓ sensor2 ↑ (box2 reaches pusher2)")

    wait_for_state(lambda s: s["gpio"][PUSHER2] == 1)
    print("✓ pusher2 holding box2")

    time.sleep(1.0)

    # ------------------------------------------------------------------
    # Phase F: box 1 diagonal measurement
    # ------------------------------------------------------------------
    gpio_set(SENSOR3, True)
    print("✓ sensor3 ↑ (box1 diagonal)")

    wait_for_state(lambda s: s["gpio"][ARM] == 0)
    gpio_set(SENSOR3, False)
    print("✓ box1 exited diagonal")

    # ------------------------------------------------------------------
    # Phase G: box 1 sealer path
    # ------------------------------------------------------------------
    gpio_set("sensor4", True)
    print("✓ sensor4 ↑ (box1 at sealer)")

    wait_for_state(lambda s: s["gpio"].get("pusher4", 0) == 1)
    print("✓ pusher4 ON (box1)")

    gpio_set("sensor4", False)
    time.sleep(0.5)

    gpio_set("sensor5", True)
    print("✓ sensor5 ↑ (box1 sealed)")

    wait_for_state(lambda s: s["gpio"].get("pusher3", 0) == 1)
    print("✓ pusher3 ON (box1)")

    gpio_set("sensor5", False)
    time.sleep(1.0)

    # ------------------------------------------------------------------
    # Phase H: box 2 released and measured
    # ------------------------------------------------------------------
    wait_for_state(lambda s: s["gpio"][PUSHER2] == 0)
    print("✓ pusher2 released box2")

    gpio_set(SENSOR2, False)
    time.sleep(1.0)

    gpio_set(SENSOR3, True)
    print("✓ sensor3 ↑ (box2 diagonal)")

    wait_for_state(lambda s: s["gpio"][ARM] == 0)
    gpio_set(SENSOR3, False)
    print("✓ box2 exited diagonal")

    # ------------------------------------------------------------------
    # Phase I: box 2 sealer path
    # ------------------------------------------------------------------
    gpio_set("sensor4", True)
    print("✓ sensor4 ↑ (box2 at sealer)")

    wait_for_state(lambda s: s["gpio"].get("pusher4", 0) == 1)
    print("✓ pusher4 ON (box2)")

    gpio_set("sensor4", False)
    time.sleep(0.5)

    gpio_set("sensor5", True)
    print("✓ sensor5 ↑ (box2 sealed)")

    wait_for_state(lambda s: s["gpio"].get("pusher3", 0) == 1)
    print("✓ pusher3 ON (box2)")

    gpio_set("sensor5", False)
    time.sleep(1.0)

    # ------------------------------------------------------------------
    # Final state
    # ------------------------------------------------------------------
    wait_for_state(lambda s: s["running"] is False)

    state = mqtt_state
    assert state["gpio"][PUSHER1] == 0
    assert state["gpio"][PUSHER2] == 0
    assert state["gpio"].get("pusher3", 0) == 0
    assert state["gpio"].get("pusher4", 0) == 0

    print("✓ system returned to idle")
    print("✓ Scenario 2 PASSED")


def scenario_3() -> None:
    print("\n=== Scenario 3: missing box timeout ===")

    # ------------------------------------------------------------------
    # Baseline
    # ------------------------------------------------------------------
    reset_all_pins()
    wait_for_state(lambda s: s["running"] is False)
    print("✓ baseline idle")

    # ------------------------------------------------------------------
    # Phase A: upstream noise
    # ------------------------------------------------------------------
    gpio_set(SENSOR1, True)
    print("✓ sensor1 ↑ (upstream noise)")

    time.sleep(3.0)

    state = mqtt_state
    assert state["running"] is False, "running triggered prematurely"
    assert state["gpio"][PUSHER1] == 0
    assert state["gpio"][PUSHER2] == 0
    print("✓ no reaction to upstream-only box")

    gpio_set(SENSOR1, False)

    # ------------------------------------------------------------------
    # Phase B: box enters pusher2
    # ------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    print("✓ sensor2 ↑ (box enters pusher2)")

    wait_for_state(lambda s: s["running"] is True)
    print("✓ running == True")

    wait_for_state(lambda s: s["gpio"][PUSHER2] == 0, timeout=3)
    print("✓ pusher2 released (horizontal measured)")

    gpio_set(SENSOR2, False)
    print("✓ sensor2 ↓ (box released)")
    time.sleep(2)

    # ------------------------------------------------------------------
    # Phase C: box never reaches sensor3 → timeout
    # ------------------------------------------------------------------
    print("⏳ waiting for BOX_MISSING_TIMEOUT...")

    wait_for_state(lambda s: s["gpio"][ARM] == 0, timeout=11.0)
    print("✓ arm forced OFF by timeout")

    # ------------------------------------------------------------------
    # Phase D: reject intent reaches sealer
    # ------------------------------------------------------------------
    gpio_set("sensor4", True)
    print("✓ sensor4 ↑ (reject path)")

    time.sleep(1.0)

    state = mqtt_state
    assert state["gpio"].get("pusher4", 0) == 0, "pusher4 should NOT actuate"
    assert state["gpio"].get("pusher3", 0) == 0, "pusher3 should NOT actuate"
    print("✓ no accept pushers actuated")

    gpio_set("sensor4", False)
    time.sleep(0.5)

    gpio_set("sensor5", True)
    print("✓ sensor5 ↑ (reject completes)")
    time.sleep(0.5)

    gpio_set("sensor5", False)
    time.sleep(1.0)

    # ------------------------------------------------------------------
    # Final state
    # ------------------------------------------------------------------
    state = mqtt_state
    assert state["running"] is False
    assert state["gpio"][PUSHER1] == 0
    assert state["gpio"][PUSHER2] == 0
    assert state["gpio"].get("pusher3", 0) == 0
    assert state["gpio"].get("pusher4", 0) == 0

    print("✓ system idle after reject")
    print("✓ Scenario 3 PASSED")


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_mqtt_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC_STATE)
    client.loop_start()

    try:
        wait_for_state(lambda s: True)

        scenario_1()
        scenario_2()
        scenario_3()

    except Exception as e:
        print("✗ TEST FAILED:", e)
        sys.exit(1)

    finally:
        client.loop_stop()

    print("\n✓ ALL TESTS PASSED")
    sys.exit(0)


if __name__ == "__main__":
    main()
