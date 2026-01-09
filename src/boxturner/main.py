#!/usr/bin/env python3
"""
Main Sequencer Service (fail-fast)
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import sys
import time
from typing import Any, Dict

import aiomqtt
import zmq
import zmq.asyncio


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] main: %(message)s",
)
log = logging.getLogger("main")


# =============================================================================
# ZMQ CONFIG
# =============================================================================

GPIO_PUB_ADDR = "tcp://127.0.0.1:5556"
GPIO_STATUS_TOPIC = b"gpio.status"
GPIO_REP_ADDR = "tcp://127.0.0.1:5557"

PUSHER_PUB_ADDR = "tcp://127.0.0.1:5560"
PUSHER_HEARTBEAT_TOPIC = b"pusher.heartbeat"

SEALER_PUB_ADDR = "tcp://127.0.0.1:5571"
SEALER_HEARTBEAT_TOPIC = b"sealer.heartbeat"

SEALER_INTENT_ADDR = "tcp://127.0.0.1:5570"


# =============================================================================
# MQTT CONFIG
# =============================================================================

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC_STATE = "lucid/boxturner/state"


# =============================================================================
# TIMEOUTS
# =============================================================================

HEALTH_TIMEOUT_SEC = 5.0
STARTUP_GRACE_SEC = 5.0
BOX_MISSING_TIMEOUT_SEC = 10.0


# =============================================================================
# TIMING CONFIG
# =============================================================================

VACUUM_HOLD_DELAY_SEC = 1.0
POST_MEASUREMENT_DELAY_SEC = 0.5
RUNNING_RESET_DELAY_SEC = 1.0
PUSHER2_HOLD_TIME_SEC = 1.0


# =============================================================================
# PINS
# =============================================================================

SENSOR2 = "sensor2"
SENSOR3 = "sensor3"

PUSHER2 = "pusher2"
ARM = "arm"
VACUUM = "vacuum"


# =============================================================================
# STATE
# =============================================================================

start_ts = time.monotonic()

last_gpio_values: Dict[str, int] | None = None
last_published_state: Dict[str, Any] | None = None

horizontal: int | None = None
diagonal: int | None = None
running: bool = False

last_gpio_seen_ts: float | None = None
last_pusher_seen_ts: float | None = None
last_sealer_seen_ts: float | None = None

health_main: bool = True
health_gpio: bool = True
health_pusher: bool = True
health_sealer: bool = True

box_expected_by_ts: float | None = None


# =============================================================================
# HELPERS
# =============================================================================


def rising_edge(prev: int, curr: int) -> bool:
    return prev == 0 and curr == 1


def falling_edge(prev: int, curr: int) -> bool:
    return prev == 1 and curr == 0


def measure_horizontal() -> int | None:
    if random.random() < 0.05:
        log.error("Horizontal measurement failed")
        return None
    val = random.randint(10, 100)
    log.info("Measured horizontal = %d", val)
    return val


def measure_diagonal() -> int | None:
    if random.random() < 0.05:
        log.error("Diagonal measurement failed")
        return None
    val = random.randint(10, 100)
    log.info("Measured diagonal = %d", val)
    return val


async def gpio_set(sock: zmq.asyncio.Socket, pin: str, value: bool) -> None:
    log.info("GPIO SET: %s -> %s", pin, value)
    await sock.send_json({"cmd": "set", "pin": pin, "value": value})
    reply = await sock.recv_json()
    if reply.get("ok") is not True:
        raise RuntimeError(f"GPIO set failed: {reply}")


async def push_intent(sock: zmq.asyncio.Socket, action: str) -> None:
    payload = {
        "action": action,
        "ts_ms": int(time.time() * 1000),
    }
    await sock.send(json.dumps(payload).encode())
    log.info("INTENT PUSHED: %s", action)


async def wait_for_sensor3_drop() -> None:
    while last_gpio_values and last_gpio_values.get(SENSOR3, 0) == 1:
        await asyncio.sleep(0.05)


async def reject_and_reset(
    req: zmq.asyncio.Socket,
    intent_sock: zmq.asyncio.Socket,
) -> None:
    global running, horizontal, diagonal

    log.warning("Rejecting box")

    await push_intent(intent_sock, "reject")
    await gpio_set(req, VACUUM, False)
    await gpio_set(req, ARM, False)

    await wait_for_sensor3_drop()
    await asyncio.sleep(RUNNING_RESET_DELAY_SEC)

    running = False
    horizontal = None
    diagonal = None


# =============================================================================
# MQTT STATE PUBLISHING
# =============================================================================


async def publish_state_if_changed(
    mqtt: aiomqtt.Client,
    gpio_values: Dict[str, int],
) -> None:
    global last_published_state

    compare_state = {
        "running": running,
        "gpio": gpio_values,
        "health": {
            "main": health_main,
            "gpio": health_gpio,
            "pusher": health_pusher,
            "sealer": health_sealer,
        },
    }

    if compare_state == last_published_state:
        return

    payload = {
        "ts_ms": int(time.time() * 1000),
        **compare_state,
    }

    await mqtt.publish(
        MQTT_TOPIC_STATE,
        json.dumps(payload, separators=(",", ":")),
        qos=0,
        retain=True,
    )

    last_published_state = compare_state


# =============================================================================
# WATCHDOGS
# =============================================================================


async def health_watchdog(mqtt: aiomqtt.Client) -> None:
    global health_gpio, health_pusher, health_sealer, health_main

    while True:
        await asyncio.sleep(0.5)
        now = time.monotonic()
        past_grace = now - start_ts > STARTUP_GRACE_SEC

        if past_grace:
            if last_gpio_seen_ts is None:
                health_gpio = False
            if last_pusher_seen_ts is None:
                health_pusher = False
            if last_sealer_seen_ts is None:
                health_sealer = False

        if last_gpio_seen_ts and now - last_gpio_seen_ts > HEALTH_TIMEOUT_SEC:
            health_gpio = False
        if (
            last_pusher_seen_ts
            and now - last_pusher_seen_ts > HEALTH_TIMEOUT_SEC
        ):
            health_pusher = False
        if (
            last_sealer_seen_ts
            and now - last_sealer_seen_ts > HEALTH_TIMEOUT_SEC
        ):
            health_sealer = False

        if not health_gpio or not health_pusher or not health_sealer:
            health_main = False
            log.critical("Health failure detected, shutting down")
            await publish_state_if_changed(mqtt, last_gpio_values or {})
            sys.exit(1)


async def box_missing_watchdog(
    mqtt: aiomqtt.Client,
    gpio_req: zmq.asyncio.Socket,
    intent_sock: zmq.asyncio.Socket,
) -> None:
    global box_expected_by_ts, running

    while True:
        await asyncio.sleep(0.1)

        if box_expected_by_ts is None:
            continue

        if time.monotonic() <= box_expected_by_ts:
            continue

        log.error("Box missing: sensor3 timeout")

        await push_intent(intent_sock, "reject")
        await gpio_set(gpio_req, ARM, False)
        await asyncio.sleep(RUNNING_RESET_DELAY_SEC)

        running = False
        box_expected_by_ts = None

        await publish_state_if_changed(mqtt, last_gpio_values or {})


# =============================================================================
# MEASUREMENT SEQUENCE
# =============================================================================


async def start_measurement_sequence(
    req: zmq.asyncio.Socket,
    mqtt: aiomqtt.Client,
) -> None:
    global running, horizontal, box_expected_by_ts

    await gpio_set(req, PUSHER2, True)

    if running:
        return

    running = True
    await publish_state_if_changed(mqtt, last_gpio_values or {})

    await gpio_set(req, ARM, True)
    await asyncio.sleep(PUSHER2_HOLD_TIME_SEC)
    await gpio_set(req, PUSHER2, False)

    horizontal = measure_horizontal()
    box_expected_by_ts = time.monotonic() + BOX_MISSING_TIMEOUT_SEC


# =============================================================================
# GPIO LISTENER
# =============================================================================


async def gpio_listener(
    ctx: zmq.asyncio.Context,
    mqtt: aiomqtt.Client,
) -> None:
    global last_gpio_values, diagonal, horizontal, running
    global last_gpio_seen_ts, box_expected_by_ts

    sub = ctx.socket(zmq.SUB)
    sub.connect(GPIO_PUB_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, GPIO_STATUS_TOPIC)

    req = ctx.socket(zmq.REQ)
    req.connect(GPIO_REP_ADDR)

    intent_sock = ctx.socket(zmq.PUSH)
    intent_sock.connect(SEALER_INTENT_ADDR)

    watchdog = asyncio.create_task(
        box_missing_watchdog(mqtt, req, intent_sock)
    )

    try:
        while True:
            _, raw = await sub.recv_multipart()
            last_gpio_seen_ts = time.monotonic()

            msg = json.loads(raw.decode())
            gpio_values = {
                pin: int(v["value"]) for pin, v in msg["pins"].items()
            }

            s2 = gpio_values[SENSOR2]
            s3 = gpio_values[SENSOR3]

            if last_gpio_values is None:
                last_gpio_values = gpio_values.copy()
                await publish_state_if_changed(mqtt, gpio_values)
                continue

            if rising_edge(last_gpio_values[SENSOR2], s2):
                await start_measurement_sequence(req, mqtt)

            if rising_edge(last_gpio_values[SENSOR3], s3):
                box_expected_by_ts = None

                if horizontal is None:
                    await reject_and_reset(req, intent_sock)
                else:
                    diagonal = measure_diagonal()
                    if diagonal is None:
                        await reject_and_reset(req, intent_sock)
                    elif diagonal > horizontal:
                        await push_intent(intent_sock, "accept")
                        await gpio_set(req, VACUUM, True)
                        await gpio_set(req, ARM, False)
                        await asyncio.sleep(VACUUM_HOLD_DELAY_SEC)
                        await gpio_set(req, VACUUM, False)
                    else:
                        await push_intent(intent_sock, "accept")
                        await asyncio.sleep(POST_MEASUREMENT_DELAY_SEC)
                        await gpio_set(req, ARM, False)

            if falling_edge(last_gpio_values[SENSOR3], s3):
                await asyncio.sleep(RUNNING_RESET_DELAY_SEC)
                running = False
                horizontal = None
                diagonal = None
                await publish_state_if_changed(mqtt, gpio_values)

                if s2 == 1:
                    await start_measurement_sequence(req, mqtt)

            last_gpio_values = gpio_values.copy()
            await publish_state_if_changed(mqtt, gpio_values)

    finally:
        watchdog.cancel()
        await asyncio.gather(watchdog, return_exceptions=True)


# =============================================================================
# HEARTBEAT LISTENERS
# =============================================================================


async def pusher_listener(ctx: zmq.asyncio.Context) -> None:
    global last_pusher_seen_ts

    sub = ctx.socket(zmq.SUB)
    sub.connect(PUSHER_PUB_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, PUSHER_HEARTBEAT_TOPIC)

    while True:
        await sub.recv_multipart()
        last_pusher_seen_ts = time.monotonic()


async def sealer_listener(ctx: zmq.asyncio.Context) -> None:
    global last_sealer_seen_ts

    sub = ctx.socket(zmq.SUB)
    sub.connect(SEALER_PUB_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, SEALER_HEARTBEAT_TOPIC)

    while True:
        await sub.recv_multipart()
        last_sealer_seen_ts = time.monotonic()


# =============================================================================
# ENTRY
# =============================================================================


async def _amain() -> None:
    ctx = zmq.asyncio.Context.instance()

    async with aiomqtt.Client(MQTT_HOST, MQTT_PORT) as mqtt:
        await asyncio.gather(
            gpio_listener(ctx, mqtt),
            pusher_listener(ctx),
            sealer_listener(ctx),
            health_watchdog(mqtt),
        )


def main() -> None:
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        log.critical("Interrupted")
        sys.exit(1)
    except Exception:
        log.critical("Fatal crash", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
