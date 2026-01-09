#!/usr/bin/env python3
"""
Sealer Service (fail-open)

Responsibilities:
- Subscribe to GPIO state
- Control pusher3 / pusher4
- Consume intents from main via queue
- Handle accept / reject logic
- Publish 1Hz heartbeat for main
- Fail open on timeouts (never fatal)
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from typing import Dict, Any

import zmq
import zmq.asyncio


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] sealer: %(message)s",
)
log = logging.getLogger("sealer")


# =============================================================================
# CONFIG
# =============================================================================

GPIO_PUB_ADDR = "tcp://127.0.0.1:5556"
GPIO_REP_ADDR = "tcp://127.0.0.1:5557"
GPIO_TOPIC = b"gpio.status"

INTENT_PULL_ADDR = "tcp://127.0.0.1:5570"

HEARTBEAT_PUB_ADDR = "tcp://127.0.0.1:5571"
HEARTBEAT_TOPIC = b"sealer.heartbeat"

GPIO_HEALTH_TIMEOUT_SEC = 5.0
PROCESS_TIMEOUT_SEC = 10.0
PUSHER_TIME_SEC = 0.8
HEARTBEAT_HZ = 1.0


# Pins
SENSOR4 = "sensor4"
SENSOR5 = "sensor5"
PUSHER3 = "pusher3"
PUSHER4 = "pusher4"


# =============================================================================
# STATE
# =============================================================================

last_gpio_ts: float | None = None
gpio_state: Dict[str, int] = {}

intent_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()


# =============================================================================
# HELPERS
# =============================================================================


def rising(prev: int, curr: int) -> bool:
    return prev == 0 and curr == 1


async def gpio_set(req: zmq.asyncio.Socket, pin: str, value: bool) -> None:
    log.info("GPIO SET: %s -> %s", pin, value)
    await req.send_json({"cmd": "set", "pin": pin, "value": value})
    reply = await req.recv_json()
    if reply.get("ok") is not True:
        raise RuntimeError(f"GPIO set failed: {reply}")


async def wait_for(predicate, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.05)
    return False


async def force_pushers_safe(req: zmq.asyncio.Socket) -> None:
    await gpio_set(req, PUSHER3, False)
    await gpio_set(req, PUSHER4, False)


# =============================================================================
# WATCHDOG
# =============================================================================


async def gpio_watchdog() -> None:
    while True:
        await asyncio.sleep(0.5)
        if last_gpio_ts is None:
            continue
        if time.monotonic() - last_gpio_ts > GPIO_HEALTH_TIMEOUT_SEC:
            log.critical("GPIO heartbeat lost")
            sys.exit(1)


# =============================================================================
# GPIO LISTENER
# =============================================================================


async def gpio_listener(ctx: zmq.asyncio.Context) -> None:
    global last_gpio_ts, gpio_state

    sub = ctx.socket(zmq.SUB)
    sub.connect(GPIO_PUB_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, GPIO_TOPIC)

    while True:
        _, raw = await sub.recv_multipart()
        last_gpio_ts = time.monotonic()

        msg = json.loads(raw.decode())
        gpio_state = {pin: int(v["value"]) for pin, v in msg["pins"].items()}


# =============================================================================
# INTENT RECEIVER
# =============================================================================


async def intent_receiver(ctx: zmq.asyncio.Context) -> None:
    sock = ctx.socket(zmq.PULL)
    sock.bind(INTENT_PULL_ADDR)

    while True:
        raw = await sock.recv()
        intent = json.loads(raw.decode())
        await intent_queue.put(intent)
        log.info("Intent queued: %s", intent["action"])


# =============================================================================
# HEARTBEAT
# =============================================================================


async def heartbeat_publisher(ctx: zmq.asyncio.Context) -> None:
    pub = ctx.socket(zmq.PUB)
    pub.bind(HEARTBEAT_PUB_ADDR)

    period = 1.0 / HEARTBEAT_HZ

    while True:
        payload = {
            "ts_ms": int(time.time() * 1000),
            "alive": True,
        }

        await pub.send_multipart(
            [HEARTBEAT_TOPIC, json.dumps(payload).encode()]
        )

        await asyncio.sleep(period)


# =============================================================================
# CORE LOGIC
# =============================================================================


async def process_loop(ctx: zmq.asyncio.Context) -> None:
    req = ctx.socket(zmq.REQ)
    req.connect(GPIO_REP_ADDR)

    prev_s4 = 0

    while True:
        await asyncio.sleep(0.02)

        s4 = gpio_state.get(SENSOR4, 0)
        s5 = gpio_state.get(SENSOR5, 0)

        if not rising(prev_s4, s4):
            prev_s4 = s4
            continue

        # --------------------------------------------------------------
        # sensor4 ↑ → pop intent
        # --------------------------------------------------------------
        try:
            intent = intent_queue.get_nowait()
        except asyncio.QueueEmpty:
            log.error("No intent available → reject by default")
            intent = {"action": "reject"}

        action = intent["action"]
        log.info("Processing box: %s", action)

        try:
            if action == "reject":
                await wait_for(
                    lambda: gpio_state.get(SENSOR4, 0) == 0,
                    PROCESS_TIMEOUT_SEC,
                )
                await wait_for(
                    lambda: gpio_state.get(SENSOR5, 0) == 1,
                    PROCESS_TIMEOUT_SEC,
                )
                await wait_for(
                    lambda: gpio_state.get(SENSOR5, 0) == 0,
                    PROCESS_TIMEOUT_SEC,
                )

            else:  # accept
                await gpio_set(req, PUSHER4, True)

                ok = await wait_for(
                    lambda: gpio_state.get(SENSOR5, 0) == 1,
                    PROCESS_TIMEOUT_SEC,
                )

                if ok:
                    await gpio_set(req, PUSHER3, True)
                    await asyncio.sleep(PUSHER_TIME_SEC)

                await force_pushers_safe(req)

        except Exception as e:
            log.error("Processing error: %s", e)
            await force_pushers_safe(req)

        prev_s4 = s4


# =============================================================================
# ENTRY
# =============================================================================


async def _amain() -> None:
    ctx = zmq.asyncio.Context.instance()

    await asyncio.gather(
        gpio_listener(ctx),
        intent_receiver(ctx),
        heartbeat_publisher(ctx),
        gpio_watchdog(),
        process_loop(ctx),
    )


def main() -> None:
    try:
        asyncio.run(_amain())
    except Exception:
        log.critical("Sealer crashed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
