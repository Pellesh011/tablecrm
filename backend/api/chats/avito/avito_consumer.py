import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

import aio_pika
from api.chats import crud

logger = logging.getLogger(__name__)

AVITO_MESSAGES_QUEUE = "avito.messages"
AVITO_MESSAGES_EXCHANGE = "avito"
AVITO_MESSAGES_ROUTING_KEY = "avito.messages.*"


class AvitoMessageConsumer:
    def __init__(self):
        self.connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self._consumer_task: Optional[asyncio.Task] = None
        self.is_running = False

    async def start(self):
        try:
            self.is_running = True
            self._consumer_task = asyncio.create_task(self._consume_messages())
        except Exception as e:
            logger.error(f"Failed to start Avito consumer: {e}")
            raise

    async def stop(self):
        self.is_running = False
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error awaiting Avito consumer task: {e}")
            finally:
                self._consumer_task = None

        if self.connection:
            try:
                await self.connection.close()
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

    async def _consume_messages(self):
        while self.is_running:
            try:
                if self.connection and not self.connection.is_closed:
                    await self.connection.close()

                self.connection = await aio_pika.connect_robust(
                    host=os.getenv("RABBITMQ_HOST", "localhost"),
                    port=int(os.getenv("RABBITMQ_PORT", "5672")),
                    login=os.getenv("RABBITMQ_USER", "guest"),
                    password=os.getenv("RABBITMQ_PASS", "guest"),
                    virtualhost=os.getenv("RABBITMQ_VHOST", "/"),
                    timeout=10,
                )

                async with self.connection:
                    channel = await self.connection.channel()
                    await channel.set_qos(prefetch_count=10)

                    exchange = await channel.declare_exchange(
                        name=AVITO_MESSAGES_EXCHANGE, type="topic", durable=True
                    )

                    queue = await channel.declare_queue(
                        name=AVITO_MESSAGES_QUEUE, durable=True
                    )

                    await queue.bind(exchange, routing_key=AVITO_MESSAGES_ROUTING_KEY)

                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            if not self.is_running:
                                break
                            async with message.process():
                                await self._process_message(message)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in message consumer loop: {e}")
                if self.is_running:
                    await asyncio.sleep(5)

    async def _process_message(self, message):
        try:
            payload = json.loads(message.body.decode())

            message_type = payload.get("message_type")
            chat_id = payload.get("chat_id")

            if message_type == "message_received":
                await self._handle_message_received(payload)
            elif message_type == "message_status":
                await self._handle_message_status(payload)
            elif message_type == "chat_closed":
                await self._handle_chat_closed(payload)
            else:
                logger.warning(f"Unknown message type: {message_type}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message JSON: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            raise

    async def _handle_message_received(self, payload: Dict[str, Any]):
        try:
            chat_id = payload.get("chat_id")
            external_message_id = payload.get("external_message_id")

            if not chat_id:
                logger.warning("Message received without chat_id")
                return

            if payload.get("message_id"):
                try:
                    await crud.update_message(payload["message_id"], status="DELIVERED")
                except Exception as e:
                    logger.warning(f"Failed to update message status: {e}")

        except Exception as e:
            logger.error(f"Error handling message received: {e}")
            raise

    async def _handle_message_status(self, payload: Dict[str, Any]):
        try:
            message_id = payload.get("message_id")
            status = payload.get("status")

            if not message_id or not status:
                logger.warning("Status update without message_id or status")
                return

            await crud.update_message(message_id, status=status)

        except Exception as e:
            logger.error(f"Error handling message status: {e}")
            raise

    async def _handle_chat_closed(self, payload: Dict[str, Any]):
        try:
            chat_id = payload.get("chat_id")

            if not chat_id:
                logger.warning("Chat closed without chat_id")
                return

            await crud.update_chat(
                chat_id, status="CLOSED", last_message_time=datetime.utcnow()
            )

        except Exception as e:
            logger.error(f"Error handling chat closed: {e}")
            raise


avito_consumer = AvitoMessageConsumer()


async def start_avito_consumer():
    try:
        await avito_consumer.start()
    except Exception as e:
        logger.error(f"Failed to start Avito consumer: {e}")
        raise


async def stop_avito_consumer():
    await avito_consumer.stop()
