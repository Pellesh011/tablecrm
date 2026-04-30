import asyncio
import traceback
from asyncio import create_task
from json import loads
from typing import Dict, List, Type

import aio_pika
import aiormq
from aio_pika import IncomingMessage, Message
from aio_pika.abc import AbstractQueue, AbstractRobustChannel

from ...amqp_channels.core.IRabbitChannel import IRabbitChannel
from ...common.core.EventHandler import IEventHandler
from ...common.core.IRabbitMessaging import IRabbitMessaging
from ...models.BaseModelMessage import BaseModelMessage
from ...models.BasePublishMessage import BasePublishMessage
from .models.QueueSettingsModel import QueueSettingsModel


class RabbitMessagingImpl(IRabbitMessaging):

    def __init__(self, channel: IRabbitChannel):
        self.__channel = channel
        self.__handlers: Dict[str, IEventHandler] = {}
        self.__consumer_configs: Dict[str, QueueSettingsModel] = {}
        self.__reconnect_tasks: Dict[str, asyncio.Task] = {}
        self.__close_callback_tasks: set[asyncio.Task] = set()
        self.__is_closing = False

    async def install(
        self, queues_settings: List[QueueSettingsModel]
    ) -> List[AbstractRobustChannel]:
        channels = []
        for queue_settings in queues_settings:
            self.__consumer_configs[queue_settings.queue_name] = queue_settings
            channel = await self._register_consumer(queue_settings)
            channels.append(channel)
        return channels

    async def _register_consumer(
        self, queue_settings: QueueSettingsModel
    ) -> AbstractRobustChannel:
        consumption_channel: AbstractRobustChannel = (
            await self.__channel.get_consumption_channel()
        )
        await consumption_channel.set_qos(prefetch_count=queue_settings.prefetch_count)
        queue: AbstractQueue = await consumption_channel.declare_queue(
            queue_settings.queue_name,
            auto_delete=False,
            durable=True,
            arguments={"x-max-priority": 10},
        )
        await queue.consume(callback=self.__amqp_event_message_consumer, no_ack=False)

        consumption_channel.close_callbacks.add(
            lambda ch, exc: self._schedule_channel_closed_task(queue_settings, ch, exc)
        )
        print(
            f"Consumer для очереди '{queue_settings.queue_name}' успешно зарегистрирован."
        )
        return consumption_channel

    def _schedule_channel_closed_task(
        self,
        queue_settings: QueueSettingsModel,
        closed_channel: AbstractRobustChannel,
        exc,
    ) -> None:
        if self.__is_closing:
            return

        task = asyncio.create_task(
            self._on_channel_closed(queue_settings, closed_channel, exc)
        )
        self.__close_callback_tasks.add(task)
        task.add_done_callback(self.__close_callback_tasks.discard)

    async def _on_channel_closed(
        self,
        queue_settings: QueueSettingsModel,
        closed_channel: AbstractRobustChannel,
        exc,
    ):
        """
        Обработчик закрытия канала. Логирует причину закрытия (если exc не None) и пытается восстановить consumer для заданной очереди.
        """
        queue_name = queue_settings.queue_name
        if self.__is_closing:
            return

        if exc:
            print(f"Канал для очереди '{queue_name}' закрылся с ошибкой: {exc}")
        else:
            print(f"Канал для очереди '{queue_name}' закрыт без ошибки.")

        print(
            f"Начинается попытка переподключения consumer-а для очереди '{queue_name}'."
        )

        # Если для этой очереди уже запущена задача переподключения — ничего не делаем
        if (
            queue_name in self.__reconnect_tasks
            and not self.__reconnect_tasks[queue_name].done()
        ):
            print(f"Задача переподключения для очереди '{queue_name}' уже выполняется.")
            return

        async def reconnect():
            delay = 5  # начальная задержка
            max_delay = 60  # максимальная задержка
            while True:
                try:
                    new_channel = await self._register_consumer(queue_settings)
                    print(
                        f"Consumer для очереди '{queue_name}' успешно переподключен на новом канале."
                    )
                    break
                except Exception as e:
                    print(
                        f"Ошибка при переподключении consumer-а для очереди '{queue_name}': {e}"
                    )
                    print(traceback.format_exc())
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, max_delay)

        task = asyncio.create_task(reconnect())
        self.__reconnect_tasks[queue_name] = task
        await task  # Ждём успешного переподключения

    async def publish_to_queue(
        self, queue_name: str, body: bytes, durable: bool = True
    ):
        channel = await self.__channel.get_publication_channel()
        await channel.default_exchange.publish(
            aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=queue_name,
        )

    async def publish(
        self,
        message: BaseModelMessage,
        routing_key: str,
        priority: int = None,
        ttl_expiration: int = None,
    ) -> aiormq.abc.ConfirmationFrameType:
        publication_channel = await self.__channel.get_publication_channel()
        publication_channel.publisher_confirms = True
        queue: aio_pika.abc.AbstractQueue = await publication_channel.declare_queue(
            routing_key,
            auto_delete=False,
            durable=True,
            arguments={"x-max-priority": 10},
        )

        return await publication_channel.default_exchange.publish(
            Message(
                body=BasePublishMessage(
                    event_name=message.__class__.__name__, event=message
                )
                .json()
                .encode("utf-8"),
                content_type="application/json",
                content_encoding="utf-8",
                message_id=message.message_id.hex,
                delivery_mode=aio_pika.abc.DeliveryMode.PERSISTENT,
                app_id="TableCRM",
                priority=priority,
                expiration=ttl_expiration,
            ),
            routing_key=routing_key,
        )

    async def subscribe(
        self, event_type: Type[BaseModelMessage], event_handler: IEventHandler
    ):
        if self.__handlers.get(event_type.__name__):
            raise Exception(f"Handler {event_type.__name__} is already registered")

        self.__handlers[event_type.__name__] = event_handler

    async def __amqp_event_message_consumer(self, message: IncomingMessage):
        async with message.process(ignore_processed=True):
            try:
                message_json = loads(message.body.decode("utf-8"))
            except Exception as error:
                print(f"Произошла ошибка при валидации сообщения {error}")
                return

            try:
                event_handler = self.__handlers[message_json["event_name"]]
                event = message_json["event"]
            except KeyError as error:
                print(
                    f"Неправильный формат сообщения или указанный хендлер не найден {error}"
                )
                return

            try:
                await create_task(event_handler(event, message))
            except Exception as e:
                print("".join(traceback.format_exception(type(e), e, e.__traceback__)))

    async def close(self) -> None:
        self.__is_closing = True

        for task in list(self.__close_callback_tasks):
            if not task.done():
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            finally:
                self.__close_callback_tasks.discard(task)

        for queue_name, task in list(self.__reconnect_tasks.items()):
            if not task.done():
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            finally:
                self.__reconnect_tasks.pop(queue_name, None)

        await self.__channel.close()
