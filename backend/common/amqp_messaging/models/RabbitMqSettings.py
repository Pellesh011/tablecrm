from pydantic import BaseModel


class RabbitMqSettings(BaseModel):
    rabbitmq_host: str
    rabbitmq_user: str
    rabbitmq_pass: str
    rabbitmq_port: int
    rabbitmq_vhost: str
