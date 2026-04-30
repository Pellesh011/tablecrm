from abc import ABC, abstractmethod


class IWalletNotificationService(ABC):
    @abstractmethod
    async def renew_pass(self, push_token: str): ...

    @abstractmethod
    async def ask_update_pass(self, card_id: int) -> bool: ...
