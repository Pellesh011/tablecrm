from abc import ABC, abstractmethod

from common.apple_wallet_service.impl.models import PassParamsModel


class IWalletPassGeneratorService(ABC):
    @abstractmethod
    async def _generate_pkpass(
        self, pass_params: PassParamsModel
    ) -> tuple[str, str]: ...

    @abstractmethod
    def get_card_path_and_name(self, card_number: str) -> tuple[str, str]: ...

    @abstractmethod
    async def update_pass(self, card_id: int) -> tuple[str, str]: ...
