import json
import os
import tempfile

from api.apple_wallet_card_settings.schemas import WalletCardSettings
from api.apple_wallet_card_settings.utils import create_default_apple_wallet_setting
from common.apple_wallet_service.impl.models import PassParamsModel
from common.apple_wallet_service.IWalletPassGeneratorService import (
    IWalletPassGeneratorService,
)
from common.s3_service.impl.S3Client import S3Client
from common.s3_service.models.S3SettingsModel import S3SettingsModel
from database.db import (
    apple_wallet_card_settings,
    contragents,
    database,
    loyality_cards,
    organizations,
)
from py_pkpass.models import Barcode, BarcodeFormat, Field, Pass, StoreCard
from sqlalchemy import select

# load_dotenv()


class WalletPassGeneratorService(IWalletPassGeneratorService):
    def __init__(self):
        self.__wallet_pass_folder = "apple_wallet_passes"
        self.__bucket_name = "5075293c-docs_generated"
        self.__s3_client = S3Client(
            S3SettingsModel(
                aws_access_key_id=os.getenv("S3_ACCESS"),
                aws_secret_access_key=os.getenv("S3_SECRET"),
                endpoint_url=os.getenv("S3_URL"),
            )
        )

    async def _get_image_from_s3_or_local(self, path: str) -> bytes:
        """
        Получает изображение из S3 или из локальной файловой системы.
        Если путь начинается с '/', то это локальный файл.
        Иначе это ключ в S3.
        """
        if path.startswith("/"):
            # Локальный файл
            with open(path, "rb") as f:
                return f.read()
        else:
            # Файл в S3
            return await self.__s3_client.get_object(self.__bucket_name, path)

    async def _generate_pkpass(self, pass_params: PassParamsModel) -> tuple[str, str]:
        # Create a store card pass type
        card_info = StoreCard()
        balance_field = Field("H1", str(pass_params.balance), "Баланс")
        balance_field.changeMessage = "Ваш баланс %@"
        cashback_field = Field("H2", str(pass_params.cashback_persent) + "%", "Бонусы")
        cashback_field.changeMessage = "Ваш кешбек теперь %@"

        ad_field = Field("B1", pass_params.advertisement, "Акции")
        ad_field.changeMessage = "%@"

        card_info.headerFields.append(balance_field)
        card_info.headerFields.append(cashback_field)
        card_info.backFields.append(ad_field)

        card_info.addSecondaryField("S1", pass_params.contragent_name, "ВЛАДЕЛЕЦ КАРТЫ")
        card_info.addSecondaryField("S2", pass_params.card_number, "НОМЕР КАРТЫ")

        # Create the Pass object with the required identifiers
        passfile = Pass(
            card_info,
            passTypeIdentifier=os.getenv("APPLE_PASS_TYPE_ID"),
            organizationName=pass_params.organization_name,
            teamIdentifier=os.getenv("APPLE_TEAM_ID"),
        )

        # Set required pass information
        passfile.serialNumber = str(pass_params.serial_number)
        passfile.description = pass_params.description

        # Add a barcode - all supported formats: PDF417, QR, AZTEC, CODE128
        passfile.barcode = Barcode(
            message=pass_params.card_number,
            altText=pass_params.barcode_message,
            format=BarcodeFormat.QR,
        )

        passfile.webServiceURL = f'https://{os.getenv("APP_URL")}/api/v1'
        passfile.authenticationToken = pass_params.auth_token

        # Optional: Set colors
        passfile.backgroundColor = pass_params.colors.backgroundColor
        passfile.foregroundColor = pass_params.colors.foregroundColor
        passfile.labelColor = pass_params.colors.labelColor

        passfile.logoText = pass_params.logo_text

        passfile.locations = [i.dict() for i in pass_params.locations]

        # Получаем изображения из S3 и создаем временные файлы
        icon_data = await self._get_image_from_s3_or_local(pass_params.icon_path)
        logo_data = await self._get_image_from_s3_or_local(pass_params.logo_path)
        strip_data = await self._get_image_from_s3_or_local(pass_params.strip_path)

        # Создаем временные файлы для изображений
        with (
            tempfile.NamedTemporaryFile(delete=False, suffix=".png") as icon_tmp,
            tempfile.NamedTemporaryFile(delete=False, suffix=".png") as logo_tmp,
            tempfile.NamedTemporaryFile(delete=False, suffix=".png") as strip_tmp,
        ):

            icon_tmp.write(icon_data)
            logo_tmp.write(logo_data)
            strip_tmp.write(strip_data)

            icon_tmp_path = icon_tmp.name
            logo_tmp_path = logo_tmp.name
            strip_tmp_path = strip_tmp.name

        # Создаем временный файл для pkpass
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pkpass") as pkpass_tmp:
            pkpass_tmp_path = pkpass_tmp.name

        try:
            # Including the icon and logo is necessary for the passbook to be valid
            passfile.addFile("icon.png", open(icon_tmp_path, "rb"))
            passfile.addFile("icon@2x.png", open(icon_tmp_path, "rb"))
            passfile.addFile("icon@3x.png", open(icon_tmp_path, "rb"))
            passfile.addFile("logo.png", open(logo_tmp_path, "rb"))
            passfile.addFile("strip@2x.png", open(strip_tmp_path, "rb"))

            # passfile.expirationDate = pass_params.exp_date.isoformat() if pass_params.exp_date else None

            # Create and output the Passbook file (.pkpass) во временный файл
            password = os.getenv("PKPASS_PASSWORD")
            passfile.create(
                os.getenv("APPLE_CERTIFICATE_PATH"),
                os.getenv("APPLE_KEY_PATH"),
                os.getenv("APPLE_WWDR_PATH"),
                password,
                pkpass_tmp_path,
            )

            # Загружаем pkpass файл в S3
            with open(pkpass_tmp_path, "rb") as f:
                pkpass_bytes = f.read()

            s3_key = f"{self.__wallet_pass_folder}/{pass_params.serial_number}.pkpass"
            await self.__s3_client.upload_file_object(
                self.__bucket_name, s3_key, pkpass_bytes
            )

        finally:
            # Удаляем временные файлы
            os.unlink(icon_tmp_path)
            os.unlink(logo_tmp_path)
            os.unlink(strip_tmp_path)
            os.unlink(pkpass_tmp_path)

        return self.get_card_path_and_name(pass_params.serial_number)

    def get_card_s3_key(self, card_number: str) -> str:
        """Возвращает S3 ключ для pkpass файла"""
        return f"{self.__wallet_pass_folder}/{card_number}.pkpass"

    def get_card_path_and_name(self, card_number: str) -> tuple[str, str]:
        """
        Deprecated: Использовать get_card_s3_key() для S3
        Возвращает S3 ключ и имя файла
        """
        return self.get_card_s3_key(card_number), f"{card_number}.pkpass"

    async def get_pkpass_from_s3(self, card_number: str) -> bytes:
        """Получает pkpass файл из S3"""
        s3_key = self.get_card_s3_key(card_number)
        return await self.__s3_client.get_object(self.__bucket_name, s3_key)

    async def pkpass_exists_in_s3(self, card_number: str) -> bool:
        """Проверяет существование pkpass файла в S3"""
        try:
            s3_key = self.get_card_s3_key(card_number)
            await self.__s3_client.get_object(self.__bucket_name, s3_key)
            return True
        except Exception:
            return False

    async def update_pass(self, card_id: int) -> tuple[str, str]:
        query = (
            select(
                loyality_cards.c.id,
                loyality_cards.c.card_number,
                contragents.c.name.label("contragent_name"),
                organizations.c.short_name.label("organization_name"),
                loyality_cards.c.cashback_percent,
                loyality_cards.c.balance,
                loyality_cards.c.end_period,
                loyality_cards.c.cashbox_id,
                loyality_cards.c.apple_wallet_advertisement,
            )
            .select_from(
                loyality_cards.join(
                    contragents, contragents.c.id == loyality_cards.c.contragent_id
                ).join(
                    organizations,
                    organizations.c.id == loyality_cards.c.organization_id,
                )
            )
            .where(loyality_cards.c.id == int(card_id))
        )
        loyality_card_db = await database.fetch_one(query)

        card_settings_query = select(apple_wallet_card_settings.c.data).where(
            apple_wallet_card_settings.c.cashbox_id == loyality_card_db.cashbox_id
        )
        card_settings_db = await database.fetch_one(card_settings_query)

        if card_settings_db is None:
            card_settings = await create_default_apple_wallet_setting(
                loyality_card_db.cashbox_id
            )
        else:
            card_settings = WalletCardSettings(**json.loads(card_settings_db.data))

        path, filename = await self._generate_pkpass(
            PassParamsModel(
                serial_number=loyality_card_db.id,
                card_number=loyality_card_db.card_number,
                contragent_name=loyality_card_db.contragent_name,
                organization_name=loyality_card_db.organization_name,
                description=card_settings.description,
                barcode_message=card_settings.barcode_message,
                colors=card_settings.colors,
                icon_path=card_settings.icon_path,
                logo_path=card_settings.logo_path,
                strip_path=card_settings.strip_path,
                cashback_persent=loyality_card_db.cashback_percent,
                locations=card_settings.locations,
                logo_text=card_settings.logo_text,
                balance=loyality_card_db.balance,
                exp_date=loyality_card_db.end_period,
                advertisement=loyality_card_db.apple_wallet_advertisement,
            )
        )

        return path, filename
