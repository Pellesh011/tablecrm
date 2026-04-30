import json
import re

import aiohttp


def normalize_number(raw: str) -> str:
    return "".join(filter(str.isdigit, raw))


def find_counterparty_account_number(text: str, control_number) -> str:
    # Найти строку, содержащую "Корр. счет"
    match = re.search(r"(\d{20})", text)
    if match:
        text = text.replace(match.group(1), "")
        if match.group(1)[-3:] != control_number:
            return match.group(1), text
        else:
            return find_counterparty_account_number(text, control_number)
    else:

        return None, text


def reverse_string_order(text):

    strings = text.splitlines()
    reversed_strings = strings[::-1]
    return "\n".join(reversed_strings)


def convert_unicode_to_text(text):

    if isinstance(text, bytes):
        text = text.decode("utf-8")
    try:
        return json.loads(f'"{text}"')
    except json.JSONDecodeError:
        return text


def replace_newlines_with_spaces(text):
    return text.replace("\n", " ")


def validate_inn(inn: str) -> bool:
    inn = "".join(filter(str.isdigit, inn))
    if len(inn) not in (10, 12):
        return False

    weights_10 = [2, 4, 10, 3, 5, 9, 4, 6, 8]
    weights_12 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8, 0]

    try:
        if len(inn) == 10:
            check = sum(int(c) * w for c, w in zip(inn[:9], weights_10)) % 11 % 10
            return check == int(inn[9])
        else:
            # Проверка 11-й цифры
            check11 = sum(int(c) * w for c, w in zip(inn[:10], weights_12)) % 11 % 10
            # Проверка 12-й цифры
            weights_12[-1] = 8
            check12 = sum(int(c) * w for c, w in zip(inn[:11], weights_12)) % 11 % 10
            return check11 == int(inn[10]) and check12 == int(inn[11])
    except:
        return False


async def download_telegram_file(file_id: str, token: str) -> bytes:
    file_info = await bot.get_file(file_id)  # noqa: F821
    file_path = file_info.file_path
    file_url = f"https://api.telegram.org/file/bot{token}/{file_path}"

    async with aiohttp.ClientSession() as session:
        async with session.get(file_url) as response:
            if response.status == 200:
                file_content = await response.read()
                return file_content
            else:
                raise Exception(
                    f"Failed to download file. Status code: {response.status}"
                )
