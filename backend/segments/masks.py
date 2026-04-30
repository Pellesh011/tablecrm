import re


def mask_replacer(text: str, replacements: dict) -> str:
    def replacer(match):
        key = match.group(1).strip()  # вытаскиваем имя переменной без {{ }}
        return str(replacements.get(key, ""))  # если нет значения — убираем маску

    return re.sub(r"\{\{\s*(.*?)\s*\}\}", replacer, text)


def replace_masks(message: any, replacements: dict):
    if isinstance(message, str):
        new_message = mask_replacer(message, replacements)
    elif isinstance(message, list):
        new_message = [replace_masks(m, replacements) for m in message]
    elif isinstance(message, dict):
        new_message = {}
        for k, v in message.items():
            new_message[replace_masks(k, replacements)] = replace_masks(v, replacements)
    else:
        new_message = message
    return new_message
