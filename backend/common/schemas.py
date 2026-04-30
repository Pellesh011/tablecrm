from typing import Union

from phonenumbers import (
    NumberParseException,
    PhoneNumberFormat,
    format_number,
    is_valid_number,
    parse,
)


class RuPhone(str):
    """Кастомный тип для российских телефонных номеров"""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    def __str__(self):
        return super().__str__()

    @classmethod
    def validate(cls, value) -> Union[str, None]:
        # Обработка различных типов None-значений
        if (
            value is None
            or value == ""
            or (isinstance(value, str) and value.strip() == "")
        ):
            return None

        # Если уже RuPhone, возвращаем как строку
        if isinstance(value, RuPhone):
            return str(value)

        # Конвертируем в строку
        try:
            value_str = str(value)
        except Exception:
            raise TypeError("string required")

        # Пустая строка после конвертации
        if not value_str or value_str.strip() == "":
            return None

        try:
            # Очистка номера от лишних символов
            cleaned = "".join(filter(str.isdigit, value_str))

            # Если пусто после очистки
            if not cleaned:
                return None

            # Нормализация российских номеров
            if cleaned.startswith("8"):
                cleaned = "7" + cleaned[1:]
            elif not cleaned.startswith("7") and len(cleaned) == 10:
                # 10 цифр без кода страны - считаем российским
                cleaned = "7" + cleaned
            elif not cleaned.startswith("7"):
                # Другие варианты оставляем как есть
                pass

            # Форматируем с +
            formatted = f"+{cleaned}" if not cleaned.startswith("+") else cleaned

            # Парсим номер
            try:
                parsed = parse(formatted, "RU")
            except NumberParseException:
                # Попробуем автоопределение региона
                parsed = parse(formatted, None)

            # Проверяем валидность
            if not is_valid_number(parsed):
                # Не валидный номер - возвращаем None вместо ошибки
                return None

            # Возвращаем в формате E164
            return format_number(parsed, PhoneNumberFormat.E164)

        except Exception:
            # Любые ошибки - возвращаем None
            return None
