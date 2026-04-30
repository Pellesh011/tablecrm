Создание документа на основании шаблона и переменных

переменные:
это словарь, где значения ключей словаря могут быть
строки, числа, булево, массивы, словари

Форматирование:
1. Использование двойных кавычек обязательно
2. Кавычки в значениях переменных типа строка обязательно экранировать: `````\"`````
3. Порядок перечисления переменных не имеет значение

Тестовый словарь переменных для генерации шаблона
bill.tpl (backend/docs_templates)


{
    "title": "cчет на оплату",
    "logo": "https://uploads.turbologo.com/uploads/design/preview_image/807421/preview_image20211222-743-19qazwo.png",
    "number": "33/22",
    "seller": {
        "name": "ООО Рога и копыта",
        "inn": 290290290,
        "kpp": 654321,
        "bic_bank": 321432,
        "bill_number": 22222222222222222,
        "adress": "Архангельск, ул. Котлаская 13",
        "bank": "Альфа Банк, Арханнельск, Обводный канал 9"
    },
    "buyer": {
        "name": "ООО Умники и Умницы",
        "inn": 2909090909,
        "kpp": 3232325,
        "bic_bank": 123456,
        "bill_number": 33333333333333333,
        "adress": "Архангельск, ул. Мира 20",
        "bank": "Сбер Банк, Арханнельск, Урицкого канал 10"},
    "products": {
        "summ": 232123,
        "list": [
            {
                "name": "Подшипник 1",
                "img": "https://cable.ru/assets/images/catalog/bearings/model_1215-k.jpg",
                "desc": "Lorem ipsum, dolor sit amet consectetur adipisicing elit. Laboriosam quos nisi tenetur quae odio perspiciatis eveniet nesciunt cum commodi in!",
                "count": 3,
                "price": 123,
                "unit": "шт"
            },
            {
                "name": "Подшипник 2",
                "img": "https://rost-holding.ru/spb/shop_6/3/6/9/item_36923/small_item_image36923.jpg",
                "desc": "Lorem ipsum, dolor sit amet consectetur adipisicing elit. Laboriosam quosn!",
                "count": 2,
                "price": 3232.97,
                "unit": "шт"
            },
            {
                "name": "Подшипник 3",
                "img": "https://dvapodshipnika.ru/wp-content/uploads/2020/04/207-870x400.jpg",
                "desc": "Lorem ipsum, dolor sit amet consectetur adipisicing elit. Laboriosam quos nisi tenetur cum commodi in!",
                "count": 1,
                "price": 323233,
                "unit": "шт"
            }
        ]
    }
}
