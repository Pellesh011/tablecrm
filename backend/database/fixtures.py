import json

from database.db import entity_or_function, global_categories, payments, segments, units
from sqlalchemy import event, text


def prepopulate_global_categories(target, connection, **kwargs):
    print("Populating global_categories...")
    with open(
        "database/initial_data/global_categories.json", "r", encoding="UTF-8"
    ) as file:
        values = json.load(file)
        connection.execute(target.insert(), *values)


def prepopulate_units(target, connection, **kwargs):
    print("Populating units...")
    with open("database/initial_data/units.json", "r", encoding="UTF-8") as file:
        values = json.loads(json.load(file))
        connection.execute(target.insert(), *values)


def prepopulate_functions(target, connection, **kwargs):
    print("Populating functions...")
    with open("database/initial_data/functions.json", "r", encoding="UTF-8") as file:
        values = json.load(file)
        connection.execute(target.insert(), *values)


def create_raschet_func(target, connection, **kwargs):
    with open("database/initial_data/raschet.sql", "r", encoding="UTF-8") as file:
        sql = file.read()
        connection.execute(text(sql))


def prepopulate_segments(target, connection, **kwargs):
    print("Populating segments...")
    with open("database/initial_data/segments.json", "r", encoding="UTF-8") as file:
        values = json.loads(json.load(file))
        connection.execute(target.insert(), *values)


def init_db():
    event.listen(units, "after_create", prepopulate_units)
    event.listen(entity_or_function, "after_create", prepopulate_functions)
    event.listen(payments, "after_create", create_raschet_func)
    event.listen(global_categories, "after_create", prepopulate_global_categories)
    event.listen(segments, "after_create", prepopulate_segments)
