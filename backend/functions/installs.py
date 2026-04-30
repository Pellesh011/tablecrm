from asyncpg.exceptions import UniqueViolationError
from database.db import database, installs, links, users_cboxes_relation


async def get_install(**kwargs):
    args = ",".join(
        [f"installs.c.{arg} == '{val}'" for arg, val in kwargs.items() if val]
    )

    install = await database.fetch_one(eval(f"installs.select().where({args})"))
    return install


async def create_install(install: dict):
    try:
        install_id = await database.execute(installs.insert().values(**install))
        return True, install_id
    except UniqueViolationError:
        return False, "duplicate device key"


async def add_md5_to_install(install_id: int, md5key: str) -> bool:
    await database.execute(
        installs.update().where(installs.c.id == install_id).values(md5key=md5key)
    )
    return True


async def install_bundle_user(tg_token: str, md5key: str) -> bool:
    user_cbox = await database.fetch_one(
        users_cboxes_relation.select().where(users_cboxes_relation.c.token == tg_token)
    )
    install = await database.fetch_one(
        installs.select().where(installs.c.md5key == md5key)
    )

    if not user_cbox or not install:
        return False

    try:
        await database.execute(
            links.insert().values(
                user_id=user_cbox.user,
                install_id=install.id,
                cashbox_id=user_cbox.cashbox_id,
                tg_token=tg_token,
            )
        )
    except UniqueViolationError:
        await database.execute(
            links.update().where(installs.c.md5key == md5key).values(delinked=False)
        )

    return True


async def delink_install(tg_token: str, md5key: str) -> bool:
    install = await database.fetch_one(
        installs.select().where(installs.c.md5key == md5key)
    )

    if not install:
        return False

    await database.execute(
        links.update()
        .where(links.c.tg_token == tg_token, links.c.install_id == install.id)
        .values(delinked=True)
    )

    return True


async def set_settings_install(md5key: str, settings: dict) -> bool:
    await database.execute(
        installs.update().where(installs.c.md5key == md5key).values(**settings)
    )
    return True
