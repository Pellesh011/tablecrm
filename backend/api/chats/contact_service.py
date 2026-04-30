# api/chats/contact_service.py
from datetime import datetime
from typing import Dict, List, Optional

from database.db import chat_contact_links, chat_contacts, chats, database


async def get_or_create_contact(
    cashbox_id: int,
    external_contact_id: str,
    name: Optional[str] = None,
    avatar: Optional[str] = None,
    phone: Optional[str] = None,
    last_activity: Optional[int] = None,
) -> int:
    contact = await database.fetch_one(
        chat_contacts.select().where(
            (chat_contacts.c.cashbox_id == cashbox_id)
            & (chat_contacts.c.external_contact_id == external_contact_id),
        )
    )

    if contact:
        contact_id = contact["id"]
        update_data: Dict = {}
        if name and not contact["name"]:
            update_data["name"] = name
        if avatar and not contact["avatar"]:
            update_data["avatar"] = avatar
        if phone and not contact["phone"]:
            update_data["phone"] = phone
        if last_activity is not None:
            update_data["last_activity"] = last_activity
        if update_data:
            update_data["updated_at"] = datetime.utcnow()
            await database.execute(
                chat_contacts.update()
                .where(chat_contacts.c.id == contact_id)
                .values(**update_data)
            )
        return contact_id

    contact_id = await database.execute(
        chat_contacts.insert().values(
            cashbox_id=cashbox_id,
            external_contact_id=external_contact_id,
            name=name,
            avatar=avatar,
            phone=phone,
            last_activity=last_activity,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
    )
    return contact_id


async def link_contact_to_chat(
    chat_id: int, contact_id: int, role: str = "participant"
) -> None:
    """Создаёт связь между чатом и контактом, если её нет."""
    existing = await database.fetch_one(
        chat_contact_links.select().where(
            (chat_contact_links.c.chat_id == chat_id)
            & (chat_contact_links.c.contact_id == contact_id),
        )
    )
    if not existing:
        await database.execute(
            chat_contact_links.insert().values(
                chat_id=chat_id,
                contact_id=contact_id,
                role=role,
                created_at=datetime.utcnow(),
            )
        )
        chat = await database.fetch_one(chats.select().where(chats.c.id == chat_id))
        if chat and chat.get("chat_contact_id") is None:
            await database.execute(
                chats.update()
                .where(chats.c.id == chat_id)
                .values(chat_contact_id=contact_id, updated_at=datetime.utcnow())
            )


async def get_chat_contacts(chat_id: int) -> List[Dict]:
    """Возвращает список контактов, связанных с чатом."""
    rows = await database.fetch_all(
        chat_contacts.select()
        .select_from(
            chat_contacts.join(
                chat_contact_links,
                chat_contacts.c.id == chat_contact_links.c.contact_id,
            )
        )
        .where(chat_contact_links.c.chat_id == chat_id)
    )
    return [dict(row) for row in rows]


async def get_contact_by_id(contact_id: int) -> Optional[Dict]:
    row = await database.fetch_one(
        chat_contacts.select().where(chat_contacts.c.id == contact_id)
    )
    return dict(row) if row else None


async def update_contact_phone(contact_id: int, phone: str) -> None:
    await database.execute(
        chat_contacts.update()
        .where(chat_contacts.c.id == contact_id)
        .values(phone=phone, updated_at=datetime.utcnow())
    )
