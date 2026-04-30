from collections.abc import Generator

from database.db import engine, users_cboxes_relation
from fastapi import HTTPException
from sqlalchemy.orm import Session


def get_db() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session


async def get_user_by_token(token: str, db: Session):
    user = db.query(users_cboxes_relation).filter(
        users_cboxes_relation.c.token == token
    )
    user = user.first()
    if not user or not user.status:
        raise HTTPException(status_code=403, detail="Вы ввели некорректный токен!")
    return user
