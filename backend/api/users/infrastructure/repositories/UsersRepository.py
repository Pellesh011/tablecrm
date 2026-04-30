from database.db import database, users


class UsersRepository:
    async def fetch_users_by_ids(self, user_ids: list[int]) -> dict[int, dict]:
        if not user_ids:
            return {}

        query = users.select().where(users.c.id.in_(user_ids))
        user_rows = await database.fetch_all(query)
        return {user["id"]: user for user in user_rows}

    async def fetch_users_by_id(self, user_id: int) -> dict:
        users_map = await self.fetch_users_by_ids([user_id])
        return users_map.get(user_id, {})
