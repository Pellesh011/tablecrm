import aiohttp


async def get_account_info(referer: str, access_token: str) -> dict:

    account_info_url = f"https://{referer}/api/v4/account"
    headers = {"Authorization": f"Bearer {access_token}"}

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(account_info_url) as response:
            data = await response.json()
            return data
