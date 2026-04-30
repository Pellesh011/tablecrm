from jobs.autoburn_job.job import AutoBurn


class TriggersNotification(AutoBurn):
    async def test(self) -> None:
        return await self.transactions(trigger=3600 * 4)


async def run():
    cards = await TriggersNotification.get_cards()
    for card in cards:
        print(await TriggersNotification(card=card).test())
