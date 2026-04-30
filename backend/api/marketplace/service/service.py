from api.marketplace.service.favorites_service.service import (
    MarketplaceFavoritesService,
)
from api.marketplace.service.locations_service.service import (
    MarketplaceLocationsService,
)
from api.marketplace.service.orders_service.service import MarketplaceOrdersService
from api.marketplace.service.product_cart_service.service import MarketplaceCartService
from api.marketplace.service.products_list_service.service import (
    MarketplaceProductsListService,
)
from api.marketplace.service.public_categories.public_categories_service import (
    MarketplacePublicCategoriesService,
)
from api.marketplace.service.qr_service.service import MarketplaceQrService
from api.marketplace.service.review_service.service import MarketplaceReviewService
from api.marketplace.service.seller_service.service import MarketplaceSellerService
from api.marketplace.service.seller_statistics_service.service import (
    MarketplaceSellerStatisticsService,
)
from api.marketplace.service.view_event_service.service import (
    MarketplaceViewEventService,
)
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.utils.ioc.ioc import ioc


class MarketplaceService(
    MarketplaceProductsListService,
    MarketplaceOrdersService,
    MarketplaceReviewService,
    MarketplaceFavoritesService,
    MarketplaceQrService,
    MarketplaceViewEventService,
    MarketplaceLocationsService,
    MarketplaceCartService,
    MarketplacePublicCategoriesService,
    MarketplaceSellerService,
    MarketplaceSellerStatisticsService,
):
    def __init__(self):
        super().__init__()

    async def connect(self):
        self._rabbitmq = await ioc.get(IRabbitFactory)()
