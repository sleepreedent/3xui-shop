import logging
import time
from datetime import datetime, timedelta

from aiogram.fsm.storage.redis import RedisStorage
from aiogram.utils.i18n import lazy_gettext as __
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.bot.services import NotificationService, VPNService
from app.db.models import User

logger = logging.getLogger(__name__)


async def notify_users_with_expiring_subscription(
    session_factory: async_sessionmaker,
    storage: RedisStorage,
    vpn_service: VPNService,
    notification_service: NotificationService,
) -> None:
    session: AsyncSession
    async with session_factory() as session:
        users = await User.get_all(session=session)

        logger.info(
            f"[Background task] Starting subscription expiration check for {len(users)} users."
        )

        for user in users:
            user_notified_key = f"user:notified:{user.tg_id}"

            # Check if user was recently notified
            if await storage.redis.get(user_notified_key):
                continue

            client_data = await vpn_service.get_client_data(user)

            # Skip if no client data or subscription is unlimited
            if not client_data or client_data._expiry_time == -1:
                continue

            current_time_ms = time.time() * 1000
            time_left_ms = client_data._expiry_time - current_time_ms
            threshold_ms = timedelta(hours=24).total_seconds() * 1000

            # Skip if not within the notification threshold
            if not (0 < time_left_ms <= threshold_ms):
                continue

            # Send notification and set Redis flag
            await notification_service.notify_by_id(
                chat_id=user.tg_id,
                text=__("task:message:subscription_expiry").format(
                    devices=client_data.max_devices,
                    expiry_time=client_data.expiry_time,
                ),
                # reply_markup=keyboard_extend
            )

            await storage.redis.set(user_notified_key, "true", ex=timedelta(hours=24))
            logger.info(
                f"[Background task] Sent expiry notification to user {user.tg_id}."
            )
        logger.info("[Background task] Subscription check finished.")


def start_scheduler(
    session_factory: async_sessionmaker,
    storage: RedisStorage,
    vpn_service: VPNService,
    notification_service: NotificationService,
) -> None:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        notify_users_with_expiring_subscription,
        "interval",
        minutes=15,
        args=[session_factory, storage, vpn_service, notification_service],
        next_run_time=datetime.now(),
    )
    scheduler.start()
