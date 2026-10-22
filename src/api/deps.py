from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends

from core.uow import IUnitOfWork, UnitOfWork
from services.payment_service import PaymentService


async def get_uow() -> AsyncGenerator[IUnitOfWork]:
    """Провайдер UnitOfWork для инъекции зависимостей"""
    uow = UnitOfWork()
    async with uow:
        yield uow


UowDepends = Annotated[IUnitOfWork, Depends(get_uow)]


async def get_payment_service(uow: UowDepends) -> PaymentService:
    service = PaymentService(uow=uow)
    return service


PaymentServiceDepends = Annotated[PaymentService, Depends(get_payment_service)]
