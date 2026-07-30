from fastapi import APIRouter

from services.api.app.api.v1 import auth, events, health, orders

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(auth.router)
api_router.include_router(events.router)
api_router.include_router(orders.router)
