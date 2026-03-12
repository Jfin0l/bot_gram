from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import uvicorn
from contextlib import asynccontextmanager

# Tip: Lifespan para manejo de inicios y cierres si fuera necesario
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Podríamos inicializar algo aquí
    yield
    # Shutdown: Limpieza aquí

app = FastAPI(
    title="bot_gram API",
    description="Interfaz web y Webhooks para FastMoney Bot P2P",
    version="1.0.0",
    lifespan=lifespan
)

# Configuración de CORS restrictiva
# En producción, cambia esto por los dominios específicos de tu Dashboard
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "online", "message": "bot_gram API is running"}

# Los routers se importarán e incluirán aquí
from api.routers import payments
app.include_router(payments.router, tags=["payments"])

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 8000))
    host = os.getenv("API_HOST", "0.0.0.0")
    uvicorn.run(app, host=host, port=port)
