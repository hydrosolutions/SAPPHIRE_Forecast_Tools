import logging
import time
from datetime import datetime
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from fastapi.responses import JSONResponse
import httpx
from typing import Optional
from app.config import settings


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.gateway_title,
    version=settings.gateway_version,
    description="API Gateway for SAPPHIRE Services",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Service URLS
SERVICES = settings.services

# Timeout configuration
TIMEOUT = settings.request_timeout


# Middleware for logging
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()

    logger.info(f"Incoming request: {request.method} {request.url} {request.url.path}")

    response = await call_next(request)

    process_time = time.time() - start_time

    logger.info(f"Request completed in {process_time:.2f}s with status: {response.status_code}")

    response.headers["X-Process-Time"] = str(process_time)
    return response


# Simple API Key authentication (replace with JWT or OAuth2 in production)
async def verify_api_key(x_api_key: Optional[str] = Header(None)):
    """
    Simple API key verification.
    In production, replace with proper authentication (JWT, OAuth2, etc.)
    """
    if settings.api_key_enabled:
        if not x_api_key or x_api_key != settings.api_key:
            raise HTTPException(status_code=401, detail="Invalid or missing API Key")
    return x_api_key


async def proxy_request(
    service_url: str,
    path: str,
    request: Request,
    method: str = "GET"
):
    """
    Proxy requests to downstream services.
    """
    url = f"{service_url}{path}"

    # Get request body if present
    body = None
    if method in ["POST", "PUT", "PATCH"]:
        body = await request.body()

    # Forward headers (excluding host)
    headers = dict(request.headers)
    headers.pop("host", None)

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                content=body,
                params=request.query_params
            )

            # Filter out headers that shouldn't be forwarded
            response_headers = {}
            excluded_headers = {
                'content-length',
                'content-encoding',
                'transfer-encoding',
                'connection'
            }

            for key, value in response.headers.items():
                if key.lower() not in excluded_headers:
                    response_headers[key] = value

            return JSONResponse(
                content=response.json() if response.text else {},
                status_code=response.status_code,
                headers=response.headers
            )
    except httpx.TimeoutException:
        logger.error(f"Timeout calling {url}")
        raise HTTPException(status_code=504, detail="Gateway Timeout")
    except httpx.ConnectError:
        logger.error(f"Connection error when connecting to {url}.")
        raise HTTPException(status_code=503, detail="Service Unavailable")
    except Exception as e:
        logger.error(f"Error proxying request to {url}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


# Health check endpoint
@app.get("/health", tags=["Health"])
async def health_check():
    """Gateway health check"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": SERVICES
    }


# Service health checks
@app.get("/health/services", tags=["Health"])
async def check_services_health():
    """Check health of all downstream services"""
    health_status = {}

    async with httpx.AsyncClient(timeout=settings.health_check_timeout) as client:
        for service_name, service_url in SERVICES.items():
            try:
                response = await client.get(f"{service_url}/health")
                health_status[service_name] = {
                    "status": "healthy" if response.status_code == 200 else "unhealthy",
                    "status_code": response.status_code
                }
            except Exception as e:
                health_status[service_name] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
    return health_status


# Preprocessing service routes
@app.api_route("/api/preprocessing/{path:path}", methods=["GET", "POST"], tags=["Preprocessing"])
async def preprocessing_proxy(
        path: str,
        request: Request,
        api_key: str = Depends(verify_api_key)
):
    """Proxy requests to preprocessing service"""
    return await proxy_request(
        service_url=SERVICES["preprocessing"],
        path=f"/{path}",
        request=request,
        method=request.method
    )


# Postprocessing service routes
@app.api_route("/api/postprocessing/{path:path}", methods=["GET", "POST"], tags=["Postprocessing"])
async def postprocessing_proxy(
    path: str,
    request: Request,
    api_key: str = Depends(verify_api_key)
):
    """Proxy requests to postprocessing service"""
    return await proxy_request(
        service_url=SERVICES["postprocessing"],
        path=f"/{path}",
        request=request,
        method=request.method
    )


# User service routes
@app.api_route("/api/user/{path:path}", methods=["GET", "POST", "PUT", "DELETE"], tags=["Users"])
async def users_proxy(path: str, request: Request, api_key: str = Depends(verify_api_key)):
    return await proxy_request(SERVICES["user"], f"/{path}", request, request.method)


# Auth service routes (no API key required for auth endpoints)
@app.api_route("/api/auth/{path:path}", methods=["GET", "POST", "PUT", "DELETE"], tags=["Authentication"])
async def auth_proxy(path: str, request: Request):
    return await proxy_request(SERVICES["auth"], f"/{path}", request, request.method)


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    return {
        "message": "Welcome to the SAPPHIRE API Gateway",
        "version": settings.gateway_version,
        "services": list(SERVICES.keys()),
        "timestamp": datetime.utcnow().isoformat(),
    }
