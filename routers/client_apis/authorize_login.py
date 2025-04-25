from fastapi import APIRouter, Depends, HTTPException, Header, Query, Request
from pydantic import BaseModel
from utils.security import decode_access_token
from fastapi.security import OAuth2PasswordBearer
from utils.security import verify_token
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import httpx
import logging

router = APIRouter()

CLOUDFLARE_WORKER_URL_LOGIN = (
    "https://d1-worker-production.rustchain64.workers.dev/api/login"
)


class AuthRequest(BaseModel):
    username: str
    password: str


@router.get("/test-authorize")
def test_authorize():
    return {"message": "Test Authorize Success"}

# following method authenticates login using username/password:
#     URL: http://localhost:8000/api/client/authorize-login
#     Body: {
#     "username":"james_bond",
#     "password":"james_bond"
# }
#     Headers:
#         Content-Type: application/json
@router.post("/authorize-login")
async def login(auth_data: AuthRequest):
    print("inside api client auth login :", auth_data)
    logging.info(f"inside api client auth login: {auth_data}")
    async with httpx.AsyncClient() as client:
        print("going to call client")
        response = await client.post(CLOUDFLARE_WORKER_URL_LOGIN, json=auth_data.dict())
        print("after calling client")

    if response.status_code != 200:
        print("did i get here !200 response???")
        logging.info("did i get here !200 response???")
        raise HTTPException(
            status_code=response.status_code, detail="Authentication failed"
        )

    data = response.json()

    logging.info(f"200 response with {data}")
    print("200 response with", data)
    return {
        "status": response.status_code,
        "token": data.get("user").get("uuid"),
        "role": data.get("user").get("role"),
        "message": data.get("message"),
        "username": data.get("user").get("username"),
    }


# following method authenticates login using token: