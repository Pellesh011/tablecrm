import os
import random
from datetime import datetime, timedelta

import aiohttp
from fastapi.openapi.utils import get_openapi
from fastapi.security import (
    OAuth2PasswordBearer,
)
from jose import jwt

SECRET_KEY = os.environ.get("JWT_SECRET_KEY")
SECRET_REFRESH_KEY = os.environ.get("JWT_REFRESH_SECRET_KEY")
ALGORITHM = os.environ.get("JWT_ALGO")
ACCESS_TOKEN_EXPIRE_MINUTES = 30  # 30 minutes
REFRESH_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days


def generate_rand_hex():
    return "%030x" % random.randrange(16**30)


def decode_jwt(token):
    return jwt.decode(token, SECRET_REFRESH_KEY, ALGORITHM)


def create_access_token(data: dict, expires_delta: int = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def create_refresh_token(data: dict, expires_delta: int = None) -> str:
    to_encode = data.copy()
    if expires_delta is not None:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=REFRESH_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_REFRESH_KEY, ALGORITHM)
    return encoded_jwt


def parse_openapi_json(routes):

    full_data = []
    openapi = get_openapi(title="1", routes=routes, version="1")
    for path, val in openapi["paths"].items():

        for k, v in val.items():
            data = {}
            full_path = ""
            descr = ""
            if k in ("get", "post", "put", "patch", "delete"):
                full_path = path + "." + k
                data["scope"] = full_path
                data["interaction"] = openapi["paths"][path][k]["summary"]
                full_data.append(data)
    return full_data


def get_jwt_groups(routes):
    data = []
    openapi = get_openapi(title="1", routes=routes, version="1")
    for path, val in openapi["paths"].items():
        for k, v in val.items():
            full_path = ""
            descr = ""
            if k in ("get", "post", "put", "patch", "delete"):
                data += openapi["paths"][path][k]["tags"]
    data = set(data)
    return list(data)


# openapi_scopes = parse_openapi_json()

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="token",
    #     scopes=openapi_scopes,
)


async def ouath_response(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            text = await resp.text()
    return text
