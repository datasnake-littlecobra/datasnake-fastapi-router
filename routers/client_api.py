from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel
from utils.security import decode_access_token
from fastapi.security import OAuth2PasswordBearer
from utils.security import verify_token
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sqlite3

router = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")
cassandra_user = ""
cassandra_password = ""

# def get_current_user(authorization: str = Header(None)):
#     if not authorization or not authorization.startswith("Bearer "):
#         raise HTTPException(status_code=401, detail="Invalid or missing token")

#     token = authorization.split(" ")[1]
#     payload = decode_access_token(token)

#     if not payload:
#         raise HTTPException(status_code=401, detail="Invalid token")

#     return payload["sub"]


def get_current_user(token: str = Depends(oauth2_scheme)):
    return verify_token(token)


# Initialize Cassandra connection
def get_cassandra_session():
    auth_provider = PlainTextAuthProvider(
        cassandra_user, cassandra_password
    )  # Update credentials
    cluster = Cluster(
        ["127.0.0.1"], auth_provider=auth_provider
    )  # Update with actual IP
    session = cluster.connect()
    session.set_keyspace("datasnake")
    return session


@router.get("/api/client/data")
def get_client_data(username: str = Depends(get_current_user)):
    return {"message": f"Hello {username}, here is your data!"}


def get_db():
    conn = sqlite3.connect("auth.db")
    conn.row_factory = sqlite3.Row
    return conn


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/api/login")
def login(request: LoginRequest, db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute(
        "SELECT username FROM users WHERE username = ? AND password = ?",
        (request.username, request.password),
    )
    if user := cursor.fetchone():
        return {"message": "Login successful", "user": request.username}
    else:
        raise HTTPException(status_code=401, detail="Invalid username or password")


@router.get("/api/users")
def get_users(db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()
    return [dict(user) for user in users]


@router.get("/api/client/sensor-data")
def get_sensor_data():
    try:
        session = get_cassandra_session()
        query = """
        SELECT lat, lon, temp, humidity, country, state 
        FROM sensor_data_processed 
        LIMIT 10
        """
        rows = session.execute(query)

        data = [
            {
                "lat": row.lat,
                "lon": row.lon,
                "temp": row.temp,
                "humidity": row.humidity,
                "country": row.country,
                "state": row.state,
            }
            for row in rows
        ]

        return {"sensor_data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# def get_sensor_data(country: str, state: str, limit: int = 10):
#     try:
#         query = """
#         SELECT lat, lon, temp, humidity, country, state
#         FROM sensor_data_processed
#         WHERE country = %s AND state = %s
#         LIMIT %s
#         """
#         rows = session.execute(query, (country, state, limit))

#         data = [{"lat": row.lat, "lon": row.lon, "temp": row.temp, "humidity": row.humidity,
#                  "country": row.country, "state": row.state} for row in rows]

#         return {"sensor_data": data}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
