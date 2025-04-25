from fastapi import FastAPI
from routers import items
from routers import chatagent, client_api, dashboard, auth
from routers.client_apis.authorize_login import router as authorize_login_router
from routers.client_apis.fetch_sensor_data import router as fetch_sensor_data
from fastapi.middleware.cors import CORSMiddleware
from langchain_core.prompts import ChatPromptTemplate
import duckdb

app = FastAPI()
app.include_router(items.router)
app.include_router(chatagent.router)
app.include_router(client_api.router)
app.include_router(authorize_login_router, prefix="/api/client")
app.include_router(fetch_sensor_data, prefix="/api/client")
app.include_router(auth.router, prefix="/auth")
app.include_router(dashboard.router, prefix="/dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Replace with your frontend's URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI Updated!"}

@app.get("/agent-full-dataset-sync")
def agent_full_dataset_sync():
    try:
        print("inside agent full dataset sync")
    except Exception as e:
        raise e
    