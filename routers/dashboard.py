from fastapi import APIRouter

router = APIRouter()

@router.get("/dashboard/data")
def get_dashboard_data():
    return {"dashboard": "This is internal dashboard data"}
