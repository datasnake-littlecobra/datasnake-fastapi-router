from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from utils.security import verify_password, create_access_token, get_users, authenticate_user
from datetime import timedelta

router = APIRouter()

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/token")
# def login(request: LoginRequest):
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    # users = get_users()
    # print("users printing: ")
    # print(users)
    # user = next((u for u in users if u["username"] == request.username), None)
    # print("inside /token user:")
    # print(user)
    # if not user or not verify_password(request.password, user["hashed_password"]):
    #     raise HTTPException(status_code=401, detail="Invalid credentials")
    
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    
    access_token = create_access_token(data={"sub": user["username"]}, expires_delta=timedelta(minutes=30))
    return {"access_token": access_token, "token_type": "bearer"}
