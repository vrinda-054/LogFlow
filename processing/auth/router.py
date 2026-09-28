import os

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from processing.auth.dependencies import get_current_user
from processing.auth.schemas import AuthResponse, LoginRequest, SignupRequest, UserResponse
from processing.auth.security import create_access_token
from processing.auth.service import authenticate_user, create_user


router = APIRouter(prefix="/auth", tags=["auth"])


def _user_response(user: dict) -> UserResponse:
    return UserResponse(id=user["id"], name=user["name"], email=user["email"])


def _set_session_cookie(response: Response, token: str, max_age: int) -> None:
    response.set_cookie(
        key="logflow_session",
        value=token,
        max_age=max_age,
        httponly=True,
        secure=os.environ.get("COOKIE_SECURE", "false").lower() == "true",
        samesite=os.environ.get("COOKIE_SAMESITE", "lax"),
        path="/",
    )


@router.post("/signup", response_model=AuthResponse, status_code=status.HTTP_201_CREATED)
def signup(payload: SignupRequest, response: Response) -> AuthResponse:
    try:
        user = create_user(payload.name, payload.email, payload.password)
    except IntegrityError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="An account with this email already exists.")
    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to create account.")

    token, max_age = create_access_token(user["id"])
    _set_session_cookie(response, token, max_age)
    return AuthResponse(user=_user_response(user))


@router.post("/login", response_model=AuthResponse)
def login(payload: LoginRequest, response: Response) -> AuthResponse:
    try:
        user = authenticate_user(payload.email, payload.password)
    except SQLAlchemyError:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Authentication service unavailable.")

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password.")

    token, max_age = create_access_token(user["id"], payload.remember_me)
    _set_session_cookie(response, token, max_age)
    return AuthResponse(user=_user_response(user))


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(response: Response) -> None:
    response.delete_cookie("logflow_session", path="/")


@router.get("/me", response_model=UserResponse)
def current_user(user: dict = Depends(get_current_user)) -> UserResponse:
    return _user_response(user)
