# main.py
import os
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

# ──────────────────────────────────────────────────────────────────────────────
# Client SDKs
# ──────────────────────────────────────────────────────────────────────────────
from clients.supabase.client import supabase
from clients.sunsynk.plants import PlantAPI
from clients.sunsynk.inverters import InverterAPI
from clients.supabase.tables.residential_estates import (
    EstateIn,
    get_structure,
    get_all_residential_estates,
    insert_residential_estate,
    update_residential_estate,
    delete_residential_estate,
)

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI & CORS
# ──────────────────────────────────────────────────────────────────────────────
app = FastAPI()

origins = [
    "http://localhost:3000",                         # local Next.js
    "https://YOUR_NEXTJS_DOMAIN",                    # prod Next.js
    "https://bit2bit.retool.com",                    # Retool
    "https://36437237-d437-4d3f-bf19-5b13320612df-00-2mvuymb9892lc.riker.replit.dev",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,    # ['*'] while debugging if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="signin")

@app.get("/")
def root():
    return {"message": "API is running"}

# ──────────────────────────────────────────────────────────────────────────────
# Pydantic models
# ──────────────────────────────────────────────────────────────────────────────
class AuthIn(BaseModel):
    email: str
    password: str

class RefreshIn(BaseModel):
    refresh_token: str

class ClientIn(BaseModel):
    supabase_uid: str
    name: str | None = ""
    email: str
    phone: str | None = ""

# ──────────────────────────────────────────────────────────────────────────────
# Auth helpers
# ──────────────────────────────────────────────────────────────────────────────
def get_current_user(token: str = Depends(oauth2_scheme)):
    user_resp = supabase.auth.get_user(token)
    user = getattr(user_resp, "user", None) or getattr(user_resp.data, "user", None)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

# ──────────────────────────────────────────────────────────────────────────────
# Auth endpoints
# ──────────────────────────────────────────────────────────────────────────────
@app.post("/signup", status_code=status.HTTP_201_CREATED)
def signup(data: AuthIn):
    res = supabase.auth.sign_up({"email": data.email, "password": data.password})
    user = getattr(res, "user", None) or getattr(res.data, "user", None)
    if not user:
        err = getattr(res, "error", None) or getattr(res, "message", None) or "Signup failed"
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=err)
    return {"message": "Signup successful — check your inbox for confirmation."}

@app.post("/signin")
def signin(data: AuthIn):
    res = supabase.auth.sign_in_with_password({"email": data.email, "password": data.password})
    session = getattr(res, "session", None) or getattr(res.data, "session", None)
    if not session:
        err = getattr(res, "error", None) or "Invalid credentials"
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=err)
    return {
        "access_token":  session.access_token,
        "refresh_token": session.refresh_token,
        "expires_in":    session.expires_in,
        "token_type":    session.token_type,
    }

@app.post("/refresh_token")
def refresh(data: RefreshIn):
    try:
        resp = supabase.auth.refresh_session(data.refresh_token)
        session = getattr(resp, "session", None)
        if not session:
            raise RuntimeError("could not refresh session")
    except Exception as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))

    return {
        "access_token":  session.access_token,
        "refresh_token": session.refresh_token,
        "expires_in":    session.expires_in,
        "token_type":    session.token_type,
    }

# ──────────────────────────────────────────────────────────────────────────────
# Client upsert (Bit2Bit user mapping)
# ──────────────────────────────────────────────────────────────────────────────
@app.post("/clients", status_code=status.HTTP_201_CREATED)
def upsert_client(client: ClientIn, user=Depends(get_current_user)):
    res = supabase.table("b2b_clients").upsert(client.dict()).execute()
    if getattr(res, "error", None):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
    return res.data

# ──────────────────────────────────────────────────────────────────────────────
# Sunsynk API instances
# ──────────────────────────────────────────────────────────────────────────────
plants = PlantAPI(
    username=os.getenv("SUNSYNK_USER", "solar@houss.co.za"),
    password=os.getenv("SUNSYNK_PWD", "Inverter@Houss"),
)
inverters = InverterAPI(
    username=os.getenv("SUNSYNK_USER", "solar@houss.co.za"),
    password=os.getenv("SUNSYNK_PWD", "Inverter@Houss"),
)

# ──────────────────────────────────────────────────────────────────────────────
# Plant endpoints
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/plants")
def get_plants(page: int = 1, user=Depends(get_current_user)):
    return plants.list(page=page)

@app.get("/plants/count")
def plant_summary(user=Depends(get_current_user)):
    return plants.count()

# ──────────────────────────────────────────────────────────────────────────────
# Inverter endpoints
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/inverters/count")
def inverter_summary(user=Depends(get_current_user)):
    return inverters.count()

# ──────────────────────────────────────────────────────────────────────────────
# Supabase Residential-Estate helpers
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/db/residential-estates/structure")
def structure(user=Depends(get_current_user)):
    res = get_structure()
    if getattr(res, "error", None):
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.error.message)
    return res.data

@app.get("/db/residential_estates")
def list_estates(user=Depends(get_current_user)):
    res = get_all_residential_estates()
    if getattr(res, "error", None):
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.error.message)
    return res.data

@app.post("/db/residential-estates", status_code=status.HTTP_201_CREATED)
def create_estate(e: EstateIn, user=Depends(get_current_user)):
    res = insert_residential_estate(e.dict())
    if getattr(res, "error", None):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
    return res.data

@app.put("/db/residential-estates/{estate_id}")
def update_estate(estate_id: int, e: EstateIn, user=Depends(get_current_user)):
    res = update_residential_estate(estate_id, e.dict())
    if getattr(res, "error", None):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
    return res.data

@app.delete("/db/residential-estates/{estate_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_estate(estate_id: int, user=Depends(get_current_user)):
    res = delete_residential_estate(estate_id)
    if getattr(res, "error", None):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
    return

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True,
    )
