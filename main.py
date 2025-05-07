# main.py
# -----------------------------------------------------------------------------
# FastAPI entry‑point for HOUSS‑API
# -----------------------------------------------------------------------------
# Updated: 2025‑05‑06
#   • Added QR‑code generation utilities to create WhatsApp deep‑link QR codes
#   • Secured the new endpoint with existing bearer‑token auth
#   • Re‑organized imports for clarity
# -----------------------------------------------------------------------------

# Standard library
import io     # in‑memory byte streams for QR images
import os
from datetime import date
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote_plus  # URL‑safe query‑string encoding

# Third‑party packages
import qrcode                         # QR‑code generator (Pillow backend)
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse  # stream PNG back to caller
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel


# Internal SDK / helper imports
from clients.supabase.client import supabase
from clients.supabase.tables.estate_plant_daily_report import (
    EstatePlantDailyReport,
    get_daily_reports,
    insert_daily_report,
)
from clients.supabase.queries.energy_metrics import fetch_hourly_energy_metrics
from clients.supabase.tables.residential_estates import (
    ResidentialEstate,
    delete_residential_estate,
    get_all_residential_estates,
    get_structure,
    insert_residential_estate,
    update_residential_estate,
)
from clients.supabase.tables.estate_plant import (
    get_estate_plant,
    get_estate_plant_totals,
    get_offline_plants,
)
from clients.sunsynk.inverters import InverterAPI
from clients.sunsynk.plants import PlantAPI
from clients.clickatell.clickatell_client import send_sms, SmsPayload   
from clients.whatsapp.whatsapp_client import send_whatsapp, WhatsappPayload



# ─────────────────────────────────────────────────────────────────────────────
#  FastAPI & CORS
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI()

origins = [
    "http://localhost:3000",
    "https://YOUR_NEXTJS_DOMAIN",
    "https://bit2bit.retool.com",
    "https://36437237-d437-4d3f-bf19-5b13320612df-00-2mvuymb9892lc.riker.replit.dev",
    "https://36437237-d437-4d3f-bf19-5b13320612df-00-2mvuymb9892lc.riker.replit.dev:3001",
    "https://houss-dashboard.replit.app",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="signin")

@app.get("/")
def root():
    return {"message": "API is running"}

# ─────────────────────────────────────────────────────────────────────────────
#  Generic auth payloads
# ─────────────────────────────────────────────────────────────────────────────
class AuthIn(BaseModel):
    email: str
    password: str

class RefreshIn(BaseModel):
    refresh_token: str

class ClientIn(BaseModel):          # (Bit2Bit mapping)
    supabase_uid: str
    name:  Optional[str] = ""
    email: str
    phone: Optional[str] = ""

# ─────────────────────────────────────────────────────────────────────────────
#  Helper – validate bearer token with Supabase
# ─────────────────────────────────────────────────────────────────────────────
def get_current_user(token: str = Depends(oauth2_scheme)):
    # strip common copy / paste artefacts
    token = token.strip().replace('\u200b', '')  # zero-width space

    # If any non-ASCII bytes remain, reject early with 401
    if any(ord(c) > 127 for c in token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Malformed access token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        user_resp = supabase.auth.get_user(token)   # ASCII-clean now
    except Exception as exc:
        # most Supabase errors become 403/404 here, treat as invalid
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc

    user = getattr(user_resp, "user", None) or getattr(user_resp.data, "user", None)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

# ─────────────────────────────────────────────────────────────────────────────
#  Auth endpoints
# ─────────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────
#  Client upsert (Bit2Bit mapping)
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/clients", status_code=status.HTTP_201_CREATED)
def upsert_client(client: ClientIn, user=Depends(get_current_user)):
    res = supabase.table("b2b_clients").upsert(client.dict()).execute()
    if getattr(res, "error", None):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
    return res.data

# ─────────────────────────────────────────────────────────────────────────────
#  Sunsynk API instances
# ─────────────────────────────────────────────────────────────────────────────
plants = PlantAPI(
    username=os.getenv("SUNSYNK_USER", "solar@houss.co.za"),
    password=os.getenv("SUNSYNK_PWD", "Inverter@Houss"),
)
inverters = InverterAPI(
    username=os.getenv("SUNSYNK_USER", "solar@houss.co.za"),
    password=os.getenv("SUNSYNK_PWD", "Inverter@Houss"),
)

# ─────────────────────────────────────────────────────────────────────────────
#  Plant & inverter endpoints
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/plants")
def get_plants(page: int = 1, user=Depends(get_current_user)):
    return plants.list(page=page)

@app.get("/plants/count")
def plant_summary(user=Depends(get_current_user)):
    return plants.count()

@app.get("/inverters/count")
def inverter_summary(user=Depends(get_current_user)):
    return inverters.count()

# ─────────────────────────────────────────────────────────────────────────────
#  Residential-estate endpoints
# ─────────────────────────────────────────────────────────────────────────────
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
def create_estate(e: ResidentialEstate, user=Depends(get_current_user)):
    res = insert_residential_estate(e)
    if getattr(res, "error", None):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
    return res.data

@app.put("/db/residential-estates/{estate_id}")
def update_estate(estate_id: int, e: ResidentialEstate, user=Depends(get_current_user)):
    res = update_residential_estate(estate_id, e)
    if getattr(res, "error", None):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
    return res.data

@app.delete("/db/residential-estates/{estate_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_estate(estate_id: int, user=Depends(get_current_user)):
    res = delete_residential_estate(estate_id)
    if getattr(res, "error", None):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
    return

# ─────────────────────────────────────────────────────────────────────────────
#  Estate-plant card view + totals
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/db/estate-plants")
def list_estate_plants(
    page: int = 1,
    page_size: int = 30,
    user=Depends(get_current_user)
):
    if page < 1 or page_size < 1 or page_size > 100:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="page must be ≥1 and 1 ≤ page_size ≤ 100",
        )
    try:
        return get_estate_plant(page=page, page_size=page_size)
    except RuntimeError as err:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(err))

@app.get("/db/estate-plant-totals/{estate_id}")
def estate_totals(estate_id: int, user=Depends(get_current_user)):
    try:
        return get_estate_plant_totals(estate_id)
    except RuntimeError as err:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(err))

# ─────────────────────────────────────────────────────────────────────────────
#  Estate-plant daily-report endpoints
# ─────────────────────────────────────────────────────────────────────────────
@app.post(
    "/db/estate-plant-daily-report",
    status_code=status.HTTP_201_CREATED,
    response_model=dict,
)
def create_daily_report(payload: EstatePlantDailyReport, user=Depends(get_current_user)):
    if payload.user_id is None:
        payload.user_id = user.id
    return insert_daily_report(payload)

@app.get(
    "/db/estate-plant-daily-report",
    response_model=dict,  # { rows, total, pageSize, pageNumber }
)
def list_daily_reports(
    estate_id: Optional[int] = None,
    plant_id:  Optional[int] = None,
    date_from: Optional[date] = None,
    date_to:   Optional[date] = None,
    page:      int = 1,
    page_size: int = 50,
    user=Depends(get_current_user),
):
    return get_daily_reports(
        estate_id=estate_id,
        plant_id=plant_id,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )

# ─────────────────────────────────────────────────────────────────────────────
#  Hourly energy metrics
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/energy-today")
def energy_today(
    estate: List[int] = Query(..., description="Repeat ?estate=<id>"),
    day:    int       = Query(0, ge=0, le=30, description="0=today, 1=yesterday, …"),
    user = Depends(get_current_user),
):
    try:
        return fetch_hourly_energy_metrics(estate_ids=estate, day_offset=day)
    except RuntimeError as err:
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(err))

# -----------------------------------------------------------------------------
# WhatsApp QR‑code endpoint (NEW)
# -----------------------------------------------------------------------------
# Returns a PNG image that launches WhatsApp chat with a pre‑filled message.
# Secured via existing bearer‑token mechanism.
# Example call:
#   GET /qr/whatsapp?phone=%2B27829215785&broker=Carla%20Prinsloo
#   Authorization: Bearer <access_token>
# -----------------------------------------------------------------------------
# ─── WhatsApp QR endpoint ───────────────────────────────────────────────────
# ─── WhatsApp QR endpoint ───────────────────────────────────────────────────
@app.get(
    "/qr/whatsapp",
    response_description="PNG QR code",
    responses={401: {"description": "Unauthorised"}},
)
def whatsapp_qr(
    phone: str = Query(..., min_length=6, description="WhatsApp number"),
    broker: str = Query(..., min_length=1, description="Broker name"),
    ios: bool = Query(False, description="Set true if scanning on iPhone / iOS"),
    user=Depends(get_current_user),
):
    # ── 1. Validate / normalise phone number ────────────────────────────────
    digits = "".join(filter(str.isdigit, phone))
    if not digits:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Provide a valid phone number",
        )

    # ── 2. Compose message and choose link ──────────────────────────────────
    msg = f"Hi {broker}, I would like us to have a quick session to discuss my financial planning."
    encoded = quote_plus(msg)
    wa_link = f"https://wa.me/{digits}?text={encoded}"      # universal link

    # ── 3. Generate high-res QR ─────────────────────────────────────────────
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_Q,
        box_size=12,
        border=4,
    )
    qr.add_data(wa_link)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")

    # ── 4. Persist PNG to QR_Broker library ────────────────────────────────
    # Create folder if it doesn’t exist
    out_dir = Path("QR_Broker")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build filename: broker_<broker>_<digits>.png
    safe_broker = broker.lower().replace(" ", "_")
    file_name = f"broker_{safe_broker}_{digits}.png"
    file_path = out_dir / file_name
    img.save(file_path, format="PNG")

    # ── 5. Stream back to caller ────────────────────────────────────────────
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return StreamingResponse(buf, media_type="image/png")

# -----------------------------------------------------------------------------
# SMS endpoint (Clickatell)
# -----------------------------------------------------------------------------
@app.post("/sms/send", status_code=status.HTTP_202_ACCEPTED)
def sms_send(payload: SmsPayload, user = Depends(get_current_user)):
    """
    Send an SMS via Clickatell Platform.
    Requires a valid bearer-token.
    """
    try:
        result = send_sms(phone=payload.phone, message=payload.message)
        return {"success": True, "clickatell": result}
    except RuntimeError as err:
        raise HTTPException(status_code=400, detail=str(err))

# -----------------------------------------------------------------------------
# WHATSAPP endpoint (Meta) / SEND MESSAGE
# -----------------------------------------------------------------------------
@app.post("/whatsapp/send", status_code=status.HTTP_202_ACCEPTED)
def whatsapp_send(payload: WhatsappPayload, user=Depends(get_current_user)):
    try:
        result = send_whatsapp(phone=payload.phone, message=payload.message)
        return {"success": True, "whatsapp": result}
    except RuntimeError as err:
        raise HTTPException(status_code=400, detail=str(err))




# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True,
    )
