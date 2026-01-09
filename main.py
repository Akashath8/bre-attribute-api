import os
from datetime import datetime, timedelta

import psycopg2
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt

# --------------------------------------------------
# App
# --------------------------------------------------
app = FastAPI(title="BRE Attribute API")

# --------------------------------------------------
# Environment / Config
# --------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "sslmode": "require"
}

SECRET_KEY = os.getenv("SECRET_KEY", "change_this_secret")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# --------------------------------------------------
# OAuth2
# --------------------------------------------------
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Hardcoded user (OK for PoC / Nected)
bre_user = {
    "username": "bre_engine",
    "password": "bre_password"
}

# --------------------------------------------------
# DB Helper
# --------------------------------------------------
def get_db():
    return psycopg2.connect(**DB_CONFIG)

# --------------------------------------------------
# Auth Helpers
# --------------------------------------------------
def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated"
            )
        return username
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )

# --------------------------------------------------
# Public Health API (USED BY Nected Test Connection)
# --------------------------------------------------
@app.get("/health")
def health():
    return {"status": "BRE API running"}

# --------------------------------------------------
# OAuth Token API (USED BY Nected)
# --------------------------------------------------
@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if (
        form_data.username != bre_user["username"]
        or form_data.password != bre_user["password"]
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )

    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    token = jwt.encode(
        {"sub": form_data.username, "exp": expire},
        SECRET_KEY,
        algorithm=ALGORITHM
    )

    return {
        "access_token": token,
        "token_type": "bearer"
    }

# --------------------------------------------------
# PROTECTED BRE ATTRIBUTE API (USED IN RULES)
# --------------------------------------------------
@app.get("/bre/attributes/{application_id}")
def get_bre_attributes(
    application_id: str,
    current_user: str = Depends(get_current_user)
):
    conn = get_db()
    cur = conn.cursor()

    query = """
        SELECT
            b.legal_name,
            b.dob,
            b.country_of_residence,
            b.is_on_bank_blacklist,
            a.requested_amount,
            a.requested_tenor_months,
            COALESCE(SUM(i.amount_monthly), 0) AS total_income,
            COALESCE(SUM(l.monthly_installment), 0) AS total_emi,
            MAX(cr.credit_score) AS credit_score,
            EXISTS (
                SELECT 1 FROM default_history d
                WHERE d.application_id = a.application_id
            ) AS has_default_history,
            EXISTS (
                SELECT 1 FROM bankruptcies bk
                WHERE bk.application_id = a.application_id
            ) AS is_bankrupt
        FROM applications a
        JOIN borrowers b ON b.borrower_id = a.borrower_id
        LEFT JOIN incomes i ON i.application_id = a.application_id
        LEFT JOIN liabilities l ON l.application_id = a.application_id
        LEFT JOIN credit_reports cr ON cr.application_id = a.application_id
        WHERE a.application_id = %s
        GROUP BY
            b.legal_name,
            b.dob,
            b.country_of_residence,
            b.is_on_bank_blacklist,
            a.requested_amount,
            a.requested_tenor_months,
            a.application_id
    """

    cur.execute(query, (application_id,))
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        raise HTTPException(
            status_code=404,
            detail="Application not found"
        )

    (
        legal_name,
        dob,
        country,
        is_blacklisted,
        requested_amount,
        tenor,
        total_income,
        total_emi,
        credit_score,
        has_default,
        is_bankrupt
    ) = row

    foir = (total_emi / total_income) if total_income > 0 else 0

    return {
        "legal_name": legal_name,
        "dob": dob,
        "country_of_residence": country,
        "is_on_bank_blacklist": is_blacklisted,
        "requested_amount": float(requested_amount),
        "requested_tenor_months": tenor,
        "total_income": float(total_income),
        "total_emi": float(total_emi),
        "foir": round(foir, 4),
        "credit_score": credit_score,
        "has_default_history": has_default,
        "is_bankrupt": is_bankrupt
    }
