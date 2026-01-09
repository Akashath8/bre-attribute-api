from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt
from datetime import datetime, timedelta
import psycopg2

# ======================================================
# APP INIT
# ======================================================
app = FastAPI(title="BRE Single Attribute API")

# ======================================================
# DB CONFIG (UPDATE)
# ======================================================
DB_CONFIG = {
    "host": "ep-blue-math-a19txkvb-pooler.ap-southeast-1.aws.neon.tech",  # 👈 Neon host
    "database": "neondb",                            # 👈 Neon DB name
    "user": "neondb_owner",                          # 👈 Neon user
    "password": "npg_HpxdhCbs29lJ",                  # 👈 Neon password
    "port": 5432,
    "sslmode": "require"                             # 👈 VERY IMPORTANT
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)

# ======================================================
# AUTH CONFIG
# ======================================================
SECRET_KEY = "BRE_SECRET_KEY_CHANGE_THIS"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

bre_user = {
    "username": "bre_engine",
    "password": "bre_password"
}

# ======================================================
# HEALTH
# ======================================================
@app.get("/")
def health():
    return {"status": "BRE API running"}

# ======================================================
# TOKEN
# ======================================================
@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if (
        form_data.username != bre_user["username"]
        or form_data.password != bre_user["password"]
    ):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    token = jwt.encode(
        {"sub": form_data.username, "exp": expire},
        SECRET_KEY,
        algorithm=ALGORITHM
    )

    return {"access_token": token, "token_type": "bearer"}

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return True
    except:
        raise HTTPException(status_code=401, detail="Invalid token")

# ======================================================
# 🚀 SINGLE BRE ATTRIBUTE API
# ======================================================
@app.get("/bre/attributes/{application_id}")
def get_all_attributes(
    application_id: str,
    user=Depends(get_current_user)
):
    conn = get_db()
    cur = conn.cursor()

    # ------------------------------
    # Borrower + Application
    # ------------------------------
    cur.execute("""
        SELECT
            b.legal_name,
            b.dob,
            b.country_of_residence,
            b.is_on_bank_blacklist,
            a.requested_amount,
            a.requested_tenor_months
        FROM applications a
        JOIN borrowers b ON b.borrower_id = a.borrower_id
        WHERE a.application_id = %s
    """, (application_id,))

    base = cur.fetchone()
    if not base:
        conn.close()
        raise HTTPException(status_code=404, detail="Application not found")

    # ------------------------------
    # Income aggregation
    # ------------------------------
    cur.execute("""
        SELECT COALESCE(SUM(amount_monthly), 0)
        FROM incomes
        WHERE application_id = %s
    """, (application_id,))
    total_income = cur.fetchone()[0]

    # ------------------------------
    # Liability aggregation
    # ------------------------------
    cur.execute("""
        SELECT COALESCE(SUM(monthly_installment), 0)
        FROM liabilities
        WHERE application_id = %s
    """, (application_id,))
    total_emi = cur.fetchone()[0]

    # ------------------------------
    # Credit score
    # ------------------------------
    cur.execute("""
        SELECT MAX(credit_score)
        FROM credit_reports
        WHERE application_id = %s
    """, (application_id,))
    credit_score = cur.fetchone()[0]

    # ------------------------------
    # Default & Bankruptcy flags
    # ------------------------------
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM default_history WHERE application_id = %s
        )
    """, (application_id,))
    has_default = cur.fetchone()[0]

    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM bankruptcies WHERE application_id = %s
        )
    """, (application_id,))
    is_bankrupt = cur.fetchone()[0]

    conn.close()

    # ------------------------------
    # Derived Metrics
    # ------------------------------
    foir = round(total_emi / total_income, 2) if total_income > 0 else 0

    # ------------------------------
    # FINAL BRE PAYLOAD
    # ------------------------------
    return {
        "legal_name": base[0],
        "dob": str(base[1]),
        "country_of_residence": base[2],
        "is_on_bank_blacklist": base[3],
        "requested_amount": base[4],
        "requested_tenor_months": base[5],
        "total_income": total_income,
        "total_emi": total_emi,
        "foir": foir,
        "credit_score": credit_score,
        "has_default_history": has_default,
        "is_bankrupt": is_bankrupt
    }
@app.get("/db-test")
def db_test():
    try:
        conn = get_db()
        conn.close()
        return {"db": "connected to Neon successfully"}
    except Exception as e:
        return {"db": "failed", "error": str(e)}
if __name__ == "__main__":
    uvicorn.run(...)
