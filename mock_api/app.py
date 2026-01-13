from flask import Flask, request, jsonify
from dateutil import parser
import os, time, random, uuid, threading
from generator import DataStore
from datetime import datetime, timedelta, timezone


COUNTRIES = ["US","GB","DE","FR","CA","AU","NL","PT","IT","ES"]
EUR_COUNTRIES = ["DE","FR","NL","PT","IT","ES"]
COUNTRY_W = [0.35,0.15,0.12,0.07,0.08,0.06,0.06,0.04,0.04,0.03]
INDUSTRIES = ["E-commerce","SaaS","Consulting","Education","Health","Finance"]
IND_W = [0.35,0.25,0.12,0.1,0.1,0.08]
PRODUCTS = ["Basic","Pro","Enterprise","Addon-Analytics","Addon-Support"]
PRICES = {"Basic":29,"Pro":99,"Enterprise":499,"Addon-Analytics":49,"Addon-Support":99}
PAYMENT_METHODS=["card","bank_transfer","paypal","apple_pay","google_pay"]
PM_W=[0.6,0.15,0.15,0.05,0.05]
STATUSES=["succeeded","failed","refunded"]
STAT_W=[0.9,0.06,0.04]
SOURCES=["google","direct","facebook","linkedin","newsletter","referral","bing"]
SRC_W=[0.45,0.18,0.12,0.08,0.07,0.06,0.04]
MEDIUMS=["organic","cpc","email","social","none","referral"] # cpc = paid ads
MED_W=[0.5,0.18,0.1,0.12,0.06,0.04]
DEVICES=["desktop","mobile","tablet"]
DEV_W=[0.55,0.4,0.05]

TZ = timezone.utc

API_KEY = os.environ["API_KEY"]
RATE = int(os.environ.get("API_RATE_LIMIT_PER_MIN", "60"))
WINDOW = 60

app = Flask(__name__)
store = DataStore()

# """ # thread-safe in-memory rate limiter
# lock = threading.Lock()
# last_reset = time.time()
# count = 0

# def check_rate_limit():
#     global last_reset, count
#     now = time.time()
#     with lock:
#         if now - last_reset > WINDOW:
#             last_reset = now
#             count = 0
#         count += 1
#         if count > RATE:
#             return False
#         return True """


# def require_auth():
#     auth = request.headers.get("Authorization", "")
#     if not auth.startswith("Bearer ") or auth.split(" ",1)[1] != API_KEY:
#         return False
#     return True

# Testing
def require_auth():
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth.split(" ",1)[1] == API_KEY
    
    # fallback para query string (somente para teste)
    key = request.args.get("api_key")
    if key:
        return key == API_KEY
    
    return False


# """ def maybe_chaos():
#     # ~2% internal errors
#     if random.random() < 0.02:
#         resp = jsonify({"error":"internal_error","id":str(uuid.uuid4())})
#         resp.status_code = 500
#         return resp
#     return None """


def paginate(items, page, page_size):
    total = len(items)
    total_pages = (total + page_size - 1) // page_size
    start = (page-1)*page_size
    end = start + page_size
    data = items[start:end]
    next_page = page+1 if page < total_pages else None
    return {
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "next_page": next_page,
        "count": len(data),
        "data": data,
    }

@app.get("/health")
def health():
    return {"status":"ok"}

@app.get("/customers")
@app.get("/payments")
@app.get("/sessions")
def list_resources():
    if not require_auth():
        return jsonify({"error":"unauthorized"}), 401
    # if not check_rate_limit():
    #     return jsonify({"error":"rate_limited","retry_after":30}), 429
    # chaos = maybe_chaos()
    # if chaos:
    #     return chaos

    path = request.path.strip("/")
    if path == "customers":
        items = store.customers
    elif path == "payments":
        items = store.payments
    else:
        items = store.sessions

    # filtering (minimal examples)
    qs = request.args

    updated_since = qs.get("updated_since") # Will be used for incremental load
    if updated_since:
        ts = parser.isoparse(updated_since)
        items = [i for i in items if parser.isoparse(i.get("updated_at") or i.get("created_at") or i.get("session_start")) >= ts]

    # example filters
    status = qs.get("status")
    if status and path == "payments":
        items = [i for i in items if i["status"] == status]

    country = qs.get("country")
    if country:
        items = [i for i in items if i.get("country") == country]

    source = qs.get("source")
    if source and path == "sessions":
        items = [i for i in items if i.get("source") == source]

    page = int(qs.get("page", 1))
    page_size = min(int(qs.get("page_size", 500)), 1000)

    return jsonify(paginate(items, page, page_size))


@app.post("/customers")
@app.post("/payments")
@app.post("/sessions")
def add_resource():

    # 1 Authentication
    if not require_auth():
        return jsonify({"error": "unauthorized"}), 401
    
    # 2 Determine the resource type based on the request path
    path = request.path.strip("/")

    # 3 Get the JSON payload from the request body
    data = request.json or {}

    now_iso = datetime.now(TZ).isoformat()

    # 4 Create the object depending on the resource type
    if path == "customers":
        # Create a new customer
        item = {
            "customer_id": str(uuid.uuid4()),
            "company_name": data.get("company_name", f"Company {str(uuid.uuid4())[:8]}"),
            "country": data.get("country", random.choices(COUNTRIES, COUNTRY_W)[0]),
            "industry": data.get("industry", random.choices(INDUSTRIES, IND_W)[0]),
            "company_size": data.get("company_size", random.choices(
                ["1-10","11-50","51-200","201-500","500+"],[0.35,0.3,0.2,0.1,0.05])[0]
            ),
            "signup_date": data.get("signup_date", now_iso),
            "updated_at": data.get("updated_at", now_iso),
            "is_churned": data.get("is_churned", False),
        }
        store.customers.append(item)

    elif path == "payments":
        # Create a new payment
        # If customer_id not provided, pick a random one
        cust = data.get("customer_id") or random.choice(store.customers)["customer_id"]
        product = data.get("product") or random.choice(PRODUCTS)
        amount = data.get("amount") or PRICES[product]
        currency = data.get("currency") or "USD"
        status = data.get("status") or "succeeded"
        refunded = data.get("refunded_amount") or 0.0
        fee = data.get("fee") or round(amount*0.029+0.3,2)
        payment_method = data.get("payment_method") or random.choices(PAYMENT_METHODS, PM_W)[0]
        country = data.get("country") or random.choice(COUNTRIES)

        item = {
            "payment_id": str(uuid.uuid4()),
            "customer_id": cust,
            "product": product,
            "amount": float(amount),
            "currency": currency,
            "status": status,
            "refunded_amount": refunded,
            "fee": fee,
            "payment_method": payment_method,
            "country": country,
            "created_at": data.get("created_at", now_iso),
            "updated_at": data.get("updated_at", now_iso),
        }
        store.payments.append(item)

    else:  # sessions
        # Create a new session
        cust = data.get("customer_id") or random.choice(store.customers)["customer_id"]
        pageviews = data.get("pageviews") or max(1, int(random.expovariate(1/2)))
        duration = data.get("session_duration_s") or int(random.gammavariate(2,45))
        bounced = data.get("bounced") or int(pageviews == 1 and duration < 10)
        conv_prob = 0.02
        medium = data.get("medium") or random.choices(MEDIUMS, MED_W)[0]
        if medium=="cpc": conv_prob += 0.02
        if medium=="email": conv_prob += 0.03
        device = data.get("device") or random.choices(DEVICES, DEV_W)[0]
        if device=="desktop": conv_prob += 0.01
        if pageviews >= 3: conv_prob += 0.01
        converted = data.get("converted") or int(conv_prob > random.random())

        item = {
            "session_id": str(uuid.uuid4()),
            "customer_id": cust,
            "source": data.get("source") or random.choices(SOURCES, SRC_W)[0],
            "medium": medium,
            "campaign": data.get("campaign") or ("" if medium in ("organic","none","referral") else "product_launch"),
            "device": device,
            "country": data.get("country") or random.choices(COUNTRIES, COUNTRY_W)[0],
            "pageviews": pageviews,
            "session_duration_s": duration,
            "bounced": bounced,
            "converted": converted,
            "session_start": data.get("session_start") or now_iso,
            "updated_at": data.get("updated_at") or now_iso,
        }
        store.sessions.append(item)

    # 5️ Return the new item
    return jsonify(item), 201



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)