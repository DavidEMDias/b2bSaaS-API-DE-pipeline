import requests
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

API_KEY = os.getenv("API_KEY")
url = "http://localhost:8000/customers"

payload = {
    "company_name": "Awesom232132saddasd1e Co",
    "country": "US",
    "industry": "SaaS",
    "company_size": "11-50",
    "is_churned": False
}

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)
print(response.json())