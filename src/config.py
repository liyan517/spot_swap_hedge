from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("COINBASE_API_KEY")
API_SECRET = os.getenv("COINBASE_API_SECRET")

assert API_KEY and API_SECRET, "Missing COINBASE_API_KEY or COINBASE_API_SECRET in .env"
