import os

from common.geocoders.impl.geoapify import Geoapify
from dotenv import load_dotenv

load_dotenv()

geocoder = Geoapify(api_key=os.getenv("GEOAPIFY_SECRET"))
