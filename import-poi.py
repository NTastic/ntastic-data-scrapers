import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
import json
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["ntastic_backend_dev"]  # Replace with your database name
pois_collection = db["poi.pois"]  # Replace if your collection name is different
categories_collection = db["poi.categories"]  # Categories collection


# Function to get category IDs from 'type' and 'subtype'
def get_cat_ids(type_name, subtype_name):
    cat_ids = []
    if type_name:
        type_cat = categories_collection.find_one(
            {"name": {"$regex": f"^{re.escape(type_name.strip())}$", "$options": "i"}}
        )
        if type_cat:
            cat_ids.append(type_cat["_id"])
        else:
            logger.warning(f"Category '{type_name}' not found.")
    if subtype_name:
        subtype_cat = categories_collection.find_one(
            {
                "name": {
                    "$regex": f"^{re.escape(subtype_name.strip())}$",
                    "$options": "i",
                }
            }
        )
        if subtype_cat:
            cat_ids.append(subtype_cat["_id"])
        else:
            logger.warning(f"Subcategory '{subtype_name}' not found.")
    return cat_ids


# List of Excel files to process
excel_files = [
    "data/place_attrattion.xlsx",
    "data/place_hotels.xlsx",
    "data/place_park_museum.xlsx",
    "data/place_restaurant.xlsx",
    "data/place_shopping.xlsx",
]  # Add your Excel file names here


def get_and_strip(row, *labels):
    result = []
    for label in labels:
        data = row.get(label, "")
        if pd.isna(data):
            result.append(None)
            continue
        if isinstance(data, str):
            data = data.strip()
        result.append(data)
    return result


# Process each Excel file
for file_name in excel_files:
    # Read Excel file
    df = pd.read_excel(file_name)

    # Iterate over each row
    for index, row in df.iterrows():
        # Read data from row
        (
            name,
            phone,
            address,
            latitude,
            longitude,
            type_name,
            subtype_name,
            rating,
            reviews_count,
            review_summary,
            photo_url,
            street_view_url,
            working_hours,
            website,
        ) = get_and_strip(
            row,
            "name",
            "phone",
            "full_address",
            "latitude",
            "longitude",
            "type",
            "subtype",
            "rating",
            "reviews",
            "review_summary",
            "photo",
            "street_view",
            "working_hours",
            "site",
        )
        # name = str(row.get('name', '')).strip()
        # phone = str(row.get('phone', '')).strip()
        # address = str(row.get('full_address', '')).strip()
        # latitude = row.get('latitude')
        # longitude = row.get('longitude')
        # type_name = str(row.get('type', '')).strip()
        # subtype_name = str(row.get('subtype', '')).strip()
        # rating = row.get('rating', 0)
        # reviews_count = row.get('reviews', 0)
        # photo_url = str(row.get('photo', '')).strip()
        # street_view_url = str(row.get('street_view', '')).strip()
        # working_hours = str(row.get('working_hours', '')).strip()
        # website = str(row.get('site', '')).strip()

        # Handle missing required fields
        if not name:
            logger.warning(f"Missing name for POI at index {index}. Skipping.")
            continue  # Skip this POI

        # Get category IDs
        cat_ids = get_cat_ids(type_name, subtype_name)
        if not cat_ids:
            logger.warning(f"No category IDs found for POI '{name}'. Skipping.")
            continue  # Skip this POI

        # Validate and convert latitude and longitude
        try:
            latitude = float(latitude)
            longitude = float(longitude)
        except (ValueError, TypeError):
            logger.warning(f"Invalid latitude or longitude for POI '{name}'. Skipping.")
            continue  # Skip this POI

        # Parse working_hours JSON string
        working_hours_list = []
        if working_hours:
            try:
                working_hours_dict = json.loads(working_hours)
                for day, time in working_hours_dict.items():
                    working_hours_list.append({"day": day, "time": time})
            except json.JSONDecodeError:
                logger.warning(
                    f"Invalid JSON in 'working_hours' for POI '{name}': {working_hours}"
                )
                # Proceed without working hours

        # Build location GeoJSON
        location = {
            "type": "Point",
            "coordinates": [
                longitude,
                latitude,
            ],  # Note: GeoJSON uses [longitude, latitude]
        }

        # Build photoUrls list
        photo_urls = []
        if photo_url and not pd.isna(photo_url):
            photo_urls.append(photo_url)
        if street_view_url and not pd.isna(street_view_url):
            photo_urls.append(street_view_url)

        # Convert rating and reviewsCount to appropriate types
        rating = pd.to_numeric(rating, errors="coerce")
        if pd.isna(rating):
            rating = 0.0
        reviews_count = pd.to_numeric(reviews_count, errors="coerce")
        if pd.isna(reviews_count):
            reviews_count = 0

        # Build POI document
        poi = {
            "name": name,
            "phone": phone,
            "address": address,
            "location": location,
            "catIds": cat_ids,
            "rating": float(rating),
            "reviewsCount": int(reviews_count),
            "reviewSummary": review_summary,
            "photoUrls": photo_urls,
            "workingHours": working_hours_list,
            "website": website,
            "votes": {"upvotes": 0, "downvotes": 0},
            "createdAt": pd.Timestamp.utcnow(),
            "updatedAt": pd.Timestamp.utcnow(),
        }

        # Insert POI into MongoDB
        pois_collection.insert_one(poi)

print("POI data has been successfully inserted into the database.")
