import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
import re

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["ntastic_backend_dev"]  # Replace with your database name
categories_collection = db[
    "poi.categories"
]  # Replace if your collection name is different


# Function to generate slug from name
def generate_slug(name):
    slug = re.sub(r"\s+", "-", name.strip().lower())
    slug = re.sub(r"[^\w\-]", "", slug)
    return slug


# Cache to store category names and their ObjectIds to avoid duplicates
category_cache = {}


# Function to create or get a category
def get_or_create_category(name, cache, parent_id=None, sub=True):
    name_lower = name.strip().lower()
    if name_lower in cache:
        return cache[name_lower]
    else:
        # Check if the category exists in the database
        existing_category = categories_collection.find_one({"name": name})
        if existing_category:
            category_id = existing_category["_id"]
            if not sub:
                categories_collection.update_one(
                    {"_id": existing_category["_id"]}, {"$set": {"parentCatId": None}}
                )
        else:
            # Create new category
            slug = generate_slug(name)
            category = {
                "name": name.strip(),
                "slug": slug,
                "description": "",
                "parentCatId": parent_id,
                "subCatIds": [],
                "poiCount": 0,
                "createdAt": pd.Timestamp.utcnow(),
                "updatedAt": pd.Timestamp.utcnow(),
            }
            category_id = categories_collection.insert_one(category).inserted_id

            # Update parent's subCatIds if parent exists
            if parent_id:
                categories_collection.update_one(
                    {"_id": parent_id}, {"$addToSet": {"subCatIds": category_id}}
                )

        # Add to cache
        cache[name_lower] = category_id
        return category_id


# List of Excel files to process
excel_files = [
    "data/place_attrattion.xlsx",
    "data/place_hotels.xlsx",
    "data/place_restaurant.xlsx",
    "data/place_shopping.xlsx",
]  # Add more file names to this list


def get_name_from_file(file_name):
    name = ""
    # Special handling for 'place_restaurant.xlsx'
    if "place_attrattion.xlsx" in file_name:
        name = "Attraction"
    elif "place_hotels.xlsx" in file_name:
        name = "Hotel"
    elif "place_restaurant.xlsx" in file_name:
        name = "Restaurant"
    elif "place_shopping.xlsx" in file_name:
        name = "Shopping"
    else:
        raise Exception(f"unknow file {file_name}")
    return name


# Process each Excel file
for file_name in excel_files:
    # Read Excel file
    df = pd.read_excel(file_name)

    # Iterate over each row
    for index, row in df.iterrows():
        # Read 'type' and 'subtypes'
        type_name = str(row.get("type", "")).strip()
        subtypes = str(row.get("subtypes", "")).strip()

        if not type_name:
            continue  # Skip if 'type' is empty

        name = get_name_from_file(file_name)

        cache = category_cache[name] = {}
        cat_id = get_or_create_category(name, cache, sub=False)
        parent_cat_id = get_or_create_category(type_name, cache, cat_id)
        # Process 'subtypes'
        if subtypes:
            subtype_list = [subtype.strip() for subtype in subtypes.split(",")]
            for subtype in subtype_list:
                if not subtype:
                    continue
                # Create or get subtype category with parentCatId
                get_or_create_category(subtype, cache, parent_cat_id)


import json
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pois_collection = db["poi.pois"]  # Replace if your collection name is different


# Function to get category IDs from 'type' and 'subtype'
def get_cat_ids(file_name, type_name, subtypes):
    cat_ids = []
    if not type_name:
        return
    name = get_name_from_file(file_name)

    cache = category_cache[name] = {}
    cat_id = get_or_create_category(name, cache, sub=False)
    if not cat_id:
        logger.warning(f"Category '{type_name}' not found.")
        return
    cat_ids.append(cat_id)
    parent_cat_id = get_or_create_category(type_name, cache, cat_id)
    if parent_cat_id and parent_cat_id not in cat_ids:
        cat_ids.append(parent_cat_id)
    # Process 'subtypes'
    if subtypes:
        subtype_list = [subtype.strip() for subtype in subtypes.split(",")]
        for subtype in subtype_list:
            if not subtype:
                continue
            # Create or get subtype category with parentCatId
            sub_cat_id = get_or_create_category(subtype, cache, parent_cat_id)
            if sub_cat_id:
                if sub_cat_id not in cat_ids:
                    cat_ids.append(sub_cat_id)
            else:
                logger.warning(f"Subcategory '{subtype}' not found.")

    # cat_ids = []
    # if type_name:
    #     type_cat = categories_collection.find_one({"name": type_name})
    #     if type_cat:
    #         cat_ids.append(type_cat["_id"])
    #     else:
    #         logger.warning(f"Category '{type_name}' not found.")
    # if subtype_name:
    #     subtype_cat = categories_collection.find_one({"name": subtype_name})
    #     if subtype_cat:
    #         cat_ids.append(subtype_cat["_id"])
    #     else:
    #         logger.warning(f"Subcategory '{subtype_name}' not found.")
    return cat_ids


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

        # Handle missing required fields
        if not name:
            logger.warning(f"Missing name for POI at index {index}. Skipping.")
            continue  # Skip this POI

        # Get category IDs
        cat_ids = get_cat_ids(file_name, type_name, subtype_name)
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


import random


comments_collection = db["poi.comments"]  # Comments collection
users_collection = db["common.users"]  # Users collection

# Create a mapping from place_id to POI name
place_id_to_name = {}
for poi_file in excel_files:
    poi_df = pd.read_excel(poi_file)

    for index, row in poi_df.iterrows():
        place_id = str(row.get("place_id", "")).strip()
        name = str(row.get("name", "")).strip()
        if place_id and name:
            place_id_to_name[place_id] = name

# Read the list of user IDs from the users collection
user_ids = list(users_collection.find({}, {"_id": 1}))
user_id_list = [user["_id"] for user in user_ids]

if not user_id_list:
    logger.error("No users found in the users collection.")
    exit(1)


# Comments Excel file
comments_excel_files = [
    "data/comments_attrattion.xlsx",
    "data/comments_hotels.xlsx",
    "data/comments_restaurant.xlsx",
    "data/comments_shopping.xlsx",
]  # Replace with your Comments Excel file name

for comments_file in comments_excel_files:
    comments_df = pd.read_excel(comments_file)
    # Process each comment
    for index, row in comments_df.iterrows():
        (place_id, content, rating, review_img_urls, review_timestamp) = get_and_strip(
            row,
            "place_id",
            "review_text",
            "rating",
            "review_img_urls",
            "review_timestamp",
        )

        # Validate required fields
        if not place_id:
            logger.warning(f"Missing 'place_id' at index {index}. Skipping.")
            continue
        if not content:
            # logger.warning(
            #     f"Missing 'review_text' for place_id '{place_id}'. Skipping."
            # )
            continue

        # Get the POI name from the mapping
        poi_name = place_id_to_name.get(place_id)
        if not poi_name:
            logger.warning(f"POI name not found for place_id '{place_id}'. Skipping.")
            continue

        # Get the poiId from MongoDB using the POI name
        poi = pois_collection.find_one({"name": name}, {"_id": 1})
        if not poi:
            logger.warning(f"POI '{poi_name}' not found in MongoDB. Skipping.")
            continue
        poi_id = poi["_id"]

        # Randomly select an authorId from the user IDs
        author_id = random.choice(user_id_list)

        # Parse rating
        try:
            rating = float(rating)
            if rating < 0 or rating > 5:
                logger.warning(
                    f"Invalid rating '{rating}' for comment at index {index}. Setting to default 5."
                )
                rating = 5
        except (ValueError, TypeError):
            logger.warning(
                f"Invalid rating '{rating}' for comment at index {index}. Setting to default 5."
            )
            rating = 5

        # Parse imageUrls
        image_urls = []
        if review_img_urls:
            image_urls = [
                url.strip() for url in review_img_urls.split(",") if url.strip()
            ]

        # Parse review_timestamp
        if review_timestamp:
            try:
                # Assuming the timestamp is in a standard format, e.g., 'YYYY-MM-DD HH:MM:SS'
                review_datetime = pd.to_datetime(review_timestamp)
            except (ValueError, TypeError):
                logger.warning(
                    f"Invalid 'review_timestamp' '{review_timestamp}' for comment at index {index}. Using current UTC time."
                )
                review_datetime = pd.Timestamp.utcnow()
        else:
            review_datetime = pd.Timestamp.utcnow()
        # Build the comment document
        comment = {
            "poiId": poi_id,
            "authorId": author_id,
            "content": content,
            "rating": rating,
            "imageUrls": image_urls,
            "votes": {"upvotes": 0, "downvotes": 0},
            "createdAt": review_datetime,
            "updatedAt": review_datetime,
        }

        # Insert the comment into MongoDB
        comments_collection.insert_one(comment)

        # logger.info(f"Inserted comment for POI '{poi_name}' by user '{author_id}'.")

print("Comment data has been successfully inserted into the database.")
