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
    "data/place_park_museum.xlsx",
    "data/place_restaurant.xlsx",
    "data/place_shopping.xlsx",
]  # Add more file names to this list

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

        name = ""
        # Special handling for 'place_restaurant.xlsx'
        if "place_attrattion.xlsx" in file_name:
            name = "Attraction"
        elif "place_hotels.xlsx" in file_name:
            name = "Hotel"
        elif "place_park_museum.xlsx" in file_name:
            name = "Park Museum"
        elif "place_restaurant.xlsx" in file_name:
            name = "Restaurant"
        elif "place_shopping.xlsx" in file_name:
            name = "Shopping"
        else:
            raise Exception(f"unknow file {file_name}")
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
