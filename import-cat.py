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
def get_or_create_category(name, parent_id=None):
    name_lower = name.strip().lower()
    if name_lower in category_cache:
        return category_cache[name_lower]
    else:
        # Check if the category exists in the database
        existing_category = categories_collection.find_one(
            {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}}
        )
        if existing_category:
            category_id = existing_category["_id"]
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
        category_cache[name_lower] = category_id
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

        # Special handling for 'place_restaurant.xlsx'
        if "place_attrattion.xlsx" in file_name:
            # Create or get 'Restaurant' category
            restaurant_cat_id = get_or_create_category("Attraction")
            # Use 'type' category as parentCatId
            parent_cat_id = get_or_create_category(
                type_name, parent_id=restaurant_cat_id
            )
        elif "place_hotels.xlsx" in file_name:
            # Create or get 'Restaurant' category
            restaurant_cat_id = get_or_create_category("Hotel")
            # Use 'type' category as parentCatId
            parent_cat_id = get_or_create_category(
                type_name, parent_id=restaurant_cat_id
            )
        elif "place_park_museum.xlsx" in file_name:
            # Create or get 'Restaurant' category
            restaurant_cat_id = get_or_create_category("Park Museum")
            # Use 'type' category as parentCatId
            parent_cat_id = get_or_create_category(
                type_name, parent_id=restaurant_cat_id
            )
        elif "place_restaurant.xlsx" in file_name:
            # Create or get 'Restaurant' category
            restaurant_cat_id = get_or_create_category("Restaurant")
            # Use 'type' category as parentCatId
            parent_cat_id = get_or_create_category(
                type_name, parent_id=restaurant_cat_id
            )
        elif "place_shopping.xlsx" in file_name:
            # Create or get 'Restaurant' category
            restaurant_cat_id = get_or_create_category("Shopping")
            # Use 'type' category as parentCatId
            parent_cat_id = get_or_create_category(
                type_name, parent_id=restaurant_cat_id
            )
        else:
            # Create or get 'type' category
            parent_cat_id = get_or_create_category(type_name)

        # Process 'subtypes'
        if subtypes:
            subtype_list = [subtype.strip() for subtype in subtypes.split(",")]
            for subtype in subtype_list:
                if not subtype:
                    continue
                # Create or get subtype category with parentCatId
                get_or_create_category(subtype, parent_id=parent_cat_id)
