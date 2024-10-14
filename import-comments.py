import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
import random
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["ntastic_backend_dev"]  # Replace with your database name
comments_collection = db["poi.comments"]  # Comments collection
pois_collection = db["poi.pois"]  # POIs collection
users_collection = db["common.users"]  # Users collection

# Read POI Excel file to map place_id to POI name
poi_excel_files = [
    "data/place_attrattion.xlsx",
    "data/place_hotels.xlsx",
    "data/place_park_museum.xlsx",
    "data/place_restaurant.xlsx",
    "data/place_shopping.xlsx",
]  # Replace with your POI Excel file name

# Create a mapping from place_id to POI name
place_id_to_name = {}
for poi_file in poi_excel_files:
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


# Comments Excel file
comments_excel_files = [
    "data/comments_attrattion.xlsx",
    "data/comments_hotels.xlsx",
    "data/comments_place_park.xlsx",
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
        # place_id = str(row.get("place_id", "")).strip()
        # content = str(row.get("review_text", "")).strip()
        # rating = row.get("rating", 5)
        # review_img_urls = str(row.get("review_img_urls", "")).strip()
        # review_timestamp = row.get("review_timestamp", "").strip()

        # Validate required fields
        if not place_id:
            logger.warning(f"Missing 'place_id' at index {index}. Skipping.")
            continue
        if not content:
            logger.warning(
                f"Missing 'review_text' for place_id '{place_id}'. Skipping."
            )
            continue

        # Get the POI name from the mapping
        poi_name = place_id_to_name.get(place_id)
        if not poi_name:
            logger.warning(f"POI name not found for place_id '{place_id}'. Skipping.")
            continue

        # Get the poiId from MongoDB using the POI name
        poi = pois_collection.find_one(
            {"name": {"$regex": f"^{poi_name}$", "$options": "i"}}, {"_id": 1}
        )
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
