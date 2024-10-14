from pymongo import MongoClient
from faker import Faker
from bson.objectid import ObjectId
import bcrypt
import random
import datetime
import csv

# Replace with your MongoDB connection string and database name
client = MongoClient("mongodb://localhost:27017/")
db = client["ntastic_backend_dev"]  # Replace with your database name
users_collection = db["common.users"]  # Replace with your collection name if different

fake = Faker()

def fake_users(count):
    users = []
    credentials = []

    for _ in range(count):
        username = fake.user_name()
        email = fake.unique.ascii_free_email()
        password_plain = fake.password(length=10)
        password_hashed = bcrypt.hashpw(
            password_plain.encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")
        phone = fake.phone_number()
        is_bot = False  # Set to True if you want some users to be bots
        # storage_used = random.randint(0, 1000000)  # Random storage used in bytes
        storage_used = 0

        # Generate random ObjectIds
        # avatar_id = ObjectId()
        # char_ids = [ObjectId() for _ in range(random.randint(0, 5))]
        # fave_cat_ids = [ObjectId() for _ in range(random.randint(0, 5))]
        # fave_sub_cat_ids = [ObjectId() for _ in range(random.randint(0, 5))]

        # Generate refresh tokens
        # refresh_tokens = []
        # for _ in range(random.randint(1, 3)):
        #     token = fake.sha256()
        #     expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=30)
        #     created_at = datetime.datetime.now(datetime.timezone.utc)
        #     device_info = fake.user_agent()
        #     refresh_tokens.append(
        #         {
        #             "token": token,
        #             "expiresAt": expires_at,
        #             "createdAt": created_at,
        #             "deviceInfo": device_info,
        #         }
        #     )

        user = {
            "username": username,
            "email": email,
            "password": password_hashed,
            "phone": phone,
            "isBot": is_bot,
            # "avatarId": avatar_id,
            "storageUsed": storage_used,
            # "charIds": char_ids,
            # "faveCatIds": fave_cat_ids,
            # "faveSubCatIds": fave_sub_cat_ids,
            # "refreshTokens": refresh_tokens,
            "createdAt": datetime.datetime.now(datetime.timezone.utc),
            "updatedAt": datetime.datetime.now(datetime.timezone.utc),
            # Include the plain password for testing purposes
            "plainPassword": password_plain,  # Remove this in production!
        }

        users.append(user)

        # Save credentials for reference
        credentials.append(
            {"username": username, "email": email, "password": password_plain}
        )

    # Insert the users into MongoDB
    result = users_collection.insert_many(users)
    print(f"Inserted {len(result.inserted_ids)} users into the database.")

    # Save the credentials to a CSV file
    with open("user_credentials.csv", mode="w", newline="") as csv_file:
        fieldnames = ["username", "email", "password"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        for cred in credentials:
            writer.writerow(cred)

    print("User credentials have been saved to user_credentials.csv")


def main():
    fake_users(50)


if __name__ == "__main__":
    main()
