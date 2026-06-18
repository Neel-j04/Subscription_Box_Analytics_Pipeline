import json
import pandas as pd
from pymongo import MongoClient

def extract():
    print("Starting Extract Phase...")
    records = []

    file_path = "/opt/airflow/data/Subscription_Boxes.jsonl"
    #file_path = r"D:\Projects\Cloud Data Engineer Projects\Project_3_Subscription_Box_Analytics_Pipeline\Project_3_Airflow_Pipeline\data\Subscription_Boxes.jsonl"
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            records.append(json.loads(line))

    print(f"Extracted {len(records)} records")
    return records

def transform(records):
    print("\nStarting Transform Phase...")

    df = pd.DataFrame(records)

    print("\nAvailable Columns:")
    print(df.columns.tolist())

    df = df[
        [
            "rating",
            "title",
            "text",
            "helpful_vote",
            "verified_purchase",
            "timestamp"
        ]
    ]

    df["review_date"] = pd.to_datetime(
        df["timestamp"],
        unit="ms"
    )

    def get_sentiment(rating):
        if rating >= 4:
            return "Positive"
        elif rating == 3:
            return "Neutral"
        else:
            return "Negative"

    df["sentiment"] = df["rating"].apply(get_sentiment)

    df.drop(columns=["timestamp"], inplace=True)
    df.dropna(subset=["text"], inplace=True)

    print(f"Records after cleaning: {len(df)}")
    print("\nTransformation Completed!")
    return df


def load(df):
    print("\nStarting MongoDB Load Phase...")

    try:
        client = MongoClient("mongodb://localhost:27017")
        client.admin.command("ping")
        print("Connected via localhost")

    except:
        client = MongoClient("mongodb://mongodb:27017")
        print("Connected via Docker hostname")

    db = client["subscription_db"]
    collection = db["reviews"]
    collection.delete_many({})
    records = df.to_dict("records")
    collection.insert_many(records)

    print(f"Inserted {len(records)} records into MongoDB")

#    print("\nStarting Load Phase...")
#
#   output_file = "/opt/airflow/output/processed_reviews.csv"
#
#    df.to_csv(
#        output_file,
#        index=False
#    )
#
#    print(f"Data saved successfully!")
#    print(f"Location: {output_file}")

if __name__ == "__main__":
    records = extract()
    transformed_df = transform(records)
    load(transformed_df)
    print("\nETL Pipeline Completed Successfully!")