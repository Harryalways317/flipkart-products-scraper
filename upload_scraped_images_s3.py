import json
import boto3
import requests
import io
from concurrent.futures import ThreadPoolExecutor
import ast

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

# S3 bucket name, DynamoDB table name, and JSON file path
s3_bucket = 'product-images-flipk'
dynamodb_table = 'product-images-flipk'
output_json_file_path = 'generated/json_fk_products_images_mappings.json'
input_json_file = 'generated/json_fk_products_scraped_data.json'

def download_and_upload_images(product_id, image_urls, s3_bucket):
    original_s3_urls = []
    thumbnail_s3_urls = []

    for index, image_url in enumerate(image_urls):
        print(image_url)
        try:
            response = requests.get(image_url)
            if response.status_code == 200:
                image_data = response.content

                # Upload to original folder within the bucket
                original_object_key = f"original/{product_id}/{index}.jpg"
                s3.upload_fileobj(
                    io.BytesIO(image_data),
                    s3_bucket,
                    original_object_key,
                )
                original_s3_url = f"https://{s3_bucket}.s3.amazonaws.com/{original_object_key}"
                original_s3_urls.append(original_s3_url)

                # Upload to thumbnail folder within the bucket
                thumbnail_object_key = f"thumbnail/{product_id}/{index}.jpg"
                s3.upload_fileobj(
                    io.BytesIO(image_data),
                    s3_bucket,
                    thumbnail_object_key,
                )
                thumbnail_s3_url = f"https://{s3_bucket}.s3.amazonaws.com/{thumbnail_object_key}"
                thumbnail_s3_urls.append(thumbnail_s3_url)
            else:
                print(f"Failed to download image: {image_url}")
        except Exception as e:
            print(f"Error: {str(e)}")

    return original_s3_urls, thumbnail_s3_urls

def store_mappings_in_dynamodb(product_id, original_s3_urls, thumbnail_s3_urls, dynamodb_table):
    try:
        item = {
            'ProductID': {'S': product_id},
            'OriginalImageURLs': {'SS': original_s3_urls},
            'ThumbnailImageURLs': {'SS': thumbnail_s3_urls}
        }
        dynamodb.put_item(TableName=dynamodb_table, Item=item)
    except Exception as e:
        print(f"Error storing mappings in DynamoDB: {str(e)}")

def save_mappings_to_json(mappings, output_json_file_path):
    with open(output_json_file_path, 'w') as json_file:
        json.dump(mappings, json_file, indent=4)
errors = {}
def main():
    with open(input_json_file, 'r') as json_file:
        json_data = json.load(json_file)
    reversed_json_data = {k: v for k, v in list(reversed(list(json_data.items())))[900:]}
    json_data = reversed_json_data
    mappings = {}

    # Configure thread pool
    executor = ThreadPoolExecutor(max_workers=30)

    for product_id, product_data in json_data.items():
        try:
            print(product_id, product_data)
            original_images_str = product_data.get("Original Images")
            thumbnail_images_str = product_data.get("Thumbnail Images")

            # Checking if the values are "nan" and provide default empty lists
            original_image_urls = ast.literal_eval(original_images_str) if original_images_str and original_images_str != "nan" else []
            thumbnail_image_urls = ast.literal_eval(thumbnail_images_str) if thumbnail_images_str and thumbnail_images_str != "nan" else []
            print(product_id, original_image_urls, thumbnail_image_urls)

            # Using multithreading to download and upload original images concurrently
            original_s3_urls, _ = download_and_upload_images(
                product_id, original_image_urls, s3_bucket
            )

            # Using multithreading to download and upload thumbnail images concurrently
            _, thumbnail_s3_urls = download_and_upload_images(
                product_id, thumbnail_image_urls, s3_bucket
            )


            # Storing mappings in DynamoDB
            store_mappings_in_dynamodb(
                product_id, original_s3_urls, thumbnail_s3_urls, dynamodb_table
            )

            # Storing mappings in the dictionary
            mappings[product_id] = {
                "OriginalImageURLs": original_s3_urls,
                "ThumbnailImageURLs": thumbnail_s3_urls
            }
        except Exception as e:
            # Handling the error and Storing it for retry
            print(f"Error processing product {product_id}: {str(e)}")
            errors[product_id] = str(e)

    # Save mappings to a JSON file
    save_mappings_to_json(mappings, output_json_file_path)

    # Shutting down the thread pool
    executor.shutdown()
    print(errors)

if __name__ == "__main__":
    main()
