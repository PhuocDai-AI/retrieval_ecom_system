import os
import json
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define the folder paths
DATA_FOLDER = "data"
IMAGE_FOLDER = os.path.join(DATA_FOLDER, "images")

# Create the images folder if it doesn't exist
if not os.path.exists(IMAGE_FOLDER):
    os.makedirs(IMAGE_FOLDER)

def download_image(sample):
    """Download an image from thumbnail_url and save it with the sample's id."""
    try:
        sample_id = sample.get("id")
        thumbnail_url = sample.get("thumbnail_url")
        
        if not sample_id or not thumbnail_url:
            print(f"Skipping sample with missing id or thumbnail_url: {sample_id}")
            return

        # Get the file extension from the URL
        parsed_url = urlparse(thumbnail_url)
        file_ext = os.path.splitext(parsed_url.path)[1] or ".jpg"  # Default to .jpg if no extension
        
        # Define the output file path
        output_path = os.path.join(IMAGE_FOLDER, f"{sample_id}{file_ext}")

        # Skip if the file already exists
        if os.path.exists(output_path):
            print(f"Image already exists for ID {sample_id}, skipping...")
            return

        # Download the image
        response = requests.get(thumbnail_url, stream=True, timeout=10)
        if response.status_code == 200:
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            print(f"Downloaded image for ID {sample_id} to {output_path}")
        else:
            print(f"Failed to download image for ID {sample_id}: Status {response.status_code}")
    except Exception as e:
        print(f"Error downloading image for ID {sample_id}: {str(e)}")

def process_json_file(json_file_path):
    """Process a single JSON file and extract samples for downloading images."""
    try:
        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Handle both single sample and list of samples
        samples = data if isinstance(data, list) else [data]
        
        # Process each sample
        for sample in samples:
            download_image(sample)
    except Exception as e:
        print(f"Error processing file {json_file_path}: {str(e)}")

def main():
    """Main function to process all JSON files in the data folder."""
    # Get all JSON files in the data folder
    json_files = [os.path.join(DATA_FOLDER, f) for f in os.listdir(DATA_FOLDER) 
                  if f.endswith(".json") and os.path.isfile(os.path.join(DATA_FOLDER, f))]
    
    if not json_files:
        print(f"No JSON files found in {DATA_FOLDER}")
        return

    print(f"Found {len(json_files)} JSON files to process.")

    # Use ThreadPoolExecutor for parallel downloading
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_json_file, json_file) for json_file in json_files]
        for future in as_completed(futures):
            try:
                future.result()  # Ensure exceptions are raised
            except Exception as e:
                print(f"Error in processing: {str(e)}")

if __name__ == "__main__":
    main()