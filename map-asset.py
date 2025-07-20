# #không chia data ra làm 9
# import json
# import os
# import requests

# def download_image(url, folder_path, file_name):
#     """Download an image from a URL and save it to the specified path."""
#     try:
#         response = requests.get(url, stream=True)
#         if response.status_code == 200:
#             with open(os.path.join(folder_path, file_name), 'wb') as f:
#                 for chunk in response.iter_content(1024):
#                     f.write(chunk)
#             return True
#         return False
#     except Exception as e:
#         print(f"Error downloading {url}: {e}")
#         return False

# def process_json_data(json_file_path, output_dir="products_phone"):
#     """Process JSON data, map required fields, download images, and save new JSON."""
#     # Load JSON data from the specified file
#     try:
#         with open(json_file_path, 'r', encoding='utf-8') as f:
#             json_data = json.load(f)
#     except FileNotFoundError:
#         print(f"Error: File {json_file_path} not found")
#         return []
#     except json.JSONDecodeError:
#         print(f"Error: Invalid JSON format in {json_file_path}")
#         return []

#     # Create output directory if it doesn't exist
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)

#     # Initialize result list for new JSON
#     result = []
    
#     # Process each sample using the original ID
#     for sample in json_data:
#         product_id = str(sample.get("id", ""))  # Use original ID as string
#         if not product_id:
#             print(f"Skipping sample with missing ID: {sample.get('name', 'Unknown')}")
#             continue
#         product_name = sample.get("name", "")
#         images = sample.get("images", [])
#         product_url = sample.get("short_url")
        
#         # Create folder for this product using original ID
#         product_folder = os.path.join(output_dir, product_id)
#         if not os.path.exists(product_folder):
#             os.makedirs(product_folder)
        
#         # Download images and generate file names
#         image_files = []
#         for img_index, image in enumerate(images, start=1):
#             base_url = image.get("base_url", "")
#             if base_url:
#                 file_name = f"{product_id}_{img_index:02d}.png"
#                 if download_image(base_url, product_folder, file_name):
#                     image_files.append(file_name)
        
#         # Add mapped data to result
#         result.append({
#             "id": product_id,
#             "name": product_name,
#             "short_url": product_url,
#             "annotations": "",
#             "image_files": image_files,
#         })
    
#     # Save new JSON file
#     output_json_path = os.path.join(output_dir, "mapped_products_phone.json")
#     with open(output_json_path, 'w', encoding='utf-8') as f:
#         json.dump(result, f, ensure_ascii=False, indent=4)
    
#     return result

# # Example usage
# if __name__ == "__main__":
#     # Specify the path to your JSON file
#     json_file_path = "/home/phuocdai/crawl-data-ecom/data/product_phone.json"  # Replace with your actual JSON file path
#     process_json_data(json_file_path)




#có chia data 
import json
import os
import requests
import shutil
from pathlib import Path
from math import ceil

def download_image(url, folder_path, file_name):
    """Download an image from a URL and save it to the specified path."""
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(os.path.join(folder_path, file_name), 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            return True
        return False
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

def read_json_file(json_file_path):
    """Read a single JSON file."""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            return [data]
    except FileNotFoundError:
        print(f"Error: File {json_file_path} not found")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {json_file_path}")
        return []

def split_list(lst, n):
    """Split a list into n approximately equal parts."""
    if not lst:
        return []
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]

def process_json_file(json_file_path, output_base_dir, num_splits=9):
    """Process a single JSON file, map fields, download images, and split into parts."""
    # Get the base filename without extension
    json_file_name = Path(json_file_path).stem  # e.g., "product_laptop"
    
    # Read JSON data
    json_data = read_json_file(json_file_path)
    if not json_data:
        print(f"No valid data in {json_file_path}")
        return []

    # Initialize result list for new JSON
    result = []
    
    # Process each sample
    for sample in json_data:
        product_id = str(sample.get("id", ""))
        if not product_id:
            print(f"Skipping sample with missing ID: {sample.get('name', 'Unknown')}")
            continue
        product_name = sample.get("name", "")
        images = sample.get("images", [])
        product_url = sample.get("short_url", "")
        
        # Create temporary folder for this product
        temp_product_folder = Path(output_base_dir) / "temp" / json_file_name / product_id
        temp_product_folder.mkdir(parents=True, exist_ok=True)
        
        # Download images and generate file names
        image_files = []
        for img_index, image in enumerate(images, start=1):
            base_url = image.get("base_url", "")
            if base_url:
                file_name = f"{product_id}_{img_index:02d}.png"
                if download_image(base_url, temp_product_folder, file_name):
                    image_files.append(file_name)
        
        # Add mapped data to result
        result.append({
            "id": product_id,
            "name": product_name,
            "short_url": product_url,
            "annotations": "",
            "image_files": image_files,
        })

    # Split results into approximately num_splits parts
    split_results = split_list(result, num_splits)
    
    # Process each split
    for split_index, split_data in enumerate(split_results, 1):
        # Create output directory for this split and file
        split_output_dir = Path(output_base_dir) / f"part{split_index}" / json_file_name
        split_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save JSON for this split
        output_json_path = split_output_dir / f"{json_file_name}_part{split_index}.json"
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(split_data, f, ensure_ascii=False, indent=4)
        
        # Move images to split directory
        for item in split_data:
            product_id = item["id"]
            temp_folder = Path(item["temp_folder"])
            product_output_folder = split_output_dir / product_id
            product_output_folder.mkdir(exist_ok=True)
            
            # Move all images from temp folder to split folder
            for image_file in item["image_files"]:
                src_path = temp_folder / image_file
                dst_path = product_output_folder / image_file
                if src_path.exists():
                    shutil.move(src_path, dst_path)
            
            # Update image_files to use relative paths
            item["image_files"] = [f"{product_id}/{img}" for img in item["image_files"]]
            del item["temp_folder"]  # Remove temp folder reference
        
        # Update JSON with final image paths
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(split_data, f, ensure_ascii=False, indent=4)
        
        # Clean up temp folder if empty
        temp_folder = Path(output_base_dir) / "temp" / json_file_name / product_id
        if temp_folder.exists() and not any(temp_folder.iterdir()):
            temp_folder.rmdir()
    
    # Clean up base temp folder for this file
    base_temp_folder = Path(output_base_dir) / "temp" / json_file_name
    if base_temp_folder.exists() and not any(base_temp_folder.iterdir()):
        base_temp_folder.rmdir()
    
    return split_results

def process_json_data(input_dir, output_dir, num_splits=9):
    """Process all JSON files in the input directory."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    # Create base output directory
    output_path.mkdir(exist_ok=True)
    
    # Process each JSON file
    for json_file in input_path.glob("*.json"):
        print(f"Processing {json_file}")
        process_json_file(json_file, output_dir, num_splits)
    
    # Clean up root temp folder
    root_temp_folder = output_path / "temp"
    if root_temp_folder.exists() and not any(root_temp_folder.iterdir()):
        root_temp_folder.rmdir()

if __name__ == "__main__":
    # Define input and output directories
    input_dir = "/home/phuocdai/crawl-data-ecom/data"  # Replace with your input directory path
    output_dir = "/home/phuocdai/Data/shared/data-ecom"  # Replace with your output directory path
    process_json_data(input_dir, output_dir)