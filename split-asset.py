import json
import os
import shutil
import math

def split_dataset(input_dir="products_laptop", output_base_dir="dataset-asset-laptop"):
    """Split the dataset into approximately four equal parts."""
    # Read the mapped_products.json file
    json_file_path = os.path.join(input_dir, "mapped_products.json")
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {json_file_path} not found")
        return
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {json_file_path}")
        return

    # Calculate the size of each split
    total_items = len(data)
    split_size = math.ceil(total_items / 4)  # Ceiling to ensure all items are included

    # Create output base directory
    if not os.path.exists(output_base_dir):
        os.makedirs(output_base_dir)

    # Split data into four parts
    for split_index in range(4):
        start_idx = split_index * split_size
        end_idx = min((split_index + 1) * split_size, total_items)
        split_data = data[start_idx:end_idx]

        # Create directory for this split
        split_dir = os.path.join(output_base_dir, f"split_{split_index + 1}")
        if not os.path.exists(split_dir):
            os.makedirs(split_dir)

        # Copy images and prepare JSON for this split
        split_json = []
        for item in split_data:
            product_id = item["id"]
            product_name = item["name"]
            image_files = item["image_files"]

            # Create product folder in split directory
            product_folder = os.path.join(split_dir, product_id)
            if not os.path.exists(product_folder):
                os.makedirs(product_folder)

            # Copy image files
            source_folder = os.path.join(input_dir, product_id)
            for image_file in image_files:
                source_path = os.path.join(source_folder, image_file)
                dest_path = os.path.join(product_folder, image_file)
                if os.path.exists(source_path):
                    shutil.copy2(source_path, dest_path)
                else:
                    print(f"Warning: Image {source_path} not found, skipping")

            # Add to split JSON
            split_json.append({
                "id": product_id,
                "name": product_name,
                "image_files": image_files
            })

        # Save JSON for this split
        split_json_path = os.path.join(split_dir, f"mapped_products_split_{split_index + 1}.json")
        with open(split_json_path, 'w', encoding='utf-8') as f:
            json.dump(split_json, f, ensure_ascii=False, indent=4)

        print(f"Created split {split_index + 1} with {len(split_data)} items in {split_dir}")

# Example usage
if __name__ == "__main__":
    # Specify the input directory (where products and mapped_products.json are located)
    input_dir = "products_laptop"
    # Specify the output base directory for splits
    output_base_dir = "dataset-asset-laptop"
    split_dataset(input_dir, output_base_dir)