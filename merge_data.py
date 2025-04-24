import json
import os
import glob
import logging
from collections import defaultdict
from bs4 import BeautifulSoup

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_html(raw_html):
    """Làm sạch HTML và trả về văn bản thuần túy."""
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator="\n").strip()

def load_json_files(data_dir, batch_size=1000):
    """Đọc file JSON từ thư mục data và tổ chức dữ liệu theo id."""
    json_files = glob.glob(os.path.join(data_dir, "*.json"))
    data_dict = defaultdict(dict)
    file_count = 0
    
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                items = data if isinstance(data, list) else [data]
                
                for item in items:
                    if 'id' in item:
                        if 'description' in item and isinstance(item['description'], str):
                            item['description'] = clean_html(item['description'])
                        data_dict[str(item['id'])].update(item)
                
                file_count += 1
                if file_count % batch_size == 0:
                    logging.info(f"Processed {file_count} files")
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")
    
    logging.info(f"Total files processed: {file_count}")
    return data_dict

def merge_data(vector_file, data_dir, output_file, batch_size=1000):
    """Map dữ liệu từ vector_file và data_dir, tính discount_percentage, lưu vào output_file."""
    # Đọc file vector-payload.json
    try:
        with open(vector_file, 'r', encoding='utf-8') as f:
            vector_data = json.load(f)
        logging.info(f"Loaded {vector_file} with {len(vector_data)} items")
    except Exception as e:
        logging.error(f"Error reading {vector_file}: {e}")
        return

    # Tải dữ liệu từ thư mục data
    data_dict = load_json_files(data_dir, batch_size)
    
    # Các trường yêu cầu trong output
    required_fields = ['id', 'name', 'price', 'original_price', 'description', 'short_url', 'discount_percentage', 'image_base64', 'vector']
    updated_data = []
    
    for item in vector_data:
        if 'id' not in item:
            logging.warning("Skipping item without id")
            continue
        
        item_id = str(item['id'])
        # Khởi tạo new_item với giá trị mặc định
        new_item = {field: '' for field in required_fields}
        new_item['id'] = item_id
        
        # Map các trường từ vector_data
        new_item.update({
            'name': item.get('name', ''),
            'image_base64': item.get('image_base64', ''),
            'vector': item.get('vector', '')
        })
        
        # Map các trường từ data_dir nếu id tồn tại
        if item_id in data_dict:
            source_item = data_dict[item_id]
            new_item.update({
                'price': source_item.get('price', ''),
                'original_price': source_item.get('original_price', ''),
                'description': source_item.get('description', ''),
                'short_url': source_item.get('short_url', '')
            })
        
        # Tính toán discount_percentage
        price = new_item['price']
        original_price = new_item['original_price']
        if (isinstance(price, (int, float)) and isinstance(original_price, (int, float)) 
            and original_price > 0 and price <= original_price):
            new_item['discount_percentage'] = round(((original_price - price) / original_price) * 100, 2)
        else:
            new_item['discount_percentage'] = None
        
        updated_data.append(new_item)
    
    # Lưu kết quả vào file output
    try:
        with open(output_file, 'w', encoding='utf-8', newline='\n') as f:
            json.dump(updated_data, f, ensure_ascii=False, indent=4)
        logging.info(f"Successfully saved updated data to {output_file}")
    except Exception as e:
        logging.error(f"Error writing to {output_file}: {e}")

if __name__ == "__main__":
    vector_file = "vector-payload.json"
    data_dir = "data"
    output_file = "test_updated.json"
    batch_size = 1000
    
    merge_data(vector_file, data_dir, output_file, batch_size)