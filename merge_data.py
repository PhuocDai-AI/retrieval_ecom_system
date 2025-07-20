# import json
# import os
# import glob
# import logging
# from collections import defaultdict
# from bs4 import BeautifulSoup

# # Thiết lập logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def clean_html(raw_html):
#     """Làm sạch HTML và trả về văn bản thuần túy."""
#     soup = BeautifulSoup(raw_html, "html.parser")
#     return soup.get_text(separator="\n").strip()

# def load_json_files(data_dir, batch_size=1000):
#     """Đọc file JSON từ thư mục data và tổ chức dữ liệu theo id."""
#     json_files = glob.glob(os.path.join(data_dir, "*.json"))
#     data_dict = defaultdict(dict)
#     file_count = 0
    
#     for file_path in json_files:
#         try:
#             with open(file_path, 'r', encoding='utf-8') as f:
#                 data = json.load(f)
#                 items = data if isinstance(data, list) else [data]
                
#                 for item in items:
#                     if 'id' in item:
#                         if 'description' in item and isinstance(item['description'], str):
#                             item['description'] = clean_html(item['description'])
#                         data_dict[str(item['id'])].update(item)
                
#                 file_count += 1
#                 if file_count % batch_size == 0:
#                     logging.info(f"Processed {file_count} files")
#         except Exception as e:
#             logging.error(f"Error reading {file_path}: {e}")
    
#     logging.info(f"Total files processed: {file_count}")
#     return data_dict

# def merge_data(vector_file, data_dir, output_file, batch_size=1000):
#     """Map dữ liệu từ vector_file và data_dir, tính discount_percentage, lưu vào output_file."""
#     # Đọc file vector-payload.json
#     try:
#         with open(vector_file, 'r', encoding='utf-8') as f:
#             vector_data = json.load(f)
#         logging.info(f"Loaded {vector_file} with {len(vector_data)} items")
#     except Exception as e:
#         logging.error(f"Error reading {vector_file}: {e}")
#         return

#     # Tải dữ liệu từ thư mục data
#     data_dict = load_json_files(data_dir, batch_size)
    
#     # Các trường yêu cầu trong output
#     required_fields = ['id', 'name', 'price', 'original_price', 'description', 'short_url', 'discount_percentage', 'image_base64', 'vector']
#     updated_data = []
    
#     for item in vector_data:
#         if 'id' not in item:
#             logging.warning("Skipping item without id")
#             continue
        
#         item_id = str(item['id'])
#         # Khởi tạo new_item với giá trị mặc định
#         new_item = {field: '' for field in required_fields}
#         new_item['id'] = item_id
        
#         # Map các trường từ vector_data
#         new_item.update({
#             'name': item.get('name', ''),
#             'image_base64': item.get('image_base64', ''),
#             'vector': item.get('vector', '')
#         })
        
#         # Map các trường từ data_dir nếu id tồn tại
#         if item_id in data_dict:
#             source_item = data_dict[item_id]
#             new_item.update({
#                 'price': source_item.get('price', ''),
#                 'original_price': source_item.get('original_price', ''),
#                 'description': source_item.get('description', ''),
#                 'short_url': source_item.get('short_url', '')
#             })
        
#         # Tính toán discount_percentage
#         price = new_item['price']
#         original_price = new_item['original_price']
#         if (isinstance(price, (int, float)) and isinstance(original_price, (int, float)) 
#             and original_price > 0 and price <= original_price):
#             new_item['discount_percentage'] = round(((original_price - price) / original_price) * 100, 2)
#         else:
#             new_item['discount_percentage'] = None
        
#         updated_data.append(new_item)
    
#     # Lưu kết quả vào file output
#     try:
#         with open(output_file, 'w', encoding='utf-8', newline='\n') as f:
#             json.dump(updated_data, f, ensure_ascii=False, indent=4)
#         logging.info(f"Successfully saved updated data to {output_file}")
#     except Exception as e:
#         logging.error(f"Error writing to {output_file}: {e}")

# if __name__ == "__main__":
#     vector_file = "vector-payload.json"
#     data_dir = "data"
#     output_file = "test_updated.json"
#     batch_size = 1000
    
#     merge_data(vector_file, data_dir, output_file, batch_size)


import os
import json
import random
import csv
import logging
from collections import defaultdict

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_labels_from_data(data_dir="data"):
    """
    Đọc tất cả file JSON trong thư mục data, lấy id và label từ breadcrumbs.
    Trả về danh sách các mẫu với id và label.
    """
    samples = []
    if not os.path.exists(data_dir):
        logger.error(f"Thư mục {data_dir} không tồn tại.")
        return samples

    for filename in os.listdir(data_dir):
        if filename.endswith(".json"):
            data_file = os.path.join(data_dir, filename)
            try:
                with open(data_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if not isinstance(data, list):
                        logger.warning(f"File {data_file} không phải danh sách, bỏ qua.")
                        continue

                    # Duyệt qua từng mẫu trong file
                    for item in data:
                        name = item.get("name")
                        id_ = item.get("id")
                        if not name or not id_:
                            logger.warning(f"Mẫu không có trường 'id' hoặc 'name' trong {data_file}, bỏ qua.")
                            continue

                        # Lấy label từ breadcrumbs
                        breadcrumbs = item.get("breadcrumbs", [])
                        label = ""
                        if len(breadcrumbs) >= 3:
                            # Kiểm tra url của breadcrumbs[2] có bắt đầu bằng https không
                            if breadcrumbs[2].get("url", "").startswith("https"):
                                # Nếu có, lùi lại một chỉ mục (lấy breadcrumbs[1])
                                label = breadcrumbs[1].get("name", "")
                                logger.info(f"Url của breadcrumbs[2] bắt đầu bằng https, lùi lại lấy label từ breadcrumbs[1]: {label}")
                            else:
                                # Nếu không, giữ nguyên lấy breadcrumbs[2]
                                label = breadcrumbs[2].get("name", "")
                                logger.info(f"Url của breadcrumbs[2] không bắt đầu bằng https, lấy label từ breadcrumbs[2]: {label}")
                        else:
                            logger.warning(f"Breadcrumbs không đủ 3 phần tử cho {name} trong {data_file}")
                            continue

                        if label:
                            samples.append({"id": id_, "label": label})
                            logger.info(f"Đã lấy label: {id_} -> {label} từ file {data_file}")
                        else:
                            logger.warning(f"Không tìm thấy 'name' trong breadcrumbs cho {name} trong {data_file}")

            except json.JSONDecodeError as e:
                logger.error(f"Lỗi khi đọc file JSON {data_file}: {e}")
            except Exception as e:
                logger.error(f"Lỗi không xác định khi đọc file {data_file}: {e}")

    return samples

def process_and_split_data(data_dir="data", train_file="train.csv", test_file="test.csv", label_train_file="label_train.json", label_test_file="label_test.json"):
    """
    Xử lý dữ liệu từ thư mục data:
    - Lấy id và label.
    - Đếm số sample của từng label.
    - Loại bỏ label có tổng số sample < 170.
    - Chia dữ liệu thành train (70%) và test (30%) cho các label còn lại.
    - Lưu vào train.csv, test.csv, label_train.json, label_test.json.
    """
    # Bước 1: Đọc dữ liệu và lấy id, label
    samples = read_labels_from_data(data_dir)
    if not samples:
        logger.error("Không có dữ liệu để xử lý.")
        return

    # Bước 2: Đếm số sample của từng label
    label_counts = defaultdict(int)
    label_samples = defaultdict(list)
    
    for sample in samples:
        label = sample["label"]
        label_counts[label] += 1
        label_samples[label].append(sample)

    # Bước 3: Loại bỏ label có tổng số sample < 170
    filtered_label_samples = {}
    for label, count in label_counts.items():
        if count >= 170:
            filtered_label_samples[label] = label_samples[label]
            logger.info(f"Label {label} được giữ lại với {count} mẫu.")
        else:
            logger.info(f"Label {label} bị loại bỏ vì chỉ có {count} mẫu (< 170).")

    if not filtered_label_samples:
        logger.error("Không có label nào đủ 170 mẫu để xử lý.")
        return

    # Bước 4: Chia dữ liệu thành train (70%) và test (30%) cho từng label
    train_data = []
    test_data = []
    label_train_counts = defaultdict(int)
    label_test_counts = defaultdict(int)

    for label, samples in filtered_label_samples.items():
        # Xáo trộn dữ liệu
        random.shuffle(samples)
        
        # Tính số lượng mẫu cho tập train
        total_samples = len(samples)
        train_size = int(total_samples * 0.7)

        # Chia dữ liệu
        train_samples = samples[:train_size]
        test_samples = samples[train_size:]

        # Thêm vào tập train và test
        train_data.extend(train_samples)
        test_data.extend(test_samples)

        # Cập nhật số lượng sample cho từng label
        label_train_counts[label] = len(train_samples)
        label_test_counts[label] = len(test_samples)

        logger.info(f"Label {label}: Train = {len(train_samples)} mẫu, Test = {len(test_samples)} mẫu")

    # Bước 5: Lưu dữ liệu vào các file đầu ra
    # train.csv và test.csv
    for data, output_file in [(train_data, train_file), (test_data, test_file)]:
        with open(output_file, 'w', encoding='utf-8', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['id', 'label'])  # Header
            for sample in data:
                writer.writerow([sample['id'], sample['label']])

    # label_train.json và label_test.json
    with open(label_train_file, 'w', encoding='utf-8') as f:
        json.dump(dict(label_train_counts), f, ensure_ascii=False, indent=4)

    with open(label_test_file, 'w', encoding='utf-8') as f:
        json.dump(dict(label_test_counts), f, ensure_ascii=False, indent=4)

    logger.info(f"Hoàn tất! Đã xử lý {len(samples)} mẫu.")
    logger.info(f"Tập train lưu tại {train_file} với {len(train_data)} mẫu.")
    logger.info(f"Tập test lưu tại {test_file} với {len(test_data)} mẫu.")
    logger.info(f"Thống kê label train lưu tại {label_train_file}")
    logger.info(f"Thống kê label test lưu tại {label_test_file}")

if __name__ == "__main__":
    # Thư mục chứa các file JSON đầu vào
    input_folder = "data"

    # Các file kết quả
    train_file = "train.csv"
    test_file = "test.csv"
    label_train_file = "label_train.json"
    label_test_file = "label_test.json"

    # Bắt đầu xử lý
    process_and_split_data(input_folder, train_file, test_file, label_train_file, label_test_file)