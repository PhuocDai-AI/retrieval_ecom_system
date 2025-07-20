import os
import json
import base64
import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor
from functools import partial

def process_json_files(folder_path, train_file, test_file, failed_file, max_workers=100):
    """
    Xử lý tất cả các file JSON trong thư mục, trích xuất thumbnail_url, chuyển sang base64,
    chia thành tập train (70%) và test (30%), và lưu kết quả vào các file JSON riêng.
    Sử dụng ThreadPoolExecutor để tải ảnh song song.
    """
    # Tạo hoặc làm trống các file output
    for output_file in [train_file, test_file]:
        with open(output_file, 'w', encoding='utf-8') as out_file:
            out_file.write('[\n')  # Bắt đầu mảng JSON

    # Danh sách lưu các ID bị lỗi
    failed_ids = []

    # Lấy tất cả các file JSON trong thư mục
    json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    print(f"Tìm thấy {len(json_files)} file JSON để xử lý")

    # Biến đếm tổng số file và item
    total_files = len(json_files)
    processed_files = 0
    processed_items = 0
    train_first_item = True
    test_first_item = True

    for file_name in json_files:
        file_path = os.path.join(folder_path, file_name)

        try:
            # Đọc dữ liệu JSON
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)

            # Nếu là 1 object thì chuyển thành list để xử lý đồng nhất
            items = data if isinstance(data, list) else [data]

            # Tính toán số lượng mẫu cho tập train
            total_samples = len(items)
            train_size = int(total_samples * 0.7)

            # Xáo trộn dữ liệu
            random.shuffle(items)

            # Chia tập train và test
            train_items = items[:train_size]
            test_items = items[train_size:]

            # Xử lý tập train bằng tải ảnh song song
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                train_results = list(executor.map(process_item, train_items))

            for result, item in zip(train_results, train_items):
                write_to_output(result, train_file, train_first_item, "train")

                if train_first_item and result:
                    train_first_item = False

                if not result and item.get('id'):
                    failed_ids.append(item.get('id'))

                processed_items += 1
                if processed_items % 10 == 0:
                    print(f"Đã xử lý {processed_items} mẫu")

            # Xử lý tập test bằng tải ảnh song song
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                test_results = list(executor.map(process_item, test_items))

            for result, item in zip(test_results, test_items):
                write_to_output(result, test_file, test_first_item, "test")

                if test_first_item and result:
                    test_first_item = False

                if not result and item.get('id'):
                    failed_ids.append(item.get('id'))

                processed_items += 1
                if processed_items % 10 == 0:
                    print(f"Đã xử lý {processed_items} mẫu")

        except Exception as e:
            print(f"Lỗi khi xử lý file {file_name}: {str(e)}")

        # Cập nhật tiến trình xử lý
        processed_files += 1
        print(f"Đã xử lý {processed_files}/{total_files} file - {processed_items} mẫu")

    # Đóng mảng JSON trong 2 file output
    for output_file in [train_file, test_file]:
        with open(output_file, 'a', encoding='utf-8') as out_file:
            out_file.write('\n]')

    # Lưu các ID bị lỗi vào file riêng
    with open(failed_file, 'w', encoding='utf-8') as fail_file:
        json.dump({"failed_ids": failed_ids}, fail_file, ensure_ascii=False, indent=2)

    print(f"Hoàn tất! Đã xử lý {processed_items} mẫu")
    print(f"Tập train lưu tại {train_file}")
    print(f"Tập test lưu tại {test_file}")
    print(f"Không xử lý được {len(failed_ids)} mẫu, lưu tại {failed_file}")

def process_item(item):
    """Xử lý 1 item JSON: trích xuất ID và thumbnail_url"""
    try:
        # Lấy ID sản phẩm
        product_id = item.get('id')
        if not product_id:
            return None

        # Lấy URL thumbnail
        thumbnail_url = item.get('thumbnail_url')
        if not thumbnail_url:
            return None

        # Lấy tên sản phẩm
        name = item.get('name')
        if not name:
            return None

        # Tải ảnh và chuyển thành base64, có retry nếu lỗi
        image_base64 = download_with_retry(thumbnail_url)
        if not image_base64:
            print(f"Tải ảnh thất bại cho sản phẩm {product_id} sau khi retry")
            return None

        result = {
            'id': product_id,
            'name': name,
            'image_base64': image_base64,
        }

        return result

    except Exception as e:
        print(f"Lỗi khi xử lý sản phẩm có ID {item.get('id', 'unknown')}: {str(e)}")
        return None

def download_with_retry(url, max_retries=3, retry_delay=2):
    """Tải ảnh từ URL với cơ chế retry nếu lỗi"""
    retries = 0

    while retries < max_retries:
        try:
            response = requests.get(url, timeout=10)  # timeout ngắn để retry nhanh
            response.raise_for_status()

            # Chuyển ảnh sang dạng base64
            image_data = base64.b64encode(response.content).decode('utf-8')
            return image_data

        except Exception as e:
            retries += 1
            if retries < max_retries:
                print(f"Lỗi khi tải ảnh từ {url}: {str(e)}. Thử lại sau {retry_delay} giây... (Lần {retries}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print(f"Không thể tải ảnh từ {url} sau {max_retries} lần thử: {str(e)}")
                return None

def write_to_output(result, output_file, is_first_item, split_type):
    """Ghi 1 item kết quả vào file output"""
    if not result:
        return

    with open(output_file, 'a', encoding='utf-8') as out_file:
        if not is_first_item:
            out_file.write(',\n')
        json_str = json.dumps(result, ensure_ascii=False, indent=4)
        out_file.write(json_str)

if __name__ == "__main__":
    # Thư mục chứa các file JSON đầu vào
    input_folder = "data"

    # Các file kết quả
    train_file = "train.json"
    test_file = "test.json"

    # File chứa ID bị lỗi
    failed_file = "fail_map.json"

    # Bắt đầu xử lý với số luồng tải song song
    process_json_files(input_folder, train_file, test_file, failed_file, max_workers=100)
