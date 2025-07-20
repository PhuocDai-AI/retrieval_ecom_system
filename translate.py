from google.cloud import translate_v2 as translate
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # Thư viện hiển thị thanh tiến độ

# API Key của bạn
API_KEY = "607b54293bb47c614af30e11b718feb6644bdc93"  # Thay bằng API Key của bạn

# Khởi tạo client với API Key
client = translate.Client(api_key=API_KEY)

# Đường dẫn file dữ liệu
INPUT_FILE = "/home/phuocdai/retrieval_ecom_system/archive/train_data.json"  # File chứa 90k mẫu
OUTPUT_FILE = "translated_data.json"

# Hàm dịch nhiều văn bản cùng lúc
def batch_translate_texts(texts, target_language="vi"):
    try:
        results = client.translate(texts, target_language=target_language)
        return [result["translatedText"] for result in results]
    except Exception as e:
        print(f"Lỗi khi dịch batch: {e}")
        return texts  # Trả về nguyên bản nếu lỗi

# Hàm xử lý một batch mẫu
def process_batch(batch):
    # Tách các trường cần dịch
    brands = [item["brand"] for item in batch]
    product_titles = [item["product_title"] for item in batch]
    
    # Dịch hàng loạt
    translated_brands = batch_translate_texts(brands)
    translated_product_titles = batch_translate_texts(product_titles)
    
    # Tạo kết quả
    translated_batch = []
    for i, item in enumerate(batch):
        translated_item = item.copy()
        translated_item["brand"] = translated_brands[i]
        translated_item["product_title"] = translated_product_titles[i]
        # Ánh xạ thủ công cho class_label
        translated_item["class_label"] = "áo saree" if item["class_label"] == "saree" else item["class_label"]
        translated_batch.append(translated_item)
    return translated_batch

# Hàm chính để dịch toàn bộ dữ liệu
def translate_large_dataset(input_file, output_file, batch_size=100, max_workers=10):
    # Đọc dữ liệu
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    total_samples = len(data)
    print(f"Tổng số mẫu: {total_samples}")
    
    # Chia dữ liệu thành các batch
    batches = [data[i:i + batch_size] for i in range(0, total_samples, batch_size)]
    
    # Xử lý song song
    translated_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_batch, batch) for batch in batches]
        for future in tqdm(as_completed(futures), total=len(batches), desc="Đang dịch"):
            translated_data.extend(future.result())
    
    # Lưu kết quả
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(translated_data, f, ensure_ascii=False, indent=2)
    
    print(f"Dịch hoàn tất. Kết quả được lưu vào {output_file}")

# Chạy chương trình
if __name__ == "__main__":
    # Cài đặt tqdm nếu chưa có
    try:
        import tqdm
    except ImportError:
        os.system("pip install tqdm")
    
    # Dịch dữ liệu
    translate_large_dataset(INPUT_FILE, OUTPUT_FILE, batch_size=100, max_workers=150)