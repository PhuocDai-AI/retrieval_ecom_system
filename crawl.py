import requests
import json
import re
import os
import time

# Danh sách các danh mục cần crawl, với ID tương ứng
categories = {
   "sport": 1975,
   "man": 915,
   "laptop": 1846,
   "shoe_man": 1686,
   "refrigeration&tv": 4221,
   "shoe_woman": 1703,
   "camera": 1801,
   "fashion_accessories": 27498,
   "watches&jewelry": 8371,
   "backpacks&suitcases": 6000,
   "fashion_bag_woman": 976,
   "fashion_bag_man": 27616,
   "house": 15078

}

# Header giả lập trình duyệt để tránh bị chặn
headers = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"
}

# URL để lấy chi tiết sản phẩm theo ID
product_url = "https://tiki.vn/api/v2/products/{}"

# Danh sách các trường sẽ được chuyển thành chuỗi JSON
flatten_field = [
    "badges", "inventory", "categories", "rating_summary", 
    "brand", "seller_specifications", "current_seller", "other_sellers", 
    "configurable_options", "configurable_products", "specifications", "product_links",
    "services_and_promotions", "promotions", "stock_item", "installment_info"
]

# Hàm crawl danh sách ID sản phẩm theo category
def crawl_product_id_by_category(category_id):
    product_list = []
    i = 1
    while True:
        print(f"Đang crawl trang {i} của danh mục {category_id}")
        url = f"https://tiki.vn/api/v2/products?limit=10000&include=advertisement&aggregations=1&category={category_id}&page={i}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print("Không thể truy cập API hoặc đã hết trang.")
            break

        products = json.loads(response.text).get("data", [])
        if not products:
            break

        for product in products:
            product_id = str(product["id"])
            product_list.append(product_id)
        i += 1
        time.sleep(0.3)  # Nghỉ giữa các lần gọi để tránh bị chặn IP

    return product_list

# Hàm crawl chi tiết sản phẩm từ danh sách ID
def crawl_product(product_list=[]):
    product_detail_list = []
    for product_id in product_list:
        response = requests.get(product_url.format(product_id), headers=headers)
        if response.status_code == 200:
            product_detail_list.append(response.text)
            print("Đã lấy sản phẩm:", product_id)
        else:
            print("Không lấy được sản phẩm:", product_id)
        time.sleep(0.3)  # Nghỉ giữa các lần gọi để tránh bị chặn IP
    return product_detail_list

def clean_html(raw_text):
    return re.sub(r"<!DOCTYPE html>.*?</html>", "", raw_text, flags=re.DOTALL)

def adjust_product(product):
    if not product.strip():
        return None
    try:
        e = json.loads(product)
    except json.JSONDecodeError:
        print("Bỏ qua JSON lỗi")
        return None
    if not e.get("id"):
        return None
    for field in flatten_field:
        if field in e:
            e[field] = json.dumps(e[field], ensure_ascii=False).replace('\n', '')
    return e

# Hàm lưu dữ liệu ra file
def save_file(data, file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        if isinstance(data, list) and all(isinstance(d, dict) for d in data):
            json.dump(data, f, ensure_ascii=False, indent=4)
        elif isinstance(data, list):
            f.write("\n".join(data))
        else:
            f.write(data)
    print("Đã lưu:", file_path)

# ========== CHƯƠNG TRÌNH CHÍNH ========== #
os.makedirs("./data", exist_ok=True)

for category_name, category_id in categories.items():
    print(f"\n============================")
    print(f"Bắt đầu crawl danh mục: {category_name}")
    print("============================")

    # Đặt tên file cho từng phần
    product_id_file = f"./data/product-id_{category_name}.txt"
    product_raw_file = f"./data/product_{category_name}.txt"
    product_json_file = f"./data/product_{category_name}.json"

    # Bước 1: Lấy danh sách ID sản phẩm
    product_ids = crawl_product_id_by_category(category_id)
    save_file(product_ids, product_id_file)

    # Bước 2: Lấy thông tin chi tiết sản phẩm
    product_raw_list = crawl_product(product_ids)
    save_file(product_raw_list, product_raw_file)

    # Bước 3: Làm sạch dữ liệu và chuyển thành JSON chuẩn
    product_cleaned_list = []
    for raw_product in product_raw_list:
        cleaned_text = clean_html(raw_product)
        adjusted = adjust_product(cleaned_text)
        if adjusted:
            product_cleaned_list.append(adjusted)

    save_file(product_cleaned_list, product_json_file)
    print(f"Đã hoàn tất danh mục: {category_name} ({len(product_cleaned_list)} sản phẩm)\n")
