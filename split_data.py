import json

# Hàm đọc dữ liệu từ file JSON
def load_data_from_json(file_path="train.json") -> list:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {file_path}.")
        return []

# Hàm chia dữ liệu thành 4 phần và lưu vào các file riêng
def split_json_to_files(data: list, num_files: int = 4) -> None:
    total_samples = len(data)
    samples_per_file = total_samples // num_files  # Số mẫu mỗi file
    remainder = total_samples % num_files  # Số mẫu còn dư

    start_idx = 0
    for i in range(num_files):
        # Xác định số mẫu cho file này
        if i < remainder:
            num_samples = samples_per_file + 1  # Thêm 1 mẫu nếu có dư
        else:
            num_samples = samples_per_file

        # Lấy phần dữ liệu cho file này
        end_idx = start_idx + num_samples
        chunk = data[start_idx:end_idx]

        # Ghi vào file mới
        output_file = f"train_part_{i + 1}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False, indent=4)
        print(f"Created {output_file} with {len(chunk)} samples.")

        start_idx = end_idx

# Hàm chính
def main():
    # Đọc dữ liệu từ file
    data = load_data_from_json()
    if not data:
        print("No data to process.")
        return

    print(f"Total samples: {len(data)}")
    # Chia và lưu vào 4 file
    split_json_to_files(data)

if __name__ == "__main__":
    main()