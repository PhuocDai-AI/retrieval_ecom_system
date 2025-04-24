import json

# Hàm đọc dữ liệu từ một file JSON
def load_data_from_json(file_path: str) -> list:
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

# Hàm gộp dữ liệu từ 4 file JSON và lưu vào file mới
def merge_json_files(file_prefix: str = "train_part_", num_files: int = 4, output_file: str = "train_update_label.json") -> None:
    merged_data = []

    # Đọc dữ liệu từ từng file
    for i in range(1, num_files + 1):
        file_path = f"{file_prefix}{i}.json"
        data = load_data_from_json(file_path)
        if data:
            merged_data.extend(data)
            print(f"Loaded {len(data)} samples from {file_path}")
        else:
            print(f"No data loaded from {file_path}")

    # Ghi dữ liệu gộp vào file mới
    if merged_data:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=4)
        print(f"Merged data saved to {output_file} with {len(merged_data)} samples.")
    else:
        print("No data to merge.")

# Hàm chính
def main():
    merge_json_files()

if __name__ == "__main__":
    main()