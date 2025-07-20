# # # import os
# # # import json
# # # import logging
# # # try:
# # #     import ujson as json  # Dùng ujson để tăng tốc (nếu có)
# # # except ImportError:
# # #     import json  # Nếu không có ujson, dùng json mặc định

# # # # Thiết lập logging
# # # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# # # logger = logging.getLogger(__name__)

# # # def read_dataset(file_path="test.json"):
# # #     """Đọc dữ liệu từ file JSON"""
# # #     if not os.path.exists(file_path):
# # #         logger.warning(f"File {file_path} không tồn tại.")
# # #         return []
    
# # #     try:
# # #         with open(file_path, "r", encoding="utf-8") as f:
# # #             return json.load(f)
# # #     except json.JSONDecodeError as e:
# # #         logger.error(f"Lỗi trong file JSON {file_path}: {e}")
# # #         return []

# # # def read_checkpoint(checkpoint_file):
# # #     """Đọc file checkpoint"""
# # #     if not os.path.exists(checkpoint_file):
# # #         logger.info(f"Checkpoint {checkpoint_file} không tồn tại.")
# # #         return {}

# # #     try:
# # #         with open(checkpoint_file, "r", encoding="utf-8") as f:
# # #             checkpoint = json.load(f)
# # #             if not isinstance(checkpoint, dict):
# # #                 logger.warning(f"Checkpoint {checkpoint_file} không đúng định dạng, đặt lại rỗng.")
# # #                 return {}
# # #             return checkpoint
# # #     except json.JSONDecodeError as e:
# # #         logger.error(f"Lỗi khi đọc checkpoint: {e}")
# # #         return {}

# # # def create_directory_for_file(file_path):
# # #     """Tạo thư mục nếu chưa tồn tại"""
# # #     directory = os.path.dirname(file_path)
# # #     if directory and not os.path.exists(directory):
# # #         os.makedirs(directory)
# # #         logger.info(f"Đã tạo thư mục: {directory}")

# # # def merge_checkpoints_to_output(input_file="test.json", checkpoint_dir="checkpoints", output_file="processed_data.json"):
# # #     """
# # #     Đọc tất cả các file checkpoint, ánh xạ tên đã rút gọn vào dữ liệu gốc, và lưu vào file output.
# # #     """
# # #     # Đọc dữ liệu gốc từ test.json
# # #     data = read_dataset(input_file)
# # #     if not data:
# # #         logger.warning("Dữ liệu gốc rỗng, không có gì để xử lý.")
# # #         return

# # #     # Tạo từ điển ánh xạ từ tất cả các checkpoint
# # #     name_mapping = {}
# # #     checkpoint_files = [f for f in os.listdir(checkpoint_dir) if f.startswith("checkpoint_part_") and f.endswith(".json")]
# # #     logger.info(f"Tìm thấy {len(checkpoint_files)} file checkpoint để xử lý")

# # #     for checkpoint_file in checkpoint_files:
# # #         checkpoint_path = os.path.join(checkpoint_dir, checkpoint_file)
# # #         checkpoint = read_checkpoint(checkpoint_path)
# # #         name_mapping.update(checkpoint)
# # #         logger.info(f"Đã đọc {len(checkpoint)} ánh xạ từ {checkpoint_path}")

# # #     # Ánh xạ tên đã rút gọn vào dữ liệu gốc
# # #     processed_data = []
# # #     unmapped_items = []
# # #     for item in data:
# # #         original_name = item.get("name", "")
# # #         if not original_name:
# # #             logger.warning(f"Sample không có trường 'name', bỏ qua: {item}")
# # #             continue

# # #         if original_name in name_mapping:
# # #             item["name"] = name_mapping[original_name]
# # #             processed_data.append(item)
# # #             logger.info(f"Đã ánh xạ: {original_name} -> {item['name']}")
# # #         else:
# # #             unmapped_items.append(item)
# # #             logger.warning(f"Không tìm thấy ánh xạ cho: {original_name}")

# # #     # Lưu dữ liệu đã xử lý
# # #     create_directory_for_file(output_file)
# # #     with open(output_file, "w", encoding="utf-8") as f:
# # #         json.dump(processed_data, f, ensure_ascii=False, indent=2)
# # #     logger.info(f"Đã lưu dữ liệu đã xử lý vào {output_file}")

# # #     # Lưu các item không ánh xạ được (nếu có)
# # #     if unmapped_items:
# # #         unmapped_file = "unmapped_data.json"
# # #         create_directory_for_file(unmapped_file)
# # #         with open(unmapped_file, "w", encoding="utf-8") as f:
# # #             json.dump(unmapped_items, f, ensure_ascii=False, indent=2)
# # #         logger.info(f"Đã lưu {len(unmapped_items)} item không ánh xạ được vào {unmapped_file}")

# # # if __name__ == "__main__":
# # #     # Chạy hàm để gộp dữ liệu từ checkpoint vào file tổng hợp
# # #     merge_checkpoints_to_output(
# # #         input_file="test.json",
# # #         checkpoint_dir="checkpoints",
# # #         output_file="processed_data.json"
# # #     )




# # import json
# # import os
# # import logging

# # # Thiết lập logging
# # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# # logger = logging.getLogger(__name__)

# # # Đọc tất cả file checkpoint trong thư mục checkpoints1
# # def read_all_checkpoints(checkpoints_dir="checkpoints1"):
# #     checkpoint_data = {}
# #     if not os.path.exists(checkpoints_dir):
# #         logger.error(f"Thư mục {checkpoints_dir} không tồn tại.")
# #         return checkpoint_data

# #     for filename in os.listdir(checkpoints_dir):
# #         if filename.endswith(".json"):
# #             checkpoint_file = os.path.join(checkpoints_dir, filename)
# #             try:
# #                 with open(checkpoint_file, "r", encoding="utf-8") as f:
# #                     checkpoint = json.load(f)
# #                     if not isinstance(checkpoint, dict):
# #                         logger.warning(f"Checkpoint {checkpoint_file} không đúng định dạng, bỏ qua.")
# #                         continue
# #                     # Gộp dữ liệu từ checkpoint vào checkpoint_data
# #                     checkpoint_data.update(checkpoint)
# #                     logger.info(f"Đã đọc file checkpoint: {checkpoint_file}")
# #             except json.JSONDecodeError as e:
# #                 logger.error(f"Lỗi khi đọc checkpoint {checkpoint_file}: {e}")
# #             except Exception as e:
# #                 logger.error(f"Lỗi không xác định khi đọc checkpoint {checkpoint_file}: {e}")
    
# #     return checkpoint_data

# # # Đọc file train.json
# # def read_train_data(train_file="train.json"):
# #     try:
# #         with open(train_file, "r", encoding="utf-8") as f:
# #             data = json.load(f)
# #             if not isinstance(data, list):
# #                 logger.warning(f"File {train_file} không phải danh sách, đặt lại rỗng.")
# #                 return []
# #             return data
# #     except json.JSONDecodeError as e:
# #         logger.error(f"Lỗi khi đọc file JSON {train_file}: {e}")
# #         return []
# #     except FileNotFoundError as e:
# #         logger.error(f"File {train_file} không tồn tại: {e}")
# #         return []

# # # Lưu dữ liệu đã map vào file mới
# # def save_mapped_data(data, output_file="train_updated1.json"):
# #     try:
# #         with open(output_file, "w", encoding="utf-8") as f:
# #             json.dump(data, f, ensure_ascii=False, indent=4)
# #         logger.info(f"Đã lưu dữ liệu đã map vào {output_file}")
# #     except Exception as e:
# #         logger.error(f"Lỗi khi ghi file {output_file}: {e}")

# # # Map thông tin từ checkpoint vào train.json
# # def map_checkpoints_to_train():
# #     # Đọc tất cả checkpoint từ thư mục checkpoints1
# #     checkpoint_data = read_all_checkpoints("checkpoints1")
# #     if not checkpoint_data:
# #         logger.warning("Không có dữ liệu checkpoint để map.")
# #         return

# #     # Đọc dữ liệu từ train.json
# #     train_data = read_train_data("train.json")
# #     if not train_data:
# #         logger.warning("File train.json rỗng, không có dữ liệu để map.")
# #         return

# #     # Tạo danh sách mới để lưu dữ liệu đã map
# #     mapped_data = []
# #     matched_count = 0
# #     skipped_count = 0

# #     # Duyệt qua từng mục trong train_data
# #     for item in train_data:
# #         original_name = item.get("name")
# #         if not original_name:
# #             logger.warning(f"Mục không có trường 'name', bỏ qua: {item}")
# #             skipped_count += 1
# #             continue

# #         # Tìm kiếm original_name trong checkpoint_data
# #         if original_name in checkpoint_data:
# #             checkpoint_entry = checkpoint_data[original_name]
# #             # Tạo mục mới với các trường yêu cầu
# #             mapped_item = {
# #                 "id": item.get("id"),
# #                 "name": checkpoint_entry.get("shortened_name", original_name),  # Dùng shortened_name
# #                 "image_base64": item.get("image_base64"),
# #                 "label": checkpoint_entry.get("label")
# #             }
# #             mapped_data.append(mapped_item)
# #             matched_count += 1
# #             logger.info(f"Đã map: {original_name} -> shortened_name: {mapped_item['name']}, label: {mapped_item['label']}")
# #         else:
# #             logger.warning(f"Không tìm thấy '{original_name}' trong checkpoint, bỏ qua.")
# #             skipped_count += 1

# #     logger.info(f"Đã map thành công {matched_count} mục, bỏ qua {skipped_count} mục.")

# #     # Lưu dữ liệu đã map
# #     save_mapped_data(mapped_data, "train_updated1.json")

# # if __name__ == "__main__":
# #     map_checkpoints_to_train()


# import json
# import os
# import logging

# # Thiết lập logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# # Đọc tất cả file checkpoint trong thư mục checkpoints1 để lấy shortened_name
# def read_all_checkpoints(checkpoints_dir="checkpoints1"):
#     checkpoint_data = {}
#     if not os.path.exists(checkpoints_dir):
#         logger.error(f"Thư mục {checkpoints_dir} không tồn tại.")
#         return checkpoint_data

#     for filename in os.listdir(checkpoints_dir):
#         if filename.endswith(".json"):
#             checkpoint_file = os.path.join(checkpoints_dir, filename)
#             try:
#                 with open(checkpoint_file, "r", encoding="utf-8") as f:
#                     checkpoint = json.load(f)
#                     if not isinstance(checkpoint, dict):
#                         logger.warning(f"Checkpoint {checkpoint_file} không đúng định dạng, bỏ qua.")
#                         continue
#                     # Gộp dữ liệu từ checkpoint vào checkpoint_data
#                     checkpoint_data.update(checkpoint)
#                     logger.info(f"Đã đọc file checkpoint: {checkpoint_file}")
#             except json.JSONDecodeError as e:
#                 logger.error(f"Lỗi khi đọc checkpoint {checkpoint_file}: {e}")
#             except Exception as e:
#                 logger.error(f"Lỗi không xác định khi đọc checkpoint {checkpoint_file}: {e}")
    
#     return checkpoint_data

# # Đọc tất cả file JSON trong thư mục data để lấy label từ breadcrumbs
# def read_labels_from_data(data_dir="data"):
#     label_mapping = {}
#     if not os.path.exists(data_dir):
#         logger.error(f"Thư mục {data_dir} không tồn tại.")
#         return label_mapping

#     for filename in os.listdir(data_dir):
#         if filename.endswith(".json"):
#             data_file = os.path.join(data_dir, filename)
#             try:
#                 with open(data_file, "r", encoding="utf-8") as f:
#                     data = json.load(f)
#                     if not isinstance(data, list):
#                         logger.warning(f"File {data_file} không phải danh sách, bỏ qua.")
#                         continue

#                     # Duyệt qua từng mẫu trong file
#                     for item in data:
#                         name = item.get("name")
#                         if not name:
#                             logger.warning(f"Mẫu không có trường 'name' trong {data_file}, bỏ qua.")
#                             continue

#                         # Lấy label từ chỉ mục thứ 3 trong breadcrumbs
#                         breadcrumbs = item.get("breadcrumbs", [])
#                         if len(breadcrumbs) >= 3:
#                             label = breadcrumbs[2].get("name", "")
#                             if label:
#                                 label_mapping[name] = label
#                                 logger.info(f"Đã lấy label: {name} -> {label} từ file {data_file}")
#                             else:
#                                 logger.warning(f"Không tìm thấy 'name' trong breadcrumbs[2] cho {name} trong {data_file}")
#                         else:
#                             logger.warning(f"Breadcrumbs không đủ 3 phần tử cho {name} trong {data_file}")

#             except json.JSONDecodeError as e:
#                 logger.error(f"Lỗi khi đọc file JSON {data_file}: {e}")
#             except Exception as e:
#                 logger.error(f"Lỗi không xác định khi đọc file {data_file}: {e}")

#     return label_mapping

# # Đọc file train.json để lấy id và image_base64
# def read_train_data(train_file="train.json"):
#     try:
#         with open(train_file, "r", encoding="utf-8") as f:
#             data = json.load(f)
#             if not isinstance(data, list):
#                 logger.warning(f"File {train_file} không phải danh sách, đặt lại rỗng.")
#                 return []
#             return data
#     except json.JSONDecodeError as e:
#         logger.error(f"Lỗi khi đọc file JSON {train_file}: {e}")
#         return []
#     except FileNotFoundError as e:
#         logger.error(f"File {train_file} không tồn tại: {e}")
#         return []

# # Lưu dữ liệu đã map vào file mới
# def save_mapped_data(data, output_file="train_updated1.json"):
#     try:
#         with open(output_file, "w", encoding="utf-8") as f:
#             json.dump(data, f, ensure_ascii=False, indent=4)
#         logger.info(f"Đã lưu dữ liệu đã map vào {output_file}")
#     except Exception as e:
#         logger.error(f"Lỗi khi ghi file {output_file}: {e}")

# # Map dữ liệu từ train.json, checkpoints1 và data
# def map_data_to_train():
#     # Đọc shortened_name từ thư mục checkpoints1
#     checkpoint_data = read_all_checkpoints("checkpoints1")
#     if not checkpoint_data:
#         logger.warning("Không có dữ liệu checkpoint để map shortened_name.")
#         return

#     # Đọc label từ thư mục data
#     label_mapping = read_labels_from_data("data")
#     if not label_mapping:
#         logger.warning("Không có dữ liệu label để map.")
#         return

#     # Đọc id và image_base64 từ train.json
#     train_data = read_train_data("train.json")
#     if not train_data:
#         logger.warning("File train.json rỗng, không có dữ liệu để map.")
#         return

#     # Tạo danh sách mới để lưu dữ liệu đã map
#     mapped_data = []
#     matched_count = 0
#     skipped_count = 0

#     # Duyệt qua từng mục trong train_data
#     for item in train_data:
#         original_name = item.get("name")
#         if not original_name:
#             logger.warning(f"Mục không có trường 'name', bỏ qua: {item}")
#             skipped_count += 1
#             continue

#         # Tìm shortened_name trong checkpoint_data
#         shortened_name = checkpoint_data.get(original_name, {}).get("shortened_name")
#         if not shortened_name:
#             logger.warning(f"Không tìm thấy shortened_name cho '{original_name}' trong checkpoint_data, bỏ qua.")
#             skipped_count += 1
#             continue

#         # Tìm label trong label_mapping
#         label = label_mapping.get(original_name)
#         if not label:
#             logger.warning(f"Không tìm thấy label cho '{original_name}' trong label_mapping, bỏ qua.")
#             skipped_count += 1
#             continue

#         # Tạo mục mới với các trường yêu cầu
#         mapped_item = {
#             "id": item.get("id"),
#             "name": shortened_name,  # Sử dụng shortened_name từ checkpoint
#             "image_base64": item.get("image_base64"),
#             "label": label  # Sử dụng label từ data
#         }
#         mapped_data.append(mapped_item)
#         matched_count += 1
#         logger.info(f"Đã map: {original_name} -> shortened_name: {shortened_name}, label: {label}")

#     logger.info(f"Đã map thành công {matched_count} mục, bỏ qua {skipped_count} mục.")

#     # Lưu dữ liệu đã map
#     save_mapped_data(mapped_data, "train_updated1.json")

# if __name__ == "__main__":
#     map_data_to_train()

import json
import os
import logging
import csv

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Đọc tất cả file checkpoint trong thư mục checkpoints1 để lấy shortened_name
def read_all_checkpoints(checkpoints_dir="checkpoints_test"):
    checkpoint_data = {}
    if not os.path.exists(checkpoints_dir):
        logger.error(f"Thư mục {checkpoints_dir} không tồn tại.")
        return checkpoint_data

    for filename in os.listdir(checkpoints_dir):
        if filename.endswith(".json"):
            checkpoint_file = os.path.join(checkpoints_dir, filename)
            try:
                with open(checkpoint_file, "r", encoding="utf-8") as f:
                    checkpoint = json.load(f)
                    if not isinstance(checkpoint, dict):
                        logger.warning(f"Checkpoint {checkpoint_file} không đúng định dạng, bỏ qua.")
                        continue
                    # Gộp dữ liệu từ checkpoint vào checkpoint_data
                    checkpoint_data.update(checkpoint)
                    logger.info(f"Đã đọc file checkpoint: {checkpoint_file}")
            except json.JSONDecodeError as e:
                logger.error(f"Lỗi khi đọc checkpoint {checkpoint_file}: {e}")
            except Exception as e:
                logger.error(f"Lỗi không xác định khi đọc checkpoint {checkpoint_file}: {e}")
    
    return checkpoint_data

# Đọc tất cả file JSON trong thư mục data để lấy label từ breadcrumbs và lưu tên file
def read_labels_from_data(data_dir="data"):
    label_mapping = {}
    if not os.path.exists(data_dir):
        logger.error(f"Thư mục {data_dir} không tồn tại.")
        return label_mapping

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
                        if not name:
                            logger.warning(f"Mẫu không có trường 'name' trong {data_file}, bỏ qua.")
                            continue

                        # Lấy label từ breadcrumbs
                        breadcrumbs = item.get("breadcrumbs", [])
                        if len(breadcrumbs) >= 3:
                            # Kiểm tra url của breadcrumbs[2] có bắt đầu bằng https không
                            if len(breadcrumbs) >= 3 and breadcrumbs[2].get("url", "").startswith("https"):
                                # Nếu có, lùi lại một chỉ mục (lấy breadcrumbs[1])
                                label = breadcrumbs[1].get("name", "")
                                logger.info(f"Url của breadcrumbs[2] bắt đầu bằng https, lùi lại lấy label từ breadcrumbs[1]: {label}")
                            else:
                                # Nếu không, giữ nguyên lấy breadcrumbs[2]
                                label = breadcrumbs[2].get("name", "")
                                logger.info(f"Url của breadcrumbs[2] không bắt đầu bằng https, lấy label từ breadcrumbs[2]: {label}")

                            if label:
                                # Lưu label và tên file chứa label
                                label_mapping[name] = {
                                    "label": label,
                                    "source_file": filename
                                }
                                logger.info(f"Đã lấy label: {name} -> {label} từ file {data_file}")
                            else:
                                logger.warning(f"Không tìm thấy 'name' trong breadcrumbs cho {name} trong {data_file}")
                        else:
                            logger.warning(f"Breadcrumbs không đủ 3 phần tử cho {name} trong {data_file}")

            except json.JSONDecodeError as e:
                logger.error(f"Lỗi khi đọc file JSON {data_file}: {e}")
            except Exception as e:
                logger.error(f"Lỗi không xác định khi đọc file {data_file}: {e}")

    return label_mapping

# Đọc file train.json để lấy id và image_base64
def read_train_data(train_file="test.json"):
    try:
        with open(train_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                logger.warning(f"File {train_file} không phải danh sách, đặt lại rỗng.")
                return []
            return data
    except json.JSONDecodeError as e:
        logger.error(f"Lỗi khi đọc file JSON {train_file}: {e}")
        return []
    except FileNotFoundError as e:
        logger.error(f"File {train_file} không tồn tại: {e}")
        return []

# Lưu dữ liệu đã map vào file JSON
def save_mapped_data(data, output_file="test_updated.json"):
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Đã lưu dữ liệu đã map vào {output_file}")
    except Exception as e:
        logger.error(f"Lỗi khi ghi file {output_file}: {e}")

# Lưu dữ liệu đã map vào file CSV với các cột id, label, name
def save_mapped_data_to_csv(data, output_file="test_updated.csv"):
    try:
        with open(output_file, "w", encoding="utf-8", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["id", "label", "name"])
            writer.writeheader()
            for item in data:
                csv_row = {
                    "id": item.get("id", ""),
                    "label": item.get("label", ""),
                    "name": item.get("name", "")
                }
                writer.writerow(csv_row)
        logger.info(f"Đã lưu dữ liệu đã map vào file CSV: {output_file}")
    except Exception as e:
        logger.error(f"Lỗi khi ghi file CSV {output_file}: {e}")

# Map dữ liệu từ train.json, checkpoints1 và data
def map_data_to_train():
    # Đọc shortened_name từ thư mục checkpoints1
    checkpoint_data = read_all_checkpoints("checkpoints_test")
    if not checkpoint_data:
        logger.warning("Không có dữ liệu checkpoint để map shortened_name.")
        return

    # Đọc label và tên file từ thư mục data
    label_mapping = read_labels_from_data("data")
    if not label_mapping:
        logger.warning("Không có dữ liệu label để map.")
        return

    # Đọc id và image_base64 từ train.json
    train_data = read_train_data("test.json")
    if not train_data:
        logger.warning("File train.json rỗng, không có dữ liệu để map.")
        return

    # Tạo danh sách mới để lưu dữ liệu đã map
    mapped_data = []
    matched_count = 0
    skipped_count = 0

    # Duyệt qua từng mục trong train_data
    for item in train_data:
        original_name = item.get("name")
        if not original_name:
            logger.warning(f"Mục không có trường 'name', bỏ qua: {item}")
            skipped_count += 1
            continue

        # Tìm shortened_name trong checkpoint_data
        shortened_name = checkpoint_data.get(original_name, {}).get("shortened_name")
        if not shortened_name:
            logger.warning(f"Không tìm thấy shortened_name cho '{original_name}' trong checkpoint_data, bỏ qua.")
            skipped_count += 1
            continue

        # Tìm label và tên file trong label_mapping
        label_info = label_mapping.get(original_name)
        if not label_info:
            logger.warning(f"Không tìm thấy label cho '{original_name}' trong label_mapping, bỏ qua.")
            skipped_count += 1
            continue

        label = label_info["label"]
        label_source_file = label_info["source_file"]

        # Tạo mục mới với các trường yêu cầu
        mapped_item = {
            "id": item.get("id"),
            "name": shortened_name,  # Sử dụng shortened_name từ checkpoint
            "image_base64": item.get("image_base64"),
            "label": label,  # Sử dụng label từ data
            "label_source_file": label_source_file  # Thêm tên file chứa label
        }
        mapped_data.append(mapped_item)
        matched_count += 1
        logger.info(f"Đã map: {original_name} -> shortened_name: {shortened_name}, label: {label}, label_source_file: {label_source_file}")

    logger.info(f"Đã map thành công {matched_count} mục, bỏ qua {skipped_count} mục.")

    # Lưu dữ liệu đã map vào file JSON
    save_mapped_data(mapped_data, "test_updated.json")

    # Lưu dữ liệu đã map vào file CSV
    save_mapped_data_to_csv(mapped_data, "test_updated.csv")

if __name__ == "__main__":
    map_data_to_train()