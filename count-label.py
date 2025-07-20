# # # import json
# # # import os
# # # import logging
# # # from collections import Counter

# # # # Thiết lập logging
# # # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# # # logger = logging.getLogger(__name__)

# # # # Đọc tất cả file checkpoint trong thư mục checkpoints1 và thu thập label
# # # def collect_labels_from_checkpoints(checkpoints_dir="checkpoints1"):
# # #     all_labels = []  # Danh sách chứa tất cả label (bao gồm trùng)
# # #     unique_labels = set()  # Tập hợp chứa các label duy nhất

# # #     if not os.path.exists(checkpoints_dir):
# # #         logger.error(f"Thư mục {checkpoints_dir} không tồn tại.")
# # #         return all_labels, unique_labels

# # #     for filename in os.listdir(checkpoints_dir):
# # #         if filename.endswith(".json"):
# # #             checkpoint_file = os.path.join(checkpoints_dir, filename)
# # #             try:
# # #                 with open(checkpoint_file, "r", encoding="utf-8") as f:
# # #                     checkpoint = json.load(f)
# # #                     if not isinstance(checkpoint, dict):
# # #                         logger.warning(f"Checkpoint {checkpoint_file} không đúng định dạng, bỏ qua.")
# # #                         continue

# # #                     # Duyệt qua từng mục trong checkpoint
# # #                     for _, entry in checkpoint.items():
# # #                         label = entry.get("label")
# # #                         if label:
# # #                             all_labels.append(label)  # Thêm vào danh sách tất cả label
# # #                             unique_labels.add(label)  # Thêm vào tập hợp label duy nhất
# # #                         else:
# # #                             logger.warning(f"Mục trong {checkpoint_file} không có trường 'label': {entry}")

# # #                 logger.info(f"Đã đọc file checkpoint: {checkpoint_file}")

# # #             except json.JSONDecodeError as e:
# # #                 logger.error(f"Lỗi khi đọc checkpoint {checkpoint_file}: {e}")
# # #             except Exception as e:
# # #                 logger.error(f"Lỗi không xác định khi đọc checkpoint {checkpoint_file}: {e}")

# # #     return all_labels, unique_labels

# # # # Lưu danh sách label duy nhất vào file JSON
# # # def save_unique_labels(unique_labels, output_file="unique_labels.json"):
# # #     try:
# # #         unique_labels_list = list(unique_labels)  # Chuyển set thành list để lưu vào JSON
# # #         with open(output_file, "w", encoding="utf-8") as f:
# # #             json.dump(unique_labels_list, f, ensure_ascii=False, indent=4)
# # #         logger.info(f"Đã lưu danh sách label duy nhất vào {output_file}")
# # #     except Exception as e:
# # #         logger.error(f"Lỗi khi ghi file {output_file}: {e}")

# # # # Đếm và ghi log số lượng label
# # # def count_and_log_labels(all_labels, unique_labels):
# # #     total_labels = len(all_labels)  # Tổng số label (bao gồm trùng)
# # #     unique_count = len(unique_labels)  # Số label duy nhất

# # #     # Đếm số lần xuất hiện của từng label
# # #     label_counts = Counter(all_labels)

# # #     logger.info(f"Tổng số label (bao gồm trùng): {total_labels}")
# # #     logger.info(f"Số loại label duy nhất: {unique_count}")
# # #     logger.info("Thống kê chi tiết:")
# # #     for label, count in label_counts.items():
# # #         logger.info(f"Label: {label}, Số lần xuất hiện: {count}")

# # # def main():
# # #     # Thu thập label từ tất cả file checkpoint
# # #     all_labels, unique_labels = collect_labels_from_checkpoints("checkpoints1")

# # #     if not all_labels:
# # #         logger.warning("Không tìm thấy label nào trong thư mục checkpoints1.")
# # #         return

# # #     # Đếm và ghi log số lượng label
# # #     count_and_log_labels(all_labels, unique_labels)

# # #     # Lưu danh sách label duy nhất
# # #     save_unique_labels(unique_labels, "unique_labels.json")

# # # if __name__ == "__main__":
# # #     main()


# # import json
# # from collections import Counter

# # # Đường dẫn file JSON
# # file_path = '/home/phuocdai/retrieval_ecom_system/archive/train_data.json'

# # # Đọc file JSON Lines
# # data = []
# # try:
# #     with open(file_path, 'r') as file:
# #         for line in file:
# #             # Bỏ qua dòng trống
# #             if line.strip():
# #                 # Chuyển từng dòng thành đối tượng JSON
# #                 data.append(json.loads(line))

# #     # Lấy tất cả các giá trị class_label
# #     class_labels = [item['class_label'] for item in data]

# #     # Đếm số lượng mỗi loại class_label
# #     class_label_counts = Counter(class_labels)

# #     # In kết quả
# #     for label, count in class_label_counts.items():
# #         print(f"Class label: {label}, Count: {count}")

# # except FileNotFoundError:
# #     print(f"Không tìm thấy file tại đường dẫn: {file_path}. Vui lòng kiểm tra lại đường dẫn.")
# # except json.JSONDecodeError as e:
# #     print(f"File JSON không hợp lệ tại một dòng. Lỗi: {e}. Vui lòng kiểm tra định dạng của file train_data.json.")
# # except KeyError:
# #     print("Không tìm thấy key 'class_label' trong dữ liệu JSON. Vui lòng kiểm tra cấu trúc dữ liệu.")


import json
import logging
from collections import defaultdict

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Đọc dữ liệu từ file train_updated.json
def read_train_updated_data(file_path="test_updated.json"):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, list):
                logger.warning(f"File {file_path} không phải danh sách, đặt lại rỗng.")
                return []
            return data
    except json.JSONDecodeError as e:
        logger.error(f"Lỗi khi đọc file JSON {file_path}: {e}")
        return []
    except FileNotFoundError as e:
        logger.error(f"File {file_path} không tồn tại: {e}")
        return []

# Thống kê số lượng mẫu cho mỗi label
def count_labels_in_train_updated():
    # Đọc dữ liệu từ train_updated.json
    train_updated_data = read_train_updated_data("test_updated.json")
    if not train_updated_data:
        logger.warning("File train_updated.json rỗng, không có dữ liệu để thống kê.")
        return

    label_counts = defaultdict(int)

    # Duyệt qua từng mẫu để đếm label
    for item in train_updated_data:
        label = item.get("label", "Không có label")
        label_counts[label] += 1

    # Ghi log và in thống kê
    total_samples = len(train_updated_data)
    total_unique_labels = len(label_counts)
    logger.info(f"Tổng số mẫu: {total_samples}")
    logger.info(f"Tổng số label duy nhất: {total_unique_labels}")
    logger.info("\nThống kê số lượng mẫu cho mỗi label:")
    for label, count in sorted(label_counts.items(), key=lambda x: x[1], reverse=True):
        logger.info(f"Label: {label} - Số lượng mẫu: {count} - Tỷ lệ: {(count/total_samples)*100:.2f}%")

    # Lưu kết quả thống kê vào file
    stats_file = "label_test.json"
    stats_data = {label: count for label, count in sorted(label_counts.items(), key=lambda x: x[1], reverse=True)}
    try:
        with open(stats_file, "w", encoding="utf-8") as f:
            json.dump(stats_data, f, ensure_ascii=False, indent=4)
        logger.info(f"Đã lưu thống kê vào {stats_file}")
    except Exception as e:
        logger.error(f"Lỗi khi ghi file {stats_file}: {e}")

if __name__ == "__main__":
    count_labels_in_train_updated()
# import json
# import logging
# from collections import defaultdict

# # Thiết lập logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# # Đọc dữ liệu từ file train_updated.json
# def read_train_updated_data(file_path="train_updated.json"):
#     try:
#         with open(file_path, "r", encoding="utf-8") as f:
#             data = json.load(f)
#             if not isinstance(data, list):
#                 logger.warning(f"File {file_path} không phải danh sách, đặt lại rỗng.")
#                 return []
#             return data
#     except json.JSONDecodeError as e:
#         logger.error(f"Lỗi khi đọc file JSON {file_path}: {e}")
#         return []
#     except FileNotFoundError as e:
#         logger.error(f"File {file_path} không tồn tại: {e}")
#         return []

# # Thống kê số lượng mẫu cho mỗi label, lưu label_source_file có count < 10 và đếm số lượng mẫu
# def count_labels_in_train_updated():
#     # Đọc dữ liệu từ train_updated1.json
#     train_updated_data = read_train_updated_data("train_updated.json")
#     if not train_updated_data:
#         logger.warning("File train_updated.json rỗng, không có dữ liệu để thống kê.")
#         return

#     # Sử dụng defaultdict để nhóm các label_source_file theo label
#     label_to_files = defaultdict(list)
#     label_counts = defaultdict(int)

#     # Duyệt qua từng mẫu để đếm label và thu thập label_source_file
#     for item in train_updated_data:
#         label = item.get("label", "Không có label")
#         source_file = item.get("label_source_file", "Không có source file")
        
#         label_counts[label] += 1
#         if source_file not in label_to_files[label]:
#             label_to_files[label].append(source_file)

#     # Ghi log và in thống kê
#     total_samples = len(train_updated_data)
#     total_unique_labels = len(label_counts)
    
#     logger.info(f"Tổng số mẫu: {total_samples}")
#     logger.info(f"Tổng số label duy nhất: {total_unique_labels}")
#     logger.info("\nThống kê số lượng mẫu cho mỗi label:")
    
#     for label, count in sorted(label_counts.items(), key=lambda x: x[1], reverse=True):
#         source_files = label_to_files[label]
#         logger.info(f"Label: {label} - Số lượng mẫu: {count} - Tỷ lệ: {(count/total_samples)*100:.2f}% - Source files: {source_files}")

#     # Lưu kết quả thống kê vào file
#     stats_file = "label1-1.json"
#     stats_data = {
#         "total_samples": total_samples,
#         "total_unique_labels": total_unique_labels,
#         "label_counts": [
#             {
#                 "label": label,
#                 "count": count,
#                 "label_source_files": label_to_files[label]
#             }
#             for label, count in sorted(label_counts.items(), key=lambda x: x[1], reverse=True)
#         ]
#     }
#     try:
#         with open(stats_file, "w", encoding="utf-8") as f:
#             json.dump(stats_data, f, ensure_ascii=False, indent=4)
#         logger.info(f"Đã lưu thống kê vào {stats_file}")
#     except Exception as e:
#         logger.error(f"Lỗi khi ghi file {stats_file}: {e}")

#     # Lấy các label_source_file của các label có count < 10
#     under_10_label_source_files = set()
#     for label, count in label_counts.items():
#         if count < 10:
#             under_10_label_source_files.update(label_to_files[label])

#     # Chuyển thành danh sách và sắp xếp
#     under_10_label_source_files = sorted(list(under_10_label_source_files))

#     # Đếm số lượng mẫu có count < 10 trong mỗi label_source_file
#     file_to_sample_count = defaultdict(int)
#     for item in train_updated_data:
#         label = item.get("label", "Không có label")
#         source_file = item.get("label_source_file", "Không có source file")
#         if label_counts[label] < 5 and source_file in under_10_label_source_files:
#             file_to_sample_count[source_file] += 1

#     # Lưu danh sách label_source_files có count < 10 và số lượng mẫu tương ứng
#     under_10_file = "label_source_files_under_10.json"
#     under_10_data = {
#         "total_files": len(under_10_label_source_files),
#         "label_source_files": [
#             {
#                 "file": source_file,
#                 "sample_count": file_to_sample_count[source_file]
#             }
#             for source_file in under_10_label_source_files
#         ]
#     }
#     try:
#         with open(under_10_file, "w", encoding="utf-8") as f:
#             json.dump(under_10_data, f, ensure_ascii=False, indent=4)
#         logger.info(f"Đã lưu danh sách label_source_files có count < 10 vào {under_10_file}")
#     except Exception as e:
#         logger.error(f"Lỗi khi ghi file {under_10_file}: {e}")

# if __name__ == "__main__":
#     count_labels_in_train_updated()
