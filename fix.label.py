import json
import os
import logging
from collections import defaultdict

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Đọc tất cả file JSON trong thư mục checkpoints1 và lưu thông tin file gốc
def read_all_checkpoints(checkpoints_dir="checkpoints1"):
    all_samples = []
    if not os.path.exists(checkpoints_dir):
        logger.error(f"Thư mục {checkpoints_dir} không tồn tại.")
        return all_samples

    for filename in os.listdir(checkpoints_dir):
        if filename.endswith(".json"):
            checkpoint_file = os.path.join(checkpoints_dir, filename)
            try:
                with open(checkpoint_file, "r", encoding="utf-8") as f:
                    checkpoint = json.load(f)
                    if not isinstance(checkpoint, dict):
                        logger.warning(f"Checkpoint {checkpoint_file} không đúng định dạng (không phải dict), bỏ qua.")
                        continue
                    # Chuyển đổi dict thành list các mẫu, lưu thông tin file gốc
                    for original_name, details in checkpoint.items():
                        if not isinstance(details, dict):
                            logger.warning(f"Dữ liệu không đúng định dạng cho {original_name} trong {checkpoint_file}, bỏ qua.")
                            continue
                        details["original_name"] = original_name
                        details["source_file"] = checkpoint_file  # Lưu thông tin file gốc
                        all_samples.append(details)
                    logger.info(f"Đã đọc file checkpoint: {checkpoint_file}")
            except json.JSONDecodeError as e:
                logger.error(f"Lỗi khi đọc checkpoint {checkpoint_file}: {e}")
            except Exception as e:
                logger.error(f"Lỗi không xác định khi đọc checkpoint {checkpoint_file}: {e}")

    return all_samples

# Xử lý các mẫu trùng lặp và giữ mẫu phù hợp
def deduplicate_samples(samples):
    # Nhóm các mẫu theo original_name
    grouped_samples = defaultdict(list)
    for sample in samples:
        original_name = sample.get("original_name")
        if not original_name:
            logger.warning(f"Mẫu không có original_name, bỏ qua: {sample}")
            continue
        grouped_samples[original_name].append(sample)

    # Xử lý trùng lặp
    deduplicated_samples = {}
    total_duplicates = 0

    for original_name, duplicates in grouped_samples.items():
        if len(duplicates) == 1:
            # Không có trùng lặp, giữ mẫu duy nhất
            deduplicated_samples[original_name] = duplicates[0]
        else:
            total_duplicates += len(duplicates) - 1
            logger.info(f"Phát hiện {len(duplicates)} mẫu trùng lặp cho original_name: {original_name}")

            # Ưu tiên mẫu có label khác original_name
            selected_sample = None
            for sample in duplicates:
                label = sample.get("label", "")
                if label != original_name:
                    selected_sample = sample
                    logger.info(f"Chọn mẫu có label khác original_name: {label} cho {original_name}")
                    break

            # Nếu không có mẫu nào có label khác original_name, chọn mẫu đầu tiên
            if not selected_sample:
                selected_sample = duplicates[0]
                logger.info(f"Không có mẫu nào có label khác original_name, chọn mẫu đầu tiên cho {original_name}")

            deduplicated_samples[original_name] = selected_sample

    logger.info(f"Tổng số mẫu trùng lặp đã loại bỏ: {total_duplicates}")
    return deduplicated_samples

# Ghi đè dữ liệu đã xử lý vào các file gốc
def overwrite_original_files(deduplicated_samples, checkpoints_dir="checkpoints1"):
    # Nhóm các mẫu theo file gốc
    samples_by_file = defaultdict(dict)
    for original_name, sample in deduplicated_samples.items():
        source_file = sample.get("source_file")
        if not source_file:
            logger.warning(f"Mẫu không có thông tin source_file, bỏ qua: {original_name}")
            continue
        # Loại bỏ trường source_file và original_name trước khi ghi vào file
        sample_data = {k: v for k, v in sample.items() if k not in ["source_file", "original_name"]}
        samples_by_file[source_file][original_name] = sample_data

    # Ghi đè vào các file gốc
    for source_file, updated_data in samples_by_file.items():
        try:
            with open(source_file, "w", encoding="utf-8") as f:
                json.dump(updated_data, f, ensure_ascii=False, indent=4)
            logger.info(f"Đã ghi đè file: {source_file} với {len(updated_data)} mẫu")
        except Exception as e:
            logger.error(f"Lỗi khi ghi đè file {source_file}: {e}")

# Hàm chính để xử lý trùng lặp và ghi đè
def deduplicate_and_overwrite_checkpoints():
    # Đọc tất cả mẫu từ checkpoints1
    samples = read_all_checkpoints("checkpoints1")
    if not samples:
        logger.warning("Không có dữ liệu để xử lý.")
        return

    logger.info(f"Tổng số mẫu ban đầu: {len(samples)}")

    # Xử lý trùng lặp
    deduplicated_data = deduplicate_samples(samples)
    
    logger.info(f"Số mẫu sau khi loại bỏ trùng lặp: {len(deduplicated_data)}")

    # Ghi đè dữ liệu đã xử lý vào các file gốc
    overwrite_original_files(deduplicated_data, "checkpoints1")

if __name__ == "__main__":
    deduplicate_and_overwrite_checkpoints()