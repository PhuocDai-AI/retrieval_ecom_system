import json
import os
import time
from together import Together
from concurrent.futures import ThreadPoolExecutor
import math
import logging

# Exception riêng cho API limit
class APILimitException(Exception):
    pass

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 1. Cấu hình API Together AI
def configure_api(api_key):
    print(f"Using API key: [{api_key}]")  # Debug
    return Together(api_key=api_key)

# 2. Tạo thư mục nếu chưa tồn tại
def create_directory_for_file(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Đã tạo thư mục: {directory}")

# 3. Đọc API keys từ file APIKEY.txt
def read_api_keys(file_path="APIKEY.txt"):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            api_keys = [
                line.strip().replace('"', '').replace(',', '')
                for line in f if line.strip()
            ]
        return api_keys
    except Exception as e:
        logger.error(f"Lỗi khi đọc API keys từ {file_path}: {e}")
        raise

# 4. Đọc file test.json
def read_dataset(file_path="test.json"):
    if not os.path.exists(file_path):
        logger.warning(f"File {file_path} không tồn tại. Tạo file rỗng.")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=4)
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Lỗi trong file JSON {file_path}: {e}")
        return []

# 5. Đọc checkpoint để biết sample nào đã xử lý
def read_checkpoint(checkpoint_file):
    if not os.path.exists(checkpoint_file):
        logger.info(f"Checkpoint {checkpoint_file} không tồn tại. Tạo mới.")
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump({}, f, ensure_ascii=False, indent=4)
        return {}
    try:
        with open(checkpoint_file, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)
            if not isinstance(checkpoint, dict):
                logger.warning(f"Checkpoint {checkpoint_file} không đúng định dạng, đặt lại rỗng.")
                return {}
            return checkpoint
    except json.JSONDecodeError as e:
        logger.error(f"Lỗi khi đọc checkpoint: {e}")
        return {}

# 6. Lưu checkpoint sau mỗi sample được xử lý
def save_checkpoint(checkpoint, checkpoint_file):
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=4)

# 7. Ghi kết quả vào file output
def append_to_output_file(file_path, new_data):
    try:
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    existing_data = json.load(f)
                    if not isinstance(existing_data, list):
                        existing_data = []
                except json.JSONDecodeError:
                    existing_data = []
        else:
            existing_data = []
        existing_data.extend(new_data)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.error(f"Lỗi khi ghi vào file JSON: {e}")

# 8. Chia dataset thành các phần
def split_dataset(data, num_parts):
    total_samples = len(data)
    samples_per_part = math.ceil(total_samples / num_parts)
    split_data = [data[i:i + samples_per_part] for i in range(0, total_samples, samples_per_part)]
    return split_data

# 9. Gửi prompt đến model Together AI với cơ chế retry
def query_model_with_retry(client, prompt, model="meta-llama/Llama-3.3-70B-Instruct-Turbo-Free", max_retries=5, delay=10):
    messages = [{"role": "user", "content": prompt}]
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                logger.info(f"Thử lại sau {delay} giây...")
                time.sleep(delay)
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                max_tokens=500
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            # Nếu lỗi quota/rate limit thì raise APILimitException
            if "quota" in str(e).lower() or "rate limit" in str(e).lower() or "429" in str(e) or "403" in str(e):
                logger.error(f"API key bị limit/quota: {e}")
                raise APILimitException(str(e))
            logger.error(f"Lỗi khi gửi yêu cầu (lần {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return None

# 10. Rút gọn tên sản phẩm
def shorten_name(name, client):
    try:
        prompt = (
            "Rút gọn tên sản phẩm sau, chỉ giữ lại tên chính và đặc trưng nổi bật (nếu có), bỏ các thông tin không cần thiết như dung tích (ml, L, kg), thương hiệu, mô tả quảng cáo (như 'không độc hại', 'mạnh mẽ', 'hàng chính hãng'), hoặc thông tin bổ sung không liên quan (như 'dùng với máy phun sương', 'mùi nam'). **Chỉ trả về tên đã rút gọn, không thêm bất kỳ giải thích hay dòng thừa nào.**\n\n"
            "Ví dụ:\n"
            "- Input: 'Xịt đuổi chuột thảo mộc cao cấp Thái Lan 325 ml không độc hại - Hàng Chính Hãng (đặc biệt)' → Output: Xịt đuổi chuột thảo mộc Thái Lan\n"
            "- Input: 'Tinh dầu nước hoa dạng xịt 5ml & 10ml Allure Sport Cha..nel (mùi nam) mạnh mẽ, thể thao' → Output: Tinh dầu nước hoa dạng xịt\n"
            "- Input: '[TẶNG Chai Xịt 500ml] Dung Dịch Kháng Khuẩn, Khử mùi Nano Xpure Nano Silver 35ppm 4L (Không Mùi) Dùng với Máy Phun Sương – Nano Bạc AHT Corp (AHTC)' → Output: Dung Dịch Kháng Khuẩn, Khử mùi\n"
            "- Input: 'Nước rửa chén hưu cơ Fuwa3e organic ENZYME sinh học 100ml an toàn cho bé và da tay' → Output: Nước rửa chén hưu cơ\n"
            "- Input: 'Combo Bộ rửa bát AILO 1,8kg + Muối Điệp Hồng +Bóng somal 750ml' → Output: Bộ rửa bát\n"
            "- Input: 'Muối rửa bát FINISH 4,0 kg' → Output: Muối rửa bát\n"
            "- Input: 'Tấm thơm khử mùi đa năng' → Output: Tấm thơm khử mùi\n"
            "- Input: 'Bột Thông Tắc Cống' → Output: Bột Thông Tắc Cống\n"
            "- Input: 'Balo DA đa năng cao cấp phong cách mới – BEE GEE BLND9006' → Output: Balo DA đa năng\n\n"
            f"Input: '{name}' → Output: "
        )
        result = query_model_with_retry(client, prompt)
        if result:
            if result.startswith("Output: "):
                result = result[len("Output: "):].strip()
            return result
        return name  # Giữ nguyên nếu không xử lý được
    except APILimitException:
        raise
    except Exception as e:
        logger.error(f"Error processing '{name}': {e}")
        return name

# 11. Hàm xử lý một phần dữ liệu với một API key
def process_part(part_data, part_index, api_key, checkpoint_file, all_checkpoints):
    try:
        client = configure_api(api_key)
        checkpoint = read_checkpoint(checkpoint_file)
        processed_data = []
        failed_items = []
        for item in part_data:
            original_name = item.get("name", "")
            if not original_name:
                logger.warning(f"Sample không có trường 'name', bỏ qua: {item}")
                continue
            if original_name in all_checkpoints:
                logger.info(f"Bỏ qua (đã xử lý toàn cục): {original_name}")
                item["name"] = all_checkpoints[original_name]["shortened_name"]
                processed_data.append(item)
                continue
            logger.info(f"Part {part_index} - Đang xử lý: {original_name}")
            try:
                shortened_name = shorten_name(original_name, client)
            except APILimitException:
                logger.error(f"API key {api_key[:10]}... bị limit/quota khi xử lý part {part_index}!")
                # Lưu lại phần chưa xử lý
                output_path = os.path.join("processed_parts", f"part_{part_index}_failed.json")
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(part_data, f, ensure_ascii=False, indent=2)
                logger.info(f"Đã lưu phần {part_index} (chưa xử lý) vào {output_path}")
                return False  # Key bị limit, trả về False
            if shortened_name != original_name:
                item["name"] = shortened_name
                checkpoint[original_name] = {
                    "original_name": original_name,
                    "shortened_name": shortened_name
                }
                save_checkpoint(checkpoint, checkpoint_file)
                logger.info(f"Part {part_index} - Original: {original_name} -> Shortened: {shortened_name}")
            else:
                logger.warning(f"Part {part_index} - Không thể rút gọn: {original_name}")
                failed_items.append(item)
            processed_data.append(item)
        output_dir = "processed_parts"
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"part_{part_index}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Đã lưu phần {part_index} vào {output_path}")
        if failed_items:
            failed_path = os.path.join("failed_parts", f"failed_part_{part_index}.json")
            os.makedirs("failed_parts", exist_ok=True)
            with open(failed_path, 'w', encoding='utf-8') as f:
                json.dump(failed_items, f, ensure_ascii=False, indent=2)
            logger.info(f"Đã lưu {len(failed_items)} sample lỗi vào {failed_path}")
        return True  # Key còn sống, trả về True
    except APILimitException:
        logger.error(f"API key {api_key[:10]}... bị limit/quota khi xử lý part {part_index}!")
        return False
    except Exception as e:
        logger.error(f"Error processing part {part_index} with API key {api_key[:10]}...: {e}")
        output_path = os.path.join("processed_parts", f"part_{part_index}_failed.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(part_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Đã lưu phần {part_index} (chưa xử lý) vào {output_path}")
        return False

# 12. Hàm chính
def main():
    api_keys = read_api_keys("APIKEY.txt")
    logger.info(f"Tổng số API keys: {len(api_keys)}")
    data = read_dataset("test.json")
    logger.info(f"Tổng số sample: {len(data)}")
    num_parts = len(api_keys)
    if num_parts < 1:
        logger.warning(f"Chỉ có {num_parts} API key, sẽ chia dataset thành {num_parts} phần thay vì 40")
    split_data = split_dataset(data, num_parts)
    logger.info(f"Đã chia thành {len(split_data)} phần, mỗi phần khoảng {len(split_data[0])} sample")
    checkpoint_dir = "checkpoints_test"
    os.makedirs(checkpoint_dir, exist_ok=True)
    # Tạo danh sách các phần chưa xử lý
    parts_to_process = [
        (i, part_data, os.path.join(checkpoint_dir, f"checkpoint_part_{i}.json"))
        for i, part_data in enumerate(split_data)
    ]
    available_keys = api_keys.copy()
    key_status = {k: True for k in available_keys}
    while parts_to_process and available_keys:
        batch = []
        all_checkpoints = load_all_checkpoints(checkpoint_dir)
        for idx, (part_index, part_data, checkpoint_file) in enumerate(parts_to_process[:len(available_keys)]):
            api_key = available_keys[idx]
            batch.append((part_index, part_data, api_key, checkpoint_file, all_checkpoints))
        with ThreadPoolExecutor(max_workers=len(batch)) as executor:
            futures = []
            for part_index, part_data, api_key, checkpoint_file, all_checkpoints in batch:
                futures.append(executor.submit(process_part, part_data, part_index, api_key, checkpoint_file, all_checkpoints))
            for i, future in enumerate(futures):
                key_alive = future.result()
                if not key_alive:
                    key = batch[i][2]
                    key_status[key] = False
        available_keys = [k for k in available_keys if key_status[k]]
        parts_to_process = parts_to_process[len(batch):]
        # Kiểm tra lại các checkpoint chưa xử lý xong (có thể kiểm tra file part_{i}_failed.json)
        retry_parts = []
        for part_index, part_data, checkpoint_file in parts_to_process:
            failed_path = os.path.join("processed_parts", f"part_{part_index}_failed.json")
            if os.path.exists(failed_path):
                with open(failed_path, 'r', encoding='utf-8') as f:
                    retry_data = json.load(f)
                retry_parts.append((part_index, retry_data, checkpoint_file))
        parts_to_process = retry_parts
    logger.info("Đã hoàn thành xử lý rút gọn tên sản phẩm.")

def load_all_checkpoints(checkpoint_dir):
    all_checkpoints = {}
    if not os.path.exists(checkpoint_dir):
        return all_checkpoints
    for fname in os.listdir(checkpoint_dir):
        if fname.endswith('.json'):
            fpath = os.path.join(checkpoint_dir, fname)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        all_checkpoints.update(data)
            except Exception as e:
                logger.error(f"Lỗi khi đọc checkpoint {fpath}: {e}")
    return all_checkpoints

if __name__ == "__main__":
    main()