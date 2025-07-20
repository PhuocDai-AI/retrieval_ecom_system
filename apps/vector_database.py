import numpy as np
import torch
from PIL import Image
import base64
import io
import gzip
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, Distance, Filter, FieldCondition, MatchValue
from transformers import AlignProcessor, AlignModel, AutoProcessor, AutoModelForZeroShotImageClassification, AutoTokenizer, RobertaConfig, AlignConfig, AlignTextConfig, AutoModel
import logging

class VectorDB:
    def __init__(self, api='http://aienthusiasm:6333', timeout=200.0, device="cuda:0" if torch.cuda.is_available() else "cpu", api_key= None):
        """Initialize QdrantImageModule with host, port, and processing mode (local or api)."""
        self.device = device
        self.api_url = api
        self.timeout = timeout
        self.api_key = api_key

        # Khởi tạo Qdrant Client
        self.client = QdrantClient(
            url=self.api_url,
            api_key=self.api_key,
            timeout=self.timeout,
            https=False  
        )

        # Kiểm tra kết nối
        try:
            self.client.get_collections()
            print("Kết nối thành công đến Qdrant!")
        except Exception as e:
            print(f"Không thể kết nối đến Qdrant: {e}")
            raise
        tokenizer = AutoTokenizer.from_pretrained("vinai/phobert-base")

        self.processor_align = AutoProcessor.from_pretrained("/home/phuocdai/retrieval_ecom_system/finetuned_align_v1", tokenizer=tokenizer)
        self.model_align = AutoModelForZeroShotImageClassification.from_pretrained("/home/phuocdai/retrieval_ecom_system/finetuned_align_v1").to(self.device)

    def text_encode(self, text):
        """Mã hóa văn bản thành vector."""
        processed_text = self.processor_align(text=text, return_tensors="pt").to(self.device)
        with torch.no_grad():
            text_features = self.model_align.get_text_features(
                input_ids=processed_text['input_ids'],
                attention_mask=processed_text['attention_mask']
            ).cpu().numpy().flatten()
        return text_features.tolist()

    def query_dataset(self, query_text=None):
        """Tìm kiếm dữ liệu trong dataset bằng văn bản hoặc hình ảnh."""
        if not query_text:
            raise ValueError("Cần cung cấp query_text hoặc files_search để tìm kiếm")

        vector = self.text_encode(query_text)
        if vector is None:
            print("Không thể tạo vector tìm kiếm")
            return []

        try:
            qdrant_results = self.client.search(
                collection_name="e_com",
                query_vector=vector,
                limit=30
            )
            return qdrant_results
        except Exception as e:
            print(f"Lỗi khi truy vấn dataset: {e}")
            return []

    def decode_and_decompress_image(self, base64_str, output_path):
        """Giải mã base64 và lưu ảnh."""
        try:
            # Giải mã base64 string thành bytes
            image_bytes = base64.b64decode(base64_str)
            
            # Mở ảnh từ bytes
            image = Image.open(io.BytesIO(image_bytes))
            
            # Chuyển đổi sang RGB nếu ảnh đang ở chế độ RGBA
            if image.mode == 'RGBA':
                # Tạo background trắng
                background = Image.new('RGB', image.size, (255, 255, 255))
                # Paste ảnh RGBA lên background với alpha channel
                background.paste(image, mask=image.split()[3])
                image = background
            elif image.mode != 'RGB':
                # Chuyển đổi các mode khác sang RGB
                image = image.convert('RGB')
            
            # Lưu ảnh dưới dạng JPEG
            image.save(output_path, 'JPEG', quality=95)
            logging.info(f"Ảnh đã được lưu thành công tại {output_path}")
            
        except Exception as e:
            logging.error(f"Lỗi khi giải mã và lưu ảnh: {e}")
            # Tạo một ảnh placeholder đơn giản nếu có lỗi
            try:
                placeholder = Image.new('RGB', (300, 300), (200, 200, 200))
                placeholder.save(output_path, 'JPEG')
                logging.info(f"Đã tạo ảnh placeholder tại {output_path}")
            except Exception as e:
                logging.error(f"Không thể tạo ảnh placeholder: {e}")

    def image_encode(self, image: Image.Image):
        """Mã hóa ảnh thành vector."""
        processed = self.processor_align(images=image, return_tensors="pt").to(self.device)
        with torch.no_grad():
            image_features = self.model_align.get_image_features(
                pixel_values=processed['pixel_values']
            ).cpu().numpy().flatten()
        return image_features.tolist()
