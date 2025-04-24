from flask import Flask, render_template, request, jsonify, send_from_directory
from vector_database import VectorDB
import os
import shutil
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static', static_url_path='/static')

load_dotenv()
# Lấy API key từ biến môi trường
QDRANT_API_KEY = os.getenv('QDRANT_API_KEY')
if not QDRANT_API_KEY:
    print("Cảnh báo: QDRANT_API_KEY không được cung cấp trong biến môi trường. Sử dụng giá trị mặc định.")

qdrant_manager = VectorDB(
    api='http://aienthusiasm:6333',
    timeout=200.0,
    api_key=QDRANT_API_KEY
)

# Định nghĩa hàm clear_directory trước khi sử dụng
def clear_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

image_folder = os.path.join('static', 'temporary', 'images')
os.makedirs(image_folder, exist_ok=True)

# Xóa các file hình ảnh tạm thời khi khởi động
if os.path.exists(image_folder):
    clear_directory(image_folder)

@app.route('/')
def home():
    return render_template('newhome.html')

@app.route('/static/<path:filename>')
def custom_static(filename):
    return send_from_directory('static', filename)

@app.route('/search', methods=['POST'])
def search():
    query_text = request.form.get('query')
    logger.debug(f"Received search query: {query_text}")

    if not query_text:
        return jsonify({'error': 'No query or files provided'}), 400

    return search_images(query_text)

def process_qdrant_results(qdrant_results):
    logger.debug(f"Processing {len(qdrant_results)} results from Qdrant")
    products = {}

    for result in qdrant_results:
        product_id = result.id
        payload = result.payload
        logger.debug(f"Processing product ID: {product_id}")
        
        # Tạo đường dẫn tương đối với static_url_path
        output_path = os.path.join('temporary', 'images', f"product_{product_id}.jpg").replace('\\', '/')
        full_path = os.path.join('static', output_path)
        logger.debug(f"Saving image to: {full_path}")
        
        # Kiểm tra và tạo thư mục nếu chưa tồn tại
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        # Giải mã và lưu ảnh
        try:
            qdrant_manager.decode_and_decompress_image(payload['image_base64'], full_path)
            logger.debug(f"Successfully saved image for product {product_id}")
        except Exception as e:
            logger.error(f"Error saving image for product {product_id}: {str(e)}")
            continue
        
        # Sử dụng URL tương đối với static_url_path
        relative_path = f"/static/{output_path}"
        logger.debug(f"Generated relative path: {relative_path}")
        
        if product_id not in products:
            products[product_id] = {
                'metadata': {
                    'name': payload['name'],
                    'price': payload['price'],
                    'original_price': payload['original_price'],
                    'description': payload['description'],
                    'short_url': payload['short_url'],
                    'discount_percentage': payload['discount_percentage'],
                    'image_path': relative_path
                }
            }

    logger.debug(f"Returning {len(products)} processed products")
    return products

@app.route('/search_images', methods=['GET', 'POST'])
def search_images(query_text=None):
    if request.method == 'GET':
        query_text = request.args.get('query')
    elif request.method == 'POST':
        query_text = request.form.get('query')

    logger.debug(f"Searching images with query: {query_text}")

    if not query_text:
        return jsonify({'error': 'Query text is required'}), 400

    qdrant_results = qdrant_manager.query_dataset(query_text)
    logger.debug(f"Received {len(qdrant_results) if qdrant_results else 0} results from Qdrant")
    
    if not qdrant_results:
        return jsonify({'error': 'No products found. Try a different query.'}), 404

    products = process_qdrant_results(qdrant_results)
    logger.debug(f"Processed {len(products)} products")

    metadata_list = [product['metadata'] for product in products.values()]
    image_paths = [product['metadata']['image_path'] for product in products.values()]

    response_data = {
        'image_paths': image_paths,
        'metadata_list': metadata_list,
    }
    logger.debug(f"Sending response with {len(image_paths)} images")
    return jsonify(response_data)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)