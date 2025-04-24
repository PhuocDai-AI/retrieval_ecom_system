import torch
import base64
import json
from PIL import Image
from io import BytesIO
from transformers import AutoProcessor, AutoModel
from torchvision import transforms


# check gpu
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")


# load model
processor = AutoProcessor.from_pretrained("./finetuned_align_v1")
model = AutoModel.from_pretrained("./finetuned_align_v1")

model.to(device)


# define transform
transform = transforms.Compose([
    transforms.Resize((280, 280)),  # resize to 280x280
    transforms.ToTensor(), # convert to tensor
])

# load data
file_path = "test.json"
with open(file_path, 'r') as file:
    data = json.load(file)

# image embedding
def get_image_embedding(img_base64):
    try:
        # decode base64 image
        img_data = base64.b64decode(img_base64)
        img = Image.open(BytesIO(img_data)).convert("RGB")
        
        # convert image to tensor
        img_tensor = transform(img).unsqueeze(0).to(device)  

        with torch.no_grad():
            image_embeddings = model.get_image_features(pixel_values=img_tensor)
        
        return image_embeddings.squeeze().cpu().numpy()  

    except Exception as e:
        print(f"Error processing image: {e}")
        return
    
    except ValueError as e:
        # skip error
        print(f"Warning: Skipping sample due to error: {e}")
        return

# create JSON data structure
def create_json_data(id, name, image_base64, vector):
    if vector is None:
        return None
    
    return {
        'id': id,
        'name': name,
        'image_base64': image_base64,
        'vector': vector.tolist()
    }


# loop through data
output_data = []
for item in data:
    id = item["id"]
    name = item["name"]
    image_base64 = item["image_base64"]

    image_embedding = get_image_embedding(image_base64)

    json_data = create_json_data(id, name, image_base64, image_embedding)
    
    if json_data:
        output_data.append(json_data)

# save to JSON file
output_file_path = "./vector-payload.json"
with open(output_file_path, 'w', encoding='utf-8') as f:
    json.dump(output_data, f, ensure_ascii=False, indent=2)

