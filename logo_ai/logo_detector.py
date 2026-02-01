import pandas as pd
import cv2
import os
from ultralytics import YOLO

model = YOLO("yolov8n.pt")
input_excel = r"C:\Users\PRKV2501\Documents\qc_hub\F1\logo_ai\input\test.xlsx"
output_folder = r"C:\Users\PRKV2501\Documents\qc_hub\F1\logo_ai\output"

print("Looking for:", input_excel)
print("Exists:", os.path.exists(input_excel))

df = pd.read_excel(input_excel)

os.makedirs(output_folder, exist_ok=True)

for i, row in df.iterrows():
    img_path = row["Image_Path"]
    print("Processing:", img_path)

    if not os.path.exists(img_path):
        print("❌ Image not found")
        continue

    results = model(img_path)

    img = cv2.imread(img_path)
    h_img, w_img = img.shape[:2]

    logo_count = 0

    for r in results:
        for box in r.boxes:
            x1, y1, x2, y2 = box.xyxy[0]
            conf = float(box.conf[0])

            if conf < 0.4:
                continue   # ignore weak detections

            x1 = int(x1)
            y1 = int(y1)
            x2 = int(x2)
            y2 = int(y2)

            crop = img[y1:y2, x1:x2]

            logo_path = os.path.join(output_folder, f"logo_{i}_{logo_count}.jpg")
            cv2.imwrite(logo_path, crop)

            print("Detected logo:", logo_path, "Confidence:", conf)

            logo_count += 1