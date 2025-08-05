# app/services/ocr_text_processor/ocr_service.py - OCR processing service
import sys
from pathlib import Path
import cv2
import numpy as np
from PIL import Image
import pytesseract
import logging
from typing import Tuple

# Add project root to path for imports
current_file = Path(__file__)  # ocr_service.py
services_dir = current_file.parent.parent  # services/
app_dir = services_dir.parent  # app/
project_root = app_dir.parent  # project root

sys.path.insert(0, str(project_root))
sys.path.insert(0, str(app_dir))

logger = logging.getLogger(__name__)

class OCRService:
    """Handles OCR processing operations - extracted from your RealtimeOCRProcessor class"""
    
    def __init__(self):
        pass
    
    def check_tesseract_installation(self) -> bool:
        """Check if Tesseract is properly installed - exact logic from your original"""
        try:
            version = pytesseract.get_tesseract_version()
            logger.info(f"Tesseract version: {version}")
            
            languages = pytesseract.get_languages()
            logger.info(f"Available languages: {len(languages)} found")
            
            # Check for Chinese languages - exactly like your original
            chinese_langs = [lang for lang in languages if 'chi' in lang]
            if chinese_langs:
                logger.info(f"Chinese language packs: {chinese_langs}")
            else:
                logger.warning("No Chinese language packs found")
            
            return True
            
        except Exception as e:
            logger.error(f"Tesseract check failed: {str(e)}")
            return False
    
    def preprocess_image(self, image_path: Path):
        """Preprocess image for better OCR accuracy - EXACT logic from your original"""
        try:
            # Read image
            image = cv2.imread(str(image_path))
            if image is None:
                return None
            
            # Convert to grayscale
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Apply denoising (fast version for real-time)
            denoised = cv2.fastNlMeansDenoising(gray, h=10)
            
            # Apply adaptive thresholding
            thresh = cv2.adaptiveThreshold(
                denoised, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
            )
            
            return thresh
            
        except Exception as e:
            logger.error(f"Error preprocessing {image_path}: {str(e)}")
            return None
    
    def perform_fast_ocr(self, image) -> Tuple[str, float, str]:
        """Perform fast OCR optimized for real-time processing - EXACT logic from your original"""
        try:
            # Convert to PIL Image
            pil_image = Image.fromarray(image)
            
            # Fast OCR configuration - exactly like your original
            config = r'--oem 3 --psm 6'
            
            # Try languages in order of speed (fastest first) - exactly like your original
            languages = ['chi_sim+eng']
            
            for lang in languages:
                try:
                    # Quick confidence check first
                    data = pytesseract.image_to_data(
                        pil_image, lang=lang, config=config, 
                        output_type=pytesseract.Output.DICT
                    )
                    
                    confidences = [int(conf) for conf in data['conf'] if int(conf) > 0]
                    avg_confidence = sum(confidences) / len(confidences) if confidences else 0
                    
                    # If confidence is good enough, use this result - exactly like your original
                    if avg_confidence > 30:  # Lower threshold for speed
                        result = pytesseract.image_to_string(pil_image, lang=lang, config=config)
                        return result.strip(), avg_confidence, lang
                    
                except Exception:
                    continue
            
            # Fallback to basic English OCR - exactly like your original
            result = pytesseract.image_to_string(pil_image, lang='eng')
            return result.strip(), 0, 'eng'
            
        except Exception as e:
            logger.error(f"OCR error: {str(e)}")
            return "", 0, "error"
    
    def clean_ocr_text(self, text: str) -> str:
        """Clean OCR text for better readability - EXACT logic from your original"""
        if not text:
            return ""
        
        lines = text.split('\n')
        cleaned_lines = [line.strip() for line in lines if line.strip()]
        return '\r\n'.join(cleaned_lines)
    
    def process_image_ocr(self, image_path: Path) -> Tuple[str, float, str]:
        """Complete OCR processing pipeline - combines your preprocessing and OCR logic"""
        try:
            # Preprocess image
            preprocessed = self.preprocess_image(image_path)
            if preprocessed is None:
                return "", 0, "preprocessing_error"
            
            # Perform OCR
            ocr_text, confidence, language = self.perform_fast_ocr(preprocessed)
            
            # Clean text
            cleaned_text = self.clean_ocr_text(ocr_text)
            
            return cleaned_text, confidence, language
            
        except Exception as e:
            logger.error(f"Error in OCR pipeline for {image_path}: {e}")
            return "", 0, "pipeline_error"