import pytesseract
from pdf2image import convert_from_bytes

# TRY https://github.com/loadlost/PP-Parser/blob/master/pdf_parser.py


def extract_text_from_pdf_images(file_bytes: bytes, lang=None) -> str:

    images = convert_from_bytes(file_bytes)
    # images  = convert_from_path(temp_pdf.name)  # This will raise an exception if the file is not a valid PDF
    extracted_text = ""
    for i, image in enumerate(images):
        if lang:
            text = pytesseract.image_to_string(image, lang=lang)
        else:
            text = pytesseract.image_to_string(image)
        extracted_text += text
    return extracted_text
