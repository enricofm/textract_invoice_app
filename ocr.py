import boto3
import io
from PIL import Image, ImageDraw
from pdf2image import convert_from_path

def draw_bounding_box(key, val, width, height, draw):
    if "Geometry" in key:
        box = val["BoundingBox"]
        left = width * box['Left']
        top = height * box['Top']
        draw.rectangle([left, top, left + (width * box['Width']), top + (height * box['Height'])],
                       outline='black')

def print_labels_and_values(field):
    if "LabelDetection" in field:
        print("Summary Label Detection - Confidence: {}".format(
            str(field.get("LabelDetection")["Confidence"])) + ", "
              + "Summary Values: {}".format(str(field.get("LabelDetection")["Text"])))
        print(field.get("LabelDetection")["Geometry"])
    if "ValueDetection" in field:
        print("Summary Value Detection - Confidence: {}".format(
            str(field.get("ValueDetection")["Confidence"])) + ", "
              + "Summary Values: {}".format(str(field.get("ValueDetection")["Text"])))
        print(field.get("ValueDetection")["Geometry"])

def process_expense_analysis(client, document_path):
    """
    Process a local PDF file for expense analysis
    
    Args:
        client: boto3 Textract client
        document_path: Path to the local PDF file
    
    Returns:
        dict: Extracted expense data
    """
    # Read the local document
    with open(document_path, 'rb') as document_file:
        document_bytes = document_file.read()
    
    # Analyze document using bytes
    response = client.analyze_expense(
        Document={'Bytes': document_bytes})
    
    # For visualization, convert first page to image
    from pdf2image import convert_from_path
    images = convert_from_path(document_path, first_page=1, last_page=1)
    image = images[0] if images else None

    # Set width and height to display image and draw bounding boxes
    # Create drawing object
    width, height = image.size
    draw = ImageDraw.Draw(image)

    for expense_doc in response["ExpenseDocuments"]:
        for line_item_group in expense_doc["LineItemGroups"]:
            for line_items in line_item_group["LineItems"]:
                for expense_fields in line_items["LineItemExpenseFields"]:
                    print_labels_and_values(expense_fields)
                    print()

        print("Summary:")
        for summary_field in expense_doc["SummaryFields"]:
            print_labels_and_values(summary_field)
            print()

        for line_item_group in expense_doc["LineItemGroups"]:
            for line_items in line_item_group["LineItems"]:
                for expense_fields in line_items["LineItemExpenseFields"]:
                    for key, val in expense_fields["ValueDetection"].items():
                        if "Geometry" in key:
                            draw_bounding_box(key, val, width, height, draw)

        for label in expense_doc["SummaryFields"]:
            if "LabelDetection" in label:
                for key, val in label["LabelDetection"].items():
                    draw_bounding_box(key, val, width, height, draw)

    # Display the image
    image.show()
    
    return response

def extract_expense_data(client, document_path):
    """
    Extract expense data from a local PDF file without visualization
    
    Args:
        client: boto3 Textract client
        document_path: Path to the local PDF file
    
    Returns:
        dict: Structured expense data
    """
    try:
        # First, try to process the PDF directly
        with open(document_path, 'rb') as document_file:
            document_bytes = document_file.read()
        
        response = client.analyze_expense(
            Document={'Bytes': document_bytes})
    
    except Exception as e:
        # If direct PDF processing fails, convert to image first
        if 'UnsupportedDocumentException' in str(e) or 'InvalidParameterException' in str(e):
            print(f"Direct PDF processing failed, converting to image: {e}")
            
            # Try to find Poppler in common locations
            import os
            poppler_paths = [
                r"C:\Program Files\poppler-25.07.0\Library\bin",
                r"C:\Program Files\poppler\Library\bin",
                r"C:\Program Files (x86)\poppler\Library\bin",
                r"C:\poppler\poppler-24.08.0\Library\bin",
                r"C:\poppler\Library\bin",
                r"C:\ProgramData\chocolatey\lib\poppler\tools\Library\bin",
            ]
            
            poppler_path = None
            for path in poppler_paths:
                if os.path.exists(path):
                    poppler_path = path
                    break
            
            # Convert first page of PDF to image
            from pdf2image import convert_from_path
            try:
                if poppler_path:
                    images = convert_from_path(document_path, first_page=1, last_page=1, dpi=300, poppler_path=poppler_path)
                else:
                    images = convert_from_path(document_path, first_page=1, last_page=1, dpi=300)
            except Exception as pdf_error:
                raise Exception(f"Poppler not found. Please install Poppler: {pdf_error}")
            
            if not images:
                raise Exception("Failed to convert PDF to image")
            
            # Convert PIL Image to bytes
            import io
            img_byte_arr = io.BytesIO()
            images[0].save(img_byte_arr, format='PNG')
            img_byte_arr.seek(0)
            document_bytes = img_byte_arr.read()
            
            # Try again with image
            response = client.analyze_expense(
                Document={'Bytes': document_bytes})
        else:
            raise
    
    # Extract structured data
    extracted_data = {
        'summary_fields': [],
        'line_items': []
    }
    
    for expense_doc in response["ExpenseDocuments"]:
        # Extract summary fields
        for summary_field in expense_doc["SummaryFields"]:
            field_data = {}
            if "LabelDetection" in summary_field:
                field_data['label'] = summary_field["LabelDetection"].get("Text", "")
                field_data['label_confidence'] = summary_field["LabelDetection"].get("Confidence", 0)
            if "ValueDetection" in summary_field:
                field_data['value'] = summary_field["ValueDetection"].get("Text", "")
                field_data['value_confidence'] = summary_field["ValueDetection"].get("Confidence", 0)
            if field_data:
                extracted_data['summary_fields'].append(field_data)
        
        # Extract line items
        for line_item_group in expense_doc["LineItemGroups"]:
            for line_item in line_item_group["LineItems"]:
                item_data = {}
                for expense_field in line_item["LineItemExpenseFields"]:
                    if "LabelDetection" in expense_field:
                        label = expense_field["LabelDetection"].get("Text", "")
                    else:
                        label = "unknown"
                    
                    if "ValueDetection" in expense_field:
                        value = expense_field["ValueDetection"].get("Text", "")
                        item_data[label] = value
                
                if item_data:
                    extracted_data['line_items'].append(item_data)
    
    return extracted_data

def extract_tables_from_document(client, document_bytes):
    """
    Extract tables from document using Textract TABLES feature
    
    Args:
        client: boto3 Textract client
        document_bytes: Document bytes
    
    Returns:
        list: List of extracted tables
    """
    # Analyze document for tables
    response = client.analyze_document(
        Document={'Bytes': document_bytes},
        FeatureTypes=['TABLES']
    )
    
    # Extract tables
    blocks = response['Blocks']
    tables = []
    
    # Create a map of block IDs to blocks
    block_map = {block['Id']: block for block in blocks}
    
    # Find all TABLE blocks
    table_blocks = [block for block in blocks if block['BlockType'] == 'TABLE']
    
    for table_block in table_blocks:
        table_data = []
        
        if 'Relationships' in table_block:
            for relationship in table_block['Relationships']:
                if relationship['Type'] == 'CHILD':
                    # Get all cells in the table
                    cells = []
                    for cell_id in relationship['Ids']:
                        cell_block = block_map.get(cell_id)
                        if cell_block and cell_block['BlockType'] == 'CELL':
                            cells.append(cell_block)
                    
                    # Organize cells by row
                    rows = {}
                    for cell in cells:
                        row_index = cell.get('RowIndex', 0)
                        col_index = cell.get('ColumnIndex', 0)
                        
                        if row_index not in rows:
                            rows[row_index] = {}
                        
                        # Get cell text
                        cell_text = ''
                        if 'Relationships' in cell:
                            for rel in cell['Relationships']:
                                if rel['Type'] == 'CHILD':
                                    for word_id in rel['Ids']:
                                        word_block = block_map.get(word_id)
                                        if word_block and word_block['BlockType'] == 'WORD':
                                            cell_text += word_block.get('Text', '') + ' '
                        
                        rows[row_index][col_index] = cell_text.strip()
                    
                    # Convert to list of lists
                    for row_idx in sorted(rows.keys()):
                        row_data = []
                        for col_idx in sorted(rows[row_idx].keys()):
                            row_data.append(rows[row_idx][col_idx])
                        table_data.append(row_data)
        
        if table_data:
            tables.append(table_data)
    
    return tables

def extract_complete_invoice_data(client, document_path):
    """
    Extract complete invoice data including expenses and tables
    
    Args:
        client: boto3 Textract client
        document_path: Path to the local PDF file
    
    Returns:
        dict: Complete extracted data with expenses and tables
    """
    # Convert PDF to image if needed
    def get_document_bytes(path):
        try:
            with open(path, 'rb') as f:
                return f.read()
        except Exception as e:
            print(f"Error reading file: {e}")
            raise
    
    def convert_to_image(path, page_num=1):
        """Convert PDF page to image bytes"""
        import os
        from pdf2image import convert_from_path
        import io
        
        poppler_paths = [
            r"C:\Program Files\poppler-25.07.0\Library\bin",
            r"C:\Program Files\poppler\Library\bin",
            r"C:\Program Files (x86)\poppler\Library\bin",
            r"C:\poppler\poppler-24.08.0\Library\bin",
            r"C:\poppler\Library\bin",
            r"C:\ProgramData\chocolatey\lib\poppler\tools\Library\bin",
        ]
        
        poppler_path = None
        for p in poppler_paths:
            if os.path.exists(p):
                poppler_path = p
                break
        
        if poppler_path:
            images = convert_from_path(path, first_page=page_num, last_page=page_num, dpi=300, poppler_path=poppler_path)
        else:
            images = convert_from_path(path, first_page=page_num, last_page=page_num, dpi=300)
        
        if not images:
            raise Exception(f"Failed to convert PDF page {page_num} to image")
        
        img_byte_arr = io.BytesIO()
        images[0].save(img_byte_arr, format='PNG')
        img_byte_arr.seek(0)
        return img_byte_arr.read()
    
    # Get document bytes
    document_bytes = get_document_bytes(document_path)
    
    # Initialize result
    result = {
        'summary_fields': [],
        'line_items': [],
        'tables': [],
        'raw_text': ''
    }
    
    # Try to extract expense data (analyze_expense processes all pages automatically)
    try:
        print(f"Processing document with analyze_expense...")
        expense_response = client.analyze_expense(Document={'Bytes': document_bytes})
        
        for expense_doc in expense_response["ExpenseDocuments"]:
            # Extract summary fields
            for summary_field in expense_doc["SummaryFields"]:
                field_data = {}
                if "LabelDetection" in summary_field:
                    field_data['label'] = summary_field["LabelDetection"].get("Text", "")
                    field_data['label_confidence'] = summary_field["LabelDetection"].get("Confidence", 0)
                if "ValueDetection" in summary_field:
                    field_data['value'] = summary_field["ValueDetection"].get("Text", "")
                    field_data['value_confidence'] = summary_field["ValueDetection"].get("Confidence", 0)
                if field_data:
                    result['summary_fields'].append(field_data)
            
            # Extract line items
            for line_item_group in expense_doc["LineItemGroups"]:
                for line_item in line_item_group["LineItems"]:
                    item_data = {}
                    for expense_field in line_item["LineItemExpenseFields"]:
                        if "LabelDetection" in expense_field:
                            label = expense_field["LabelDetection"].get("Text", "")
                        else:
                            label = "unknown"
                        
                        if "ValueDetection" in expense_field:
                            value = expense_field["ValueDetection"].get("Text", "")
                            item_data[label] = value
                    
                    if item_data:
                        result['line_items'].append(item_data)
    
    except Exception as e:
        if 'UnsupportedDocumentException' in str(e):
            print(f"PDF not supported directly, converting pages to images: {e}")
            # Get number of pages
            from pdf2image import pdfinfo_from_path
            import os
            
            poppler_paths = [
                r"C:\Program Files\poppler-25.07.0\Library\bin",
                r"C:\Program Files\poppler\Library\bin",
                r"C:\Program Files (x86)\poppler\Library\bin",
                r"C:\poppler\poppler-24.08.0\Library\bin",
                r"C:\poppler\Library\bin",
                r"C:\ProgramData\chocolatey\lib\poppler\tools\Library\bin",
            ]
            
            poppler_path = None
            for p in poppler_paths:
                if os.path.exists(p):
                    poppler_path = p
                    break
            
            try:
                if poppler_path:
                    pdf_info = pdfinfo_from_path(document_path, poppler_path=poppler_path)
                else:
                    pdf_info = pdfinfo_from_path(document_path)
                num_pages = pdf_info.get('Pages', 1)
            except:
                num_pages = 1
            
            print(f"Processing {num_pages} page(s)...")
            
            # Process each page
            for page_num in range(1, num_pages + 1):
                try:
                    print(f"Processing page {page_num}/{num_pages}...")
                    page_bytes = convert_to_image(document_path, page_num)
                    
                    # Analyze expense for this page
                    page_expense_response = client.analyze_expense(Document={'Bytes': page_bytes})
                    
                    for expense_doc in page_expense_response["ExpenseDocuments"]:
                        for summary_field in expense_doc["SummaryFields"]:
                            field_data = {}
                            if "LabelDetection" in summary_field:
                                field_data['label'] = summary_field["LabelDetection"].get("Text", "")
                                field_data['label_confidence'] = summary_field["LabelDetection"].get("Confidence", 0)
                            if "ValueDetection" in summary_field:
                                field_data['value'] = summary_field["ValueDetection"].get("Text", "")
                                field_data['value_confidence'] = summary_field["ValueDetection"].get("Confidence", 0)
                            if field_data:
                                result['summary_fields'].append(field_data)
                        
                        # Extract line items from this page
                        for line_item_group in expense_doc["LineItemGroups"]:
                            for line_item in line_item_group["LineItems"]:
                                item_data = {}
                                for expense_field in line_item["LineItemExpenseFields"]:
                                    if "LabelDetection" in expense_field:
                                        label = expense_field["LabelDetection"].get("Text", "")
                                    else:
                                        label = "unknown"
                                    
                                    if "ValueDetection" in expense_field:
                                        value = expense_field["ValueDetection"].get("Text", "")
                                        item_data[label] = value
                                
                                if item_data:
                                    result['line_items'].append(item_data)
                    
                    # Extract tables from this page
                    page_tables = extract_tables_from_document(client, page_bytes)
                    result['tables'].extend(page_tables)
                    
                except Exception as page_error:
                    print(f"Error processing page {page_num}: {page_error}")
                    continue
        else:
            print(f"Expense analysis error: {e}")
    
    # Extract tables (only if not already extracted page-by-page)
    if not result['tables']:
        try:
            print(f"Extracting tables from document...")
            tables = extract_tables_from_document(client, document_bytes)
            result['tables'] = tables
        except Exception as e:
            print(f"Table extraction error: {e}")
    
    return result
