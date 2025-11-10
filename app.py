from flask import Flask, render_template, request, jsonify, send_file
import os
import boto3
from werkzeug.utils import secure_filename
import json
from ocr import extract_expense_data, extract_complete_invoice_data
from invoice_normalizer import normalize_invoice_from_json
from datetime import datetime

app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'pdf'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Create upload folder if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        return '', 204  # Prevent form resubmission on refresh
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload, process invoice with OCR and normalize data"""
    try:
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Only PDF files are allowed'}), 400
        
        # Save the file
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        arquivo_id = f"{timestamp}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], arquivo_id)
        file.save(filepath)
        
        # Get AWS credentials
        aws_profile = request.form.get('aws_profile', 'default')
        aws_region = request.form.get('aws_region', 'us-east-1')
        
        # Initialize AWS Textract client
        try:
            if aws_profile and aws_profile != 'default':
                session = boto3.Session(profile_name=aws_profile)
            else:
                session = boto3.Session()
            
            client = session.client('textract', region_name=aws_region)
        except Exception as e:
            return jsonify({'error': f'AWS configuration error: {str(e)}'}), 500
        
        # Extract OCR data
        try:
            ocr_data = extract_complete_invoice_data(client, filepath)
            
            # Save raw OCR data
            ocr_filename = arquivo_id.replace('.pdf', '_ocr.json')
            ocr_path = os.path.join('output', ocr_filename)
            os.makedirs('output', exist_ok=True)
            
            with open(ocr_path, 'w', encoding='utf-8') as f:
                json.dump(ocr_data, f, indent=2, ensure_ascii=False)
            
            # Normalize the data
            input_data = {
                'arquivo_id': arquivo_id,
                'arquivo_nome': filename,
                'ocr_json': ocr_data,
                'raw_text': ocr_data.get('raw_text', '')
            }
            
            normalized_data = normalize_invoice_from_json(input_data)
            
            # Save normalized data
            normalized_filename = arquivo_id.replace('.pdf', '_normalized.json')
            normalized_path = os.path.join('output', normalized_filename)
            
            with open(normalized_path, 'w', encoding='utf-8') as f:
                json.dump(normalized_data, f, indent=2, ensure_ascii=False)
            
            return jsonify({
                'success': True,
                'arquivo_id': arquivo_id,
                'ocr_data': ocr_data,
                'normalized_data': normalized_data,
                'files': {
                    'uploaded': arquivo_id,
                    'ocr_json': ocr_filename,
                    'normalized_json': normalized_filename
                }
            })
        
        except Exception as e:
            return jsonify({'error': f'Error processing invoice: {str(e)}'}), 500
    
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500



@app.route('/download/<filename>')
def download_file(filename):
    """Download extracted data as JSON"""
    try:
        filepath = os.path.join('output', filename)
        if os.path.exists(filepath):
            return send_file(filepath, as_attachment=True)
        else:
            return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/clear-data', methods=['POST'])
def clear_data():
    """Clear all uploaded files and extracted data"""
    try:
        import shutil
        
        deleted_files = {
            'uploads': 0,
            'output': 0
        }
        
        # Clear uploads folder
        if os.path.exists(UPLOAD_FOLDER):
            for filename in os.listdir(UPLOAD_FOLDER):
                file_path = os.path.join(UPLOAD_FOLDER, filename)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                        deleted_files['uploads'] += 1
                except Exception as e:
                    print(f"Error deleting {file_path}: {e}")
        
        # Clear output folder
        if os.path.exists('output'):
            for filename in os.listdir('output'):
                file_path = os.path.join('output', filename)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                        deleted_files['output'] += 1
                except Exception as e:
                    print(f"Error deleting {file_path}: {e}")
        
        return jsonify({
            'success': True,
            'message': f"Dados limpos com sucesso! {deleted_files['uploads']} uploads e {deleted_files['output']} arquivos de saída removidos.",
            'deleted_files': deleted_files
        })
    
    except Exception as e:
        return jsonify({'error': f'Erro ao limpar dados: {str(e)}'}), 500

@app.route('/graficos')
def graficos():
    """Render the graphs page"""
    return render_template('graficos.html')

@app.route('/api/dados-graficos', methods=['GET'])
def dados_graficos():
    try:
        from pathlib import Path
        from datetime import datetime
        
        # Load normalized invoices
        output_folder = Path('output')
        faturas = []
        
        # Try normalized files first
        for json_file in output_folder.glob('*_normalized.json'):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Accept invoice if it has at least one valid field
                    consumo = data.get('consumo_kwh')
                    valor = data.get('valor_total')
                    
                    # Try to extract from outros if main fields are None or 0
                    if not valor:  # Covers None, 0, 0.0
                        outros = data.get('detalhe_componentes', {}).get('outros', [])
                        if outros and len(outros) > 0:
                            # Use the first value as total (usually the most recent month)
                            valor = outros[0].get('valor')
                            if valor:
                                data['valor_total'] = valor
                    
                    # Accept if has data_fim (date) regardless of values
                    if data.get('data_fim') or consumo is not None or valor is not None:
                        faturas.append(data)
            except Exception:
                continue
        
        # If no normalized files, process existing JSONs
        if not faturas:
            for json_file in output_folder.glob('*.json'):
                if '_normalized' in json_file.name or '_ocr' in json_file.name:
                    continue
                
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        ocr_data = json.load(f)
                    
                    result = normalize_invoice_from_json({
                        'arquivo_nome': json_file.name,
                        'ocr_json': ocr_data
                    })
                    
                    consumo = result.get('consumo_kwh')
                    valor = result.get('valor_total')
                    
                    # Try to extract from outros if main fields are None or 0
                    if not valor:  # Covers None, 0, 0.0
                        outros = result.get('detalhe_componentes', {}).get('outros', [])
                        if outros and len(outros) > 0:
                            valor = outros[0].get('valor')
                            if valor:
                                result['valor_total'] = valor
                    
                    # Accept if has data_fim (date) regardless of values
                    if result.get('data_fim') or consumo is not None or valor is not None:
                        faturas.append(result)
                except Exception:
                    continue
        
        if not faturas:
            return jsonify({
                'success': True,
                'labels': [],
                'datasets': {
                    'consumo': [],
                    'valor': [],
                    'icms': [],
                    'pis': [],
                    'cofins': []
                },
                'stats': {
                    'total_faturas': 0,
                    'consumo_total': 0,
                    'consumo_medio': 0,
                    'valor_total': 0,
                    'valor_medio': 0,
                    'tarifa_media': 0
                },
                'message': 'Nenhuma fatura encontrada. Faça upload de faturas primeiro.'
            })
        
        # Sort by date (handle None values)
        faturas.sort(key=lambda x: x.get('data_fim') or '')
        
        # Prepare data for charts
        labels = []
        consumos = []
        valores = []
        icms_vals = []
        pis_vals = []
        cofins_vals = []
        
        for fatura in faturas:
            if fatura.get('data_fim'):
                try:
                    data = datetime.strptime(fatura['data_fim'], '%Y-%m-%d')
                    labels.append(data.strftime('%b/%Y'))
                    
                    consumos.append(float(fatura.get('consumo_kwh') or 0))
                    valores.append(float(fatura.get('valor_total') or 0))
                    
                    componentes = fatura.get('detalhe_componentes', {})
                    icms_vals.append(float(componentes.get('icms') or 0))
                    pis_vals.append(float(componentes.get('pis') or 0))
                    cofins_vals.append(float(componentes.get('cofins') or 0))
                except Exception as e:
                    continue
        
        # Check if we have valid data after processing
        if not labels:
            return jsonify({
                'success': True,
                'labels': [],
                'datasets': {
                    'consumo': [],
                    'valor': [],
                    'icms': [],
                    'pis': [],
                    'cofins': []
                },
                'stats': {
                    'total_faturas': 0,
                    'consumo_total': 0,
                    'consumo_medio': 0,
                    'valor_total': 0,
                    'valor_medio': 0,
                    'tarifa_media': 0
                },
                'message': f'Encontradas {len(faturas)} faturas, mas nenhuma possui data válida para exibição.'
            })
        
        # Calculate statistics
        consumo_total = sum(consumos)
        valor_total = sum(valores)
        num_faturas_validas = len(consumos)
        
        stats = {
            'total_faturas': num_faturas_validas,
            'consumo_total': consumo_total,
            'consumo_medio': consumo_total / num_faturas_validas if num_faturas_validas > 0 else 0,
            'valor_total': valor_total,
            'valor_medio': valor_total / num_faturas_validas if num_faturas_validas > 0 else 0,
            'tarifa_media': valor_total / consumo_total if consumo_total > 0 else 0
        }
        
        return jsonify({
            'success': True,
            'labels': labels,
            'datasets': {
                'consumo': consumos,
                'valor': valores,
                'icms': icms_vals,
                'pis': pis_vals,
                'cofins': cofins_vals
            },
            'stats': stats
        })
    
    except Exception as e:
        return jsonify({'error': f'Erro ao carregar dados: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
