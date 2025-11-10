"""
Invoice Normalizer Service
---------------------------
Extracts and normalizes invoice fields from AWS Textract OCR JSON output.
Returns standardized JSON with confidence levels and raw snippets for auditing.
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any


class InvoiceNormalizer:
    """Normalizes invoice data from Textract OCR JSON"""
    
    # Keywords for field matching (Portuguese)
    KEYWORDS = {
        'unidade_consumidora': ['MATRÍCULA', 'MATRICULA', 'UNIDADE CONSUMIDORA', 'UC'],
        'codigo_instalacao': ['CÓDIGO DA INSTALAÇÃO', 'CODIGO DA INSTALACAO', 'INSTALAÇÃO', 'INSTALACAO'],
        'identificador_fatura': ['N° DOCUMENTO', 'Nº DOCUMENTO', 'NOSSO NÚMERO', 'NOTA FISCAL', 'NF'],
        'data_vencimento': ['VENCIMENTO', 'DATA DE VENCIMENTO', 'DATA VENCIMENTO'],
        'data_emissao': ['DATA DE EMISSÃO', 'DATA EMISSAO', 'DATA DO DOCUMENTO'],
        'leitura_anterior': ['LEITURA ANTERIOR', 'LEITURA\nANTERIOR'],
        'leitura_atual': ['LEITURA ATUAL', 'LEITURA\nATUAL'],
        'proxima_leitura': ['PRÓXIMA LEITURA', 'PROXIMA LEITURA', 'PRÓXIMA\nLEITURA'],
        'consumo': ['CONSUMO', 'KWH', 'kWh', 'CONSUMO (KWH)', 'CONSUMO KWH'],
        'valor_total': ['TOTAL A PAGAR', 'VALOR TOTAL', 'TOTAL', 'VALOR DO DOCUMENTO'],
        'tarifa_classe': ['CLASSIFICAÇÃO', 'CLASSIFICACAO', 'CLASSE', 'TARIFA'],
        'demanda_contratada': ['DEMANDA CONTRATADA', 'DEM. CONTRATADA'],
        'demanda_registrada': ['DEMANDA REGISTRADA', 'DEM. REGISTRADA', 'DEMANDA MEDIDA'],
        'bandeira': ['BANDEIRA', 'BAND.'],
        'icms': ['ICMS'],
        'pis': ['PIS'],
        'cofins': ['COFINS'],
        'energia': ['ENERGIA', 'CONSUMO DE ENERGIA', 'ENERGIA ELÉTRICA'],
        'tusd': ['TUSD', 'TUST'],
    }
    
    # Bandeira tarifária patterns
    BANDEIRA_PATTERNS = {
        'VERDE': r'VERDE',
        'AMARELA': r'AMARELA',
        'VERMELHA_P1': r'VERMELHA\s*P1|VERMELHA\s*PATAMAR\s*1',
        'VERMELHA_P2': r'VERMELHA\s*P2|VERMELHA\s*PATAMAR\s*2',
    }
    
    # Tarifa classe patterns
    CLASSE_PATTERNS = {
        'RESIDENCIAL': r'RESIDENCIAL',
        'COMERCIAL': r'COMERCIAL',
        'INDUSTRIAL': r'INDUSTRIAL',
        'GRUPO_A': r'GRUPO\s*A|A4|A3',
        'GRUPO_B': r'GRUPO\s*B|B1|B2|B3',
    }

    def __init__(self):
        self.warnings = []
        self.raw_snippets = []

    def normalize_invoice(self, input_data: Dict) -> Dict:
        """
        Main normalization function
        
        Args:
            input_data: Dictionary with arquivo_id, arquivo_nome, ocr_json, raw_text
            
        Returns:
            Normalized invoice JSON
        """
        self.warnings = []
        self.raw_snippets = []
        
        arquivo_id = input_data.get('arquivo_id', '')
        arquivo_nome = input_data.get('arquivo_nome', '')
        ocr_json = input_data.get('ocr_json', {})
        raw_text = input_data.get('raw_text', '')
        
        # Extract summary fields from Textract
        summary_fields = ocr_json.get('summary_fields', [])
        line_items = ocr_json.get('line_items', [])
        tables = ocr_json.get('tables', [])
        
        # Build field map for easy lookup
        field_map = self._build_field_map(summary_fields)
        
        # Extract all fields
        result = {
            'arquivo_id': arquivo_id,
            'arquivo_nome': arquivo_nome,
            'unidade_consumidora_id': self._extract_unidade_consumidora(field_map),
            'codigo_instalacao': self._extract_codigo_instalacao(field_map),
            'identificador_fatura': self._extract_identificador_fatura(field_map),
            'data_inicio': None,
            'data_fim': None,
            'dias_faturamento': None,
            'leitura_anterior': None,
            'leitura_atual': None,
            'consumo_kwh': None,
            'consumo_estimado': False,
            'valor_total': self._extract_valor_total(field_map),
            'detalhe_componentes': {
                'energia': None,
                'tusd_tust': None,
                'bandeira': None,
                'bandeira_valor': None,
                'icms': None,
                'pis': None,
                'cofins': None,
                'outros': []
            },
            'tarifa_classe': self._extract_tarifa_classe(field_map),
            'demanda_contratada': None,
            'demanda_registrada': None,
            'status_pagamento': None,
            'data_vencimento': self._extract_data_vencimento(field_map),
            'confidence_overall': 'medium',
            'warnings': [],
            'raw_snippets': []
        }
        
        # Extract dates
        data_inicio, data_fim = self._extract_periodo_leitura(field_map)
        result['data_inicio'] = data_inicio
        result['data_fim'] = data_fim
        
        # Calculate billing days
        if data_inicio and data_fim:
            try:
                d1 = datetime.strptime(data_inicio, '%Y-%m-%d')
                d2 = datetime.strptime(data_fim, '%Y-%m-%d')
                result['dias_faturamento'] = (d2 - d1).days + 1
            except Exception as e:
                self.warnings.append(f"Erro ao calcular dias de faturamento: {str(e)}")
        
        # Extract consumption data
        leitura_ant, leitura_atu, consumo = self._extract_consumo_data(field_map, line_items, tables)
        result['leitura_anterior'] = leitura_ant
        result['leitura_atual'] = leitura_atu
        result['consumo_kwh'] = consumo
        
        # Validate consumption calculation
        if leitura_ant is not None and leitura_atu is not None:
            calculated_consumo = leitura_atu - leitura_ant
            if calculated_consumo < 0:
                self.warnings.append(f"Consumo calculado negativo: {calculated_consumo} kWh")
            elif consumo and abs(calculated_consumo - consumo) > 1:
                self.warnings.append(f"Divergência entre consumo informado ({consumo}) e calculado ({calculated_consumo})")
        
        # Extract component details
        result['detalhe_componentes'] = self._extract_componentes(field_map, line_items, tables)
        
        # Extract demand data
        result['demanda_contratada'] = self._extract_demanda_contratada(field_map)
        result['demanda_registrada'] = self._extract_demanda_registrada(field_map)
        
        # Calculate overall confidence
        result['confidence_overall'] = self._calculate_confidence(result, field_map)
        
        # Add warnings and snippets
        result['warnings'] = self.warnings
        result['raw_snippets'] = self.raw_snippets
        
        return result

    def _build_field_map(self, summary_fields: List[Dict]) -> Dict:
        """Build a map of labels to values with confidence"""
        field_map = {}
        for field in summary_fields:
            label = field.get('label', '').upper().strip()
            value = field.get('value', '').strip()
            label_conf = field.get('label_confidence', 0)
            value_conf = field.get('value_confidence', 0)
            
            if label:
                field_map[label] = {
                    'value': value,
                    'label_confidence': label_conf,
                    'value_confidence': value_conf
                }
            
            # Also store by value if no label (for unlabeled fields)
            if not label and value:
                field_map[f"_UNLABELED_{value[:20]}"] = {
                    'value': value,
                    'label_confidence': 0,
                    'value_confidence': value_conf
                }
        
        return field_map

    def _find_field(self, field_map: Dict, keywords: List[str]) -> Optional[Tuple[str, float]]:
        """Find a field by matching keywords"""
        for label, data in field_map.items():
            for keyword in keywords:
                if keyword.upper() in label.upper():
                    self.raw_snippets.append({
                        'campo': keywords[0],
                        'trecho': f"{label}: {data['value']}",
                        'confidence_ocr': data['value_confidence']
                    })
                    return data['value'], data['value_confidence']
        return None, 0

    def _extract_unidade_consumidora(self, field_map: Dict) -> Optional[str]:
        """Extract unidade consumidora ID"""
        value, conf = self._find_field(field_map, self.KEYWORDS['unidade_consumidora'])
        if not value:
            self.warnings.append("Unidade consumidora não encontrada")
        return value

    def _extract_codigo_instalacao(self, field_map: Dict) -> Optional[str]:
        """Extract installation code"""
        value, conf = self._find_field(field_map, self.KEYWORDS['codigo_instalacao'])
        if not value:
            self.warnings.append("Código da instalação não encontrado")
        return value

    def _extract_identificador_fatura(self, field_map: Dict) -> Optional[str]:
        """Extract invoice identifier"""
        value, conf = self._find_field(field_map, self.KEYWORDS['identificador_fatura'])
        if not value:
            self.warnings.append("Identificador da fatura não encontrado")
        return value

    def _extract_data_vencimento(self, field_map: Dict) -> Optional[str]:
        """Extract due date"""
        value, conf = self._find_field(field_map, self.KEYWORDS['data_vencimento'])
        if value:
            return self._normalize_date(value)
        return None

    def _extract_periodo_leitura(self, field_map: Dict) -> Tuple[Optional[str], Optional[str]]:
        """Extract reading period (start and end dates)"""
        data_inicio = None
        data_fim = None
        
        # Try to find "Leitura Anterior" date
        value_ant, _ = self._find_field(field_map, self.KEYWORDS['leitura_anterior'])
        if value_ant:
            data_inicio = self._normalize_date(value_ant)
        
        # Try to find "Leitura Atual" date
        value_atu, _ = self._find_field(field_map, self.KEYWORDS['leitura_atual'])
        if value_atu:
            data_fim = self._normalize_date(value_atu)
        
        if not data_inicio:
            self.warnings.append("Data de início do período não encontrada")
        if not data_fim:
            self.warnings.append("Data de fim do período não encontrada")
        
        return data_inicio, data_fim

    def _extract_consumo_data(self, field_map: Dict, line_items: List, tables: List) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Extract consumption data (previous reading, current reading, consumption)"""
        leitura_anterior = None
        leitura_atual = None
        consumo_kwh = None
        consumo_ponta = 0
        consumo_fora_ponta = 0
        
        # Try to find readings in summary fields
        # Note: In the sample JSON, "Leitura Anterior" and "Leitura Atual" contain dates, not values
        # We need to look in tables or line items for actual meter readings
        
        # Search in tables for meter reading data
        for table in tables:
            # Check if this is a meter reading table
            if len(table) > 0:
                header_row = ' '.join(table[0]).upper()
                
                # Look for meter reading table (Medidor, Grandezas, Leitura Anterior, Leitura Atual, Consumo)
                if 'MEDIDOR' in header_row and 'LEITURA ANTERIOR' in header_row and 'LEITURA ATUAL' in header_row:
                    # Find the column index for "Consumo kWh"
                    consumo_col_idx = None
                    for idx, col in enumerate(table[0]):
                        if 'CONSUMO' in str(col).upper() and ('KWH' in str(col).upper() or 'KW' in str(col).upper()):
                            consumo_col_idx = idx
                            break
                    
                    for i, row in enumerate(table[1:], 1):
                        if len(row) > consumo_col_idx if consumo_col_idx else len(row) >= 7:
                            try:
                                # Extract readings
                                leit_ant = self._parse_number(row[3]) if len(row) > 3 else None
                                leit_atu = self._parse_number(row[4]) if len(row) > 4 else None
                                
                                # Use the consumo column if found, otherwise column 6
                                consumo = None
                                if consumo_col_idx is not None and len(row) > consumo_col_idx:
                                    consumo = self._parse_number(row[consumo_col_idx])
                                elif len(row) > 6:
                                    consumo = self._parse_number(row[6])
                                
                                if consumo:
                                    # Sum all consumo values from this table
                                    consumo_kwh = (consumo_kwh or 0) + consumo
                                    
                                    self.raw_snippets.append({
                                        'campo': 'consumo_medidor',
                                        'trecho': f"{row[1] if len(row) > 1 else 'Medidor'}: {consumo} kWh",
                                        'confidence_ocr': 0
                                    })
                                
                                # Store first valid readings
                                if leit_ant is not None and leitura_anterior is None:
                                    leitura_anterior = leit_ant
                                if leit_atu is not None and leitura_atual is None:
                                    leitura_atual = leit_atu
                                
                            except Exception as e:
                                continue
                
                # Look for consumption in "Itens da Fatura" table (only if not found in meter table)
                elif 'ITENS' in header_row and 'FATURA' in header_row and consumo_kwh is None:
                    for row in table[1:]:
                        if len(row) >= 3:
                            item_desc = str(row[0]).upper()
                            # Look for CONSUMO ATIVO items with quantity in kWh
                            if 'CONSUMO' in item_desc and 'KWH' in str(row[1]).upper():
                                try:
                                    # Quantity is usually in column 2
                                    quantidade = self._parse_number(row[2]) if len(row) > 2 else None
                                    if quantidade:
                                        # Check for variations of PONTA and FORA PONTA
                                        is_ponta = 'PONTA' in item_desc
                                        is_fora_ponta = any(x in item_desc for x in ['FORA PONTA', 'FORA DE PONTA', 'FPONTA', 'F PONTA', 'F.PONTA'])
                                        
                                        if is_ponta and not is_fora_ponta:
                                            consumo_ponta += quantidade
                                        elif is_fora_ponta:
                                            consumo_fora_ponta += quantidade
                                        
                                        self.raw_snippets.append({
                                            'campo': 'consumo_item',
                                            'trecho': f"{row[0]}: {quantidade} kWh",
                                            'confidence_ocr': 0
                                        })
                                except Exception:
                                    continue
        
        # Calculate total consumption from ponta + fora ponta
        if consumo_ponta > 0 or consumo_fora_ponta > 0:
            consumo_kwh = consumo_ponta + consumo_fora_ponta
            self.raw_snippets.append({
                'campo': 'consumo_total',
                'trecho': f"Ponta: {consumo_ponta} kWh + Fora Ponta: {consumo_fora_ponta} kWh = {consumo_kwh} kWh",
                'confidence_ocr': 0
            })
        
        # If still not found, search in line items
        if consumo_kwh is None:
            for item in line_items:
                for key, value in item.items():
                    if 'CONSUMO' in key.upper() or 'KWH' in key.upper():
                        consumo_kwh = self._parse_number(value)
                        if consumo_kwh:
                            self.raw_snippets.append({
                                'campo': 'consumo_kwh',
                                'trecho': f"{key}: {value}",
                                'confidence_ocr': 0
                            })
                            break
        
        if consumo_kwh is None:
            self.warnings.append("Consumo em kWh não encontrado")
        
        return leitura_anterior, leitura_atual, consumo_kwh

    def _extract_valor_total(self, field_map: Dict) -> Optional[float]:
        """Extract total value"""
        value, conf = self._find_field(field_map, self.KEYWORDS['valor_total'])
        if value:
            return self._parse_currency(value)
        self.warnings.append("Valor total não encontrado")
        return None

    def _extract_tarifa_classe(self, field_map: Dict) -> Optional[str]:
        """Extract tariff class"""
        value, conf = self._find_field(field_map, self.KEYWORDS['tarifa_classe'])
        if value:
            # Try to match known classes
            for classe, pattern in self.CLASSE_PATTERNS.items():
                if re.search(pattern, value.upper()):
                    return classe
            return value  # Return raw value if no pattern matches
        return None

    def _extract_demanda_contratada(self, field_map: Dict) -> Optional[float]:
        """Extract contracted demand"""
        value, conf = self._find_field(field_map, self.KEYWORDS['demanda_contratada'])
        if value:
            return self._parse_number(value)
        return None

    def _extract_demanda_registrada(self, field_map: Dict) -> Optional[float]:
        """Extract registered demand"""
        value, conf = self._find_field(field_map, self.KEYWORDS['demanda_registrada'])
        if value:
            return self._parse_number(value)
        return None

    def _extract_componentes(self, field_map: Dict, line_items: List, tables: List) -> Dict:
        """Extract component details (energia, TUSD, bandeira, impostos)"""
        componentes = {
            'energia': None,
            'tusd_tust': None,
            'bandeira': None,
            'bandeira_valor': None,
            'icms': None,
            'pis': None,
            'cofins': None,
            'outros': []
        }
        
        # Extract ICMS
        value, conf = self._find_field(field_map, self.KEYWORDS['icms'])
        if value:
            componentes['icms'] = self._parse_currency(value)
        
        # Extract PIS
        value, conf = self._find_field(field_map, self.KEYWORDS['pis'])
        if value:
            componentes['pis'] = self._parse_currency(value)
        
        # Extract COFINS
        value, conf = self._find_field(field_map, self.KEYWORDS['cofins'])
        if value:
            componentes['cofins'] = self._parse_currency(value)
        
        # Extract energia
        value, conf = self._find_field(field_map, self.KEYWORDS['energia'])
        if value:
            componentes['energia'] = self._parse_currency(value)
        
        # Extract TUSD/TUST
        value, conf = self._find_field(field_map, self.KEYWORDS['tusd'])
        if value:
            componentes['tusd_tust'] = self._parse_currency(value)
        
        # Extract bandeira
        value, conf = self._find_field(field_map, self.KEYWORDS['bandeira'])
        if value:
            for bandeira, pattern in self.BANDEIRA_PATTERNS.items():
                if re.search(pattern, value.upper()):
                    componentes['bandeira'] = bandeira
                    break
        
        # Search in tables for component details
        for table in tables:
            for row in table:
                if len(row) >= 2:
                    desc = row[0].upper()
                    valor_text = row[-1]  # Last column usually has values
                    
                    # Try to match components
                    if 'ENERGIA' in desc and not componentes['energia']:
                        componentes['energia'] = self._parse_currency(valor_text)
                    elif 'TUSD' in desc or 'TUST' in desc:
                        componentes['tusd_tust'] = self._parse_currency(valor_text)
                    elif 'BANDEIRA' in desc:
                        componentes['bandeira_valor'] = self._parse_currency(valor_text)
                        # Try to extract bandeira type
                        for bandeira, pattern in self.BANDEIRA_PATTERNS.items():
                            if re.search(pattern, desc):
                                componentes['bandeira'] = bandeira
                                break
                    elif 'ICMS' in desc and not componentes['icms']:
                        componentes['icms'] = self._parse_currency(valor_text)
                    elif 'PIS' in desc and not componentes['pis']:
                        componentes['pis'] = self._parse_currency(valor_text)
                    elif 'COFINS' in desc and not componentes['cofins']:
                        componentes['cofins'] = self._parse_currency(valor_text)
                    else:
                        # Other components
                        valor = self._parse_currency(valor_text)
                        if valor and valor > 0:
                            componentes['outros'].append({
                                'nome': row[0],
                                'valor': valor
                            })
        
        return componentes

    def _normalize_date(self, date_str: str) -> Optional[str]:
        """Normalize date to YYYY-MM-DD format"""
        if not date_str:
            return None
        
        # Try common Brazilian date formats
        patterns = [
            r'(\d{2})/(\d{2})/(\d{4})',  # DD/MM/YYYY
            r'(\d{2})-(\d{2})-(\d{4})',  # DD-MM-YYYY
        ]
        
        for pattern in patterns:
            match = re.search(pattern, date_str)
            if match:
                day, month, year = match.groups()
                try:
                    date_obj = datetime(int(year), int(month), int(day))
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        
        self.warnings.append(f"Formato de data não reconhecido: {date_str}")
        return None

    def _parse_currency(self, value_str: str) -> Optional[float]:
        """Parse Brazilian currency format to float"""
        if not value_str:
            return None
        
        # Remove currency symbols and spaces
        value_str = re.sub(r'[R$\s]', '', value_str)
        
        # Replace Brazilian decimal format (1.234,56 -> 1234.56)
        value_str = value_str.replace('.', '').replace(',', '.')
        
        try:
            return float(value_str)
        except ValueError:
            return None

    def _parse_number(self, value_str: str) -> Optional[float]:
        """Parse number from string"""
        if not value_str:
            return None
        
        # Remove non-numeric characters except dots and commas
        value_str = re.sub(r'[^\d.,\-]', '', value_str)
        
        # Handle Brazilian format
        if ',' in value_str and '.' in value_str:
            # Format: 1.234,56
            value_str = value_str.replace('.', '').replace(',', '.')
        elif ',' in value_str:
            # Format: 1234,56
            value_str = value_str.replace(',', '.')
        
        try:
            return float(value_str)
        except ValueError:
            return None

    def _extract_numbers(self, text: str) -> List[float]:
        """Extract all numbers from text"""
        numbers = []
        # Find all number patterns
        patterns = re.findall(r'[\d.,]+', text)
        for pattern in patterns:
            num = self._parse_number(pattern)
            if num is not None:
                numbers.append(num)
        return numbers

    def _calculate_confidence(self, result: Dict, field_map: Dict) -> str:
        """Calculate overall confidence level"""
        # Count how many critical fields were extracted
        critical_fields = [
            'unidade_consumidora_id',
            'identificador_fatura',
            'valor_total',
            'data_vencimento'
        ]
        
        found_count = sum(1 for field in critical_fields if result.get(field) is not None)
        
        # Calculate average confidence from field_map
        confidences = [data['value_confidence'] for data in field_map.values() if data['value_confidence'] > 0]
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0
        
        # Determine overall confidence
        if found_count >= 4 and avg_confidence >= 80:
            return 'high'
        elif found_count >= 3 and avg_confidence >= 60:
            return 'medium'
        else:
            return 'low'


def normalize_invoice_from_json(input_data: Dict) -> Dict:
    """
    Convenience function to normalize invoice data
    
    Args:
        input_data: Dictionary with arquivo_id, arquivo_nome, ocr_json, raw_text
        
    Returns:
        Normalized invoice JSON
    """
    normalizer = InvoiceNormalizer()
    return normalizer.normalize_invoice(input_data)


if __name__ == '__main__':
    # Example usage
    import json
    
    # Load sample JSON
    with open('output/20251105_213744_EMG.json', 'r', encoding='utf-8') as f:
        ocr_json = json.load(f)
    
    input_data = {
        'arquivo_id': '123',
        'arquivo_nome': '20251105_213744_EMG.pdf',
        'ocr_json': ocr_json,
        'raw_text': ''
    }
    
    result = normalize_invoice_from_json(input_data)
    
    print(json.dumps(result, indent=2, ensure_ascii=False))
