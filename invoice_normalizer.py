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
        'pis': ['PIS', 'PASEP'],
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
        data_inicio, data_fim = self._extract_periodo_leitura(field_map, tables)
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
        """Find a field by matching keywords (partial match, prioritizing exact matches)"""
        # First pass: try exact matches
        for label, data in field_map.items():
            label_normalized = label.upper()
            
            for keyword in keywords:
                keyword_normalized = keyword.upper()
                # Check for exact match
                if label_normalized == keyword_normalized:
                    original_label = data.get('original_label', label)
                    self.raw_snippets.append({
                        'campo': keywords[0],
                        'trecho': f"{original_label}: {data['value']}",
                        'confidence_ocr': data['value_confidence']
                    })
                    return data['value'], data['value_confidence']
        
        # Second pass: try partial matches (keyword in label)
        for label, data in field_map.items():
            label_normalized = label.upper()
            
            for keyword in keywords:
                keyword_normalized = keyword.upper()
                # Check if keyword is in label (partial match)
                if keyword_normalized in label_normalized:
                    original_label = data.get('original_label', label)
                    self.raw_snippets.append({
                        'campo': keywords[0],
                        'trecho': f"{original_label}: {data['value']}",
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

    def _extract_periodo_leitura(self, field_map: Dict, tables: List = None) -> Tuple[Optional[str], Optional[str]]:
        """Extract reading period (start and end dates)"""
        if tables is None:
            tables = []
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
        
        if not data_inicio or not data_fim:
            for label, data in field_map.items():
                if 'DATAS DE LEITURA' in label.upper() or 'DATAS DE' in label.upper():
                    value = data['value']
                    anterior_match = re.search(r'Anterior[:\s]+(\d{2}/\d{2}(?:/\d{4})?)', value, re.IGNORECASE)
                    atual_match = re.search(r'Atual[:\s]+(\d{2}/\d{2}(?:/\d{4})?)', value, re.IGNORECASE)
                    
                    if not anterior_match or not atual_match:
                        # Look for dates after the keywords line
                        lines = value.split('\n')
                        if len(lines) >= 2:
                            # Check if first line has keywords
                            if 'ANTERIOR' in lines[0].upper() and 'ATUAL' in lines[0].upper():
                                # Extract dates from second line
                                dates = re.findall(r'\d{2}/\d{2}(?:/\d{4})?', lines[1])
                                if len(dates) >= 2:
                                    if not anterior_match:
                                        anterior_match = type('obj', (object,), {'group': lambda self, n: dates[0]})()
                                    if not atual_match:
                                        atual_match = type('obj', (object,), {'group': lambda self, n: dates[1]})()

                    
                    if anterior_match and not data_inicio:
                        date_str = anterior_match.group(1)
                        # Add year if missing (assume current year from reference month)
                        if len(date_str) <= 5:  # DD/MM format
                            # Try to get year from "Referente a" field
                            year = self._extract_year_from_reference(field_map)
                            if year:
                                date_str = f"{date_str}/{year}"
                        data_inicio = self._normalize_date(date_str)
                    
                    if atual_match and not data_fim:
                        date_str = atual_match.group(1)
                        # Add year if missing
                        if len(date_str) <= 5:  # DD/MM format
                            year = self._extract_year_from_reference(field_map)
                            if year:
                                date_str = f"{date_str}/{year}"
                        data_fim = self._normalize_date(date_str)
                    
                    break
        
        # If still not found, search in tables
        if (not data_inicio or not data_fim) and tables:
            for table in tables:
                if len(table) < 2:
                    continue
                
                # Check if table has reading dates (format: [['Leitura Anterior', '31/10/2024'], ...])
                for row in table:
                    if len(row) >= 2:
                        label = str(row[0]).upper()
                        value = str(row[1]) if len(row) > 1 else ''
                        
                        if 'LEITURA ANTERIOR' in label and not data_inicio:
                            data_inicio = self._normalize_date(value)
                            if data_inicio:
                                self.raw_snippets.append({
                                    'campo': 'data_inicio_tabela',
                                    'trecho': f"{row[0]}: {value}",
                                    'confidence_ocr': 0
                                })
                        
                        if 'LEITURA ATUAL' in label and not data_fim:
                            data_fim = self._normalize_date(value)
                            if data_fim:
                                self.raw_snippets.append({
                                    'campo': 'data_fim_tabela',
                                    'trecho': f"{row[0]}: {value}",
                                    'confidence_ocr': 0
                                })
                
                if data_inicio and data_fim:
                    break
        
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
        
        # Define generic patterns for energy consumption detection
        ENERGY_KEYWORDS = ['CONSUMO', 'ENERGIA', 'COMPONENTE', 'TUSD', 'TUST', 'ENCARGO', 'ACL']
        PONTA_KEYWORDS = ['PONTA', ' HP', 'HP ', 'HORÁRIO PONTA', 'HORARIO PONTA']
        FORA_PONTA_KEYWORDS = ['FORA PONTA', 'FPONTA', 'F PONTA', 'F.PONTA', 'HFP', 'FORA DE PONTA']
        EXCLUDE_KEYWORDS = ['DESC', 'DESCONTO', 'CREDITO', 'CREDIT', 'AJUSTE', 'DEMANDA', 'REATIVA']
        
        # Search in tables for consumption data
        for table in tables:
            # Check if this is a meter reading table
            if len(table) > 0:
                header_row = ' '.join(table[0]).upper()
                
                # Try to extract consumption using generic table analysis
                if consumo_kwh is None and len(table) > 1:
                    result = self._extract_from_generic_table(
                        table, ENERGY_KEYWORDS, PONTA_KEYWORDS, 
                        FORA_PONTA_KEYWORDS, EXCLUDE_KEYWORDS
                    )
                    
                    if result:
                        consumo_ponta += result.get('ponta', 0)
                        consumo_fora_ponta += result.get('fora_ponta', 0)
                        if result.get('total'):
                            consumo_kwh = (consumo_kwh or 0) + result['total']
                        
                        # Add snippets
                        for snippet in result.get('snippets', []):
                            self.raw_snippets.append(snippet)
        
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

    def _extract_from_generic_table(self, table: List, energy_keywords: List, 
                                     ponta_keywords: List, fora_ponta_keywords: List,
                                     exclude_keywords: List) -> Optional[Dict]:
        """
        Generic table parser that works for any invoice format.
        Automatically detects columns and extracts energy consumption.
        
        Returns dict with: {'ponta': float, 'fora_ponta': float, 'total': float, 'snippets': list}
        """
        if len(table) < 2:
            return None
        
        header = [str(col).upper() for col in table[0]]
        
        # Find relevant columns dynamically
        desc_col = None  # Description/Product/Item column
        unit_col = None  # Unit column (kWh)
        value_cols = []  # Quantity/Registered/Billed columns
        
        for idx, col in enumerate(header):
            col_clean = col.strip()
            
            # Description column (first priority)
            if desc_col is None and any(kw in col_clean for kw in ['DESCRI', 'PRODUTO', 'ITEM', 'GRANDEZA']):
                desc_col = idx
            
            # Unit column
            if unit_col is None and any(kw in col_clean for kw in ['UNID', 'UN.', 'U.M.']):
                unit_col = idx
            
            # Value columns (quantity, registered, billed, consumption)
            if any(kw in col_clean for kw in ['QUANT', 'REGISTRADO', 'FATURADO', 'CONSUMO', 'MEDIDO']):
                # Prefer "Consumo" > "Faturado" > "Registrado"
                priority = 3 if 'CONSUMO' in col_clean else 2 if 'FATURADO' in col_clean else 1 if 'REGISTRADO' in col_clean else 0
                value_cols.append((idx, priority, col_clean))
        
        # Sort value columns by priority and use the best one
        if value_cols:
            value_cols.sort(key=lambda x: x[1], reverse=True)
            value_col = value_cols[0][0]
        else:
            return None
        
        # If no description column found by keywords, find it heuristically
        if desc_col is None:
            # Look for columns that likely contain descriptions (not numbers, not empty)
            # Check first few rows to determine which column has text content
            for idx in range(len(header)):
                col_name = header[idx].strip()
                # Skip columns that are clearly not descriptions
                if any(kw in col_name for kw in ['MEDIDOR', 'N°', 'Nº', 'NUMERO', 'NÚMERO', 'LEITURA', 'CONST']):
                    continue
                # Check if this column has text content in rows
                has_text = False
                for row in table[1:3]:  # Check first 2 rows
                    if len(row) > idx:
                        cell = str(row[idx]).strip()
                        # Check if cell contains text (not just numbers or empty)
                        if cell and not cell.replace('.', '').replace(',', '').replace('-', '').isdigit():
                            has_text = True
                            break
                if has_text:
                    desc_col = idx
                    break
        
        if desc_col is None:
            return None
        
        # Extract consumption from rows
        ponta = 0
        fora_ponta = 0
        total = 0
        snippets = []
        
        for row in table[1:]:
            if len(row) <= max(desc_col, value_col):
                continue
            
            desc = str(row[desc_col]).upper()
            unit = str(row[unit_col]).upper() if unit_col and len(row) > unit_col else ''
            
            # Skip if description is just a number (likely a meter number)
            desc_clean = desc.strip().replace('.', '').replace(',', '').replace('-', '')
            if desc_clean.isdigit():
                continue
            
            # Check if this row should be excluded
            if any(excl in desc for excl in exclude_keywords):
                continue
            
            # Check if this is an energy consumption row
            has_energy_keyword = any(kw in desc for kw in energy_keywords)
            has_kwh_unit = 'KWH' in unit or 'KW' in unit
            
            if not (has_energy_keyword or has_kwh_unit):
                continue
            
            # Extract value
            try:
                value = self._parse_number(row[value_col])
                if not value or value <= 0:
                    continue
                
                # Determine if it's ponta or fora ponta
                is_fora_ponta = any(kw in desc for kw in fora_ponta_keywords)
                is_ponta = any(kw in desc for kw in ponta_keywords) and not is_fora_ponta
                
                if is_ponta:
                    ponta += value
                elif is_fora_ponta:
                    fora_ponta += value
                else:
                    total += value
                
                snippets.append({
                    'campo': 'consumo_generico',
                    'trecho': f"{row[desc_col]}: {value} kWh",
                    'confidence_ocr': 0
                })
                
            except Exception:
                continue
        
        # Return results if any consumption was found
        if ponta > 0 or fora_ponta > 0 or total > 0:
            return {
                'ponta': ponta,
                'fora_ponta': fora_ponta,
                'total': total,
                'snippets': snippets
            }
        
        return None

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
            # Check if this table has tax columns
            if len(table) > 1:
                # Check if first row is empty (header might be in row 1)
                first_row_empty = all(not str(cell).strip() for cell in table[0])
                header_row_idx = 1 if first_row_empty and len(table) > 2 else 0
                header_row = [str(col).upper() for col in table[header_row_idx]]
                
                # Look for tables with ICMS and PIS/COFINS columns (EDP format)
                icms_col_idx = None
                pis_cofins_col_idx = None
                
                # Look for separate ICMS, PIS, COFINS columns (CPFL format)
                icms_sep_idx = None
                pis_sep_idx = None
                cofins_sep_idx = None
                
                for idx, col in enumerate(header_row):
                    col_upper = str(col).upper()
                    
                    # EDP format: combined PIS/COFINS column
                    if ('PIS/COFINS' in col_upper or 'PIS / COFINS' in col_upper) and pis_cofins_col_idx is None:
                        pis_cofins_col_idx = idx
                    
                    # EDP format: ICMS with currency indicator
                    if 'ICMS' in col_upper and '(' in col_upper and 'R$' in col_upper and icms_col_idx is None:
                        icms_col_idx = idx
                    
                    # Generic format: separate tax columns (CPFL, CEMIG, etc)
                    # ICMS column (not base calculation, not aliquot, not percentage)
                    if 'ICMS' in col_upper and icms_sep_idx is None:
                        if 'BASE' not in col_upper and 'ALIQ' not in col_upper and 'CALC' not in col_upper:
                            # Prefer columns with currency indicators or just "ICMS"
                            if col_upper.strip() == 'ICMS' or 'R$' in col_upper or '(R$)' in col_upper:
                                icms_sep_idx = idx
                    
                    # PIS column (with percentage or currency indicator)
                    if 'PIS' in col_upper and pis_sep_idx is None:
                        if '%' in col_upper or 'R$' in col_upper or col_upper.strip() == 'PIS':
                            if 'BASE' not in col_upper and 'CALC' not in col_upper:
                                pis_sep_idx = idx
                    
                    # COFINS column (with percentage or currency indicator)
                    if 'COFINS' in col_upper and cofins_sep_idx is None:
                        if '%' in col_upper or 'R$' in col_upper or col_upper.strip() == 'COFINS':
                            if 'BASE' not in col_upper and 'CALC' not in col_upper:
                                cofins_sep_idx = idx
                
                # If we found tax columns, look for TOTAL/CONSOLIDADO row
                if (icms_col_idx is not None or pis_cofins_col_idx is not None or 
                    icms_sep_idx is not None or pis_sep_idx is not None or cofins_sep_idx is not None):
                    
                    # Collect all total rows with their values
                    total_rows = []
                    for row in table[header_row_idx + 1:]:
                        # Check if any column in the row contains "TOTAL" or "CONSOLIDADO"
                        row_text = ' '.join([str(cell).upper() for cell in row])
                        # Look for total/summary rows
                        is_total_row = any(keyword in row_text for keyword in ['TOTAL', 'CONSOLIDADO', 'SOMA'])
                        
                        if is_total_row:
                            # Check if this row has tax values
                            has_values = False
                            test_indices = [icms_col_idx, icms_sep_idx, pis_cofins_col_idx, pis_sep_idx, cofins_sep_idx]
                            for idx in test_indices:
                                if idx is not None and len(row) > idx:
                                    val = self._parse_currency(row[idx])
                                    if val and val > 0:
                                        has_values = True
                                        break
                            
                            if has_values:
                                # Prioritize "CONSOLIDADO" over other totals
                                priority = 2 if 'CONSOLIDADO' in row_text else 1
                                total_rows.append((priority, row))
                    
                    # Sort by priority (CONSOLIDADO first) and use the best row
                    if total_rows:
                        total_rows.sort(key=lambda x: x[0], reverse=True)
                        row = total_rows[0][1]
                        
                        # EDP format: Extract ICMS from TOTAL row
                        if icms_col_idx is not None and len(row) > icms_col_idx and not componentes['icms']:
                            icms_val = self._parse_currency(row[icms_col_idx])
                            if icms_val and icms_val > 0:
                                componentes['icms'] = icms_val
                        
                        # EDP format: Extract PIS/COFINS combined from TOTAL row
                        if pis_cofins_col_idx is not None and len(row) > pis_cofins_col_idx:
                            pis_cofins_val = self._parse_currency(row[pis_cofins_col_idx])
                            if pis_cofins_val and pis_cofins_val > 0:
                                # Split PIS/COFINS (typically 0.65% PIS + 3% COFINS = ~16.7% of total)
                                # Approximate: PIS ≈ 17.8% of total, COFINS ≈ 82.2% of total
                                if not componentes['pis'] and not componentes['cofins']:
                                    componentes['pis'] = round(pis_cofins_val * 0.178, 2)
                                    componentes['cofins'] = round(pis_cofins_val * 0.822, 2)
                        
                        # Generic format: Extract separate ICMS, PIS, COFINS from TOTAL row
                        if icms_sep_idx is not None and len(row) > icms_sep_idx and not componentes['icms']:
                            icms_val = self._parse_currency(row[icms_sep_idx])
                            if icms_val and icms_val > 0:
                                componentes['icms'] = icms_val
                        
                        if pis_sep_idx is not None and len(row) > pis_sep_idx and not componentes['pis']:
                            pis_val = self._parse_currency(row[pis_sep_idx])
                            if pis_val and pis_val > 0:
                                componentes['pis'] = pis_val
                        
                        if cofins_sep_idx is not None and len(row) > cofins_sep_idx and not componentes['cofins']:
                            cofins_val = self._parse_currency(row[cofins_sep_idx])
                            if cofins_val and cofins_val > 0:
                                componentes['cofins'] = cofins_val
            
            # Standard row-by-row search
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
                    elif ('PIS' in desc or 'PASEP' in desc) and not componentes['pis']:
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

    def _extract_year_from_reference(self, field_map: Dict) -> Optional[str]:
        """Extract year from 'Referente a' field (e.g., 'NOV/2024' -> '2024')"""
        for label, data in field_map.items():
            if 'REFERENTE' in label.upper():
                value = data['value']
                # Pattern: NOV/2024, 11/2024, etc.
                year_match = re.search(r'/(\d{4})', value)
                if year_match:
                    return year_match.group(1)
        return None

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
