# Extrator OCR de Faturas

Aplicação Flask que extrai informações de PDFs de faturas usando AWS Textract.

## Requisitos

- Python 3.8+
- Conta AWS com acesso ao Textract
- Credenciais AWS configuradas
- Poppler instalado

## Instalação

### 1. Instalar dependências Python

```bash
pip install -r requirements.txt
```

### 2. Instalar Poppler

**Windows:**
```
Baixar de: https://github.com/oschwartz10612/poppler-windows/releases
Extrair para: C:\Program Files\poppler
Adicionar ao PATH: C:\Program Files\poppler\Library\bin
```

**Linux:**
```bash
sudo apt-get install poppler-utils
```

### 3. Configurar AWS

```bash
aws configure
```

Ou criar arquivo `~/.aws/credentials`:
```ini
[default]
aws_access_key_id = SUA_ACCESS_KEY
aws_secret_access_key = SUA_SECRET_KEY
```

## Como Usar

1. Iniciar o servidor:
```bash
python app.py
```

2. Acessar no navegador:
```
http://localhost:5000
```

3. Fazer upload de um PDF de fatura e visualizar os dados extraídos

## Estrutura do Projeto

```
tcc/
├── app.py              # Aplicação Flask
├── ocr.py              # Funções de processamento OCR
├── templates/
│   └── index.html      # Interface web
├── uploads/            # PDFs enviados
├── output/             # Dados extraídos (JSON)
└── requirements.txt    # Dependências
```