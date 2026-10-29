FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
# CPU-only torch wheels: the default Linux wheels bundle CUDA and are several GB larger
RUN pip install --no-cache-dir -r requirements.txt --extra-index-url https://download.pytorch.org/whl/cpu

COPY . .

CMD ["streamlit", "run", "src/ui.py", "--server.port=8501", "--server.address=0.0.0.0"]
