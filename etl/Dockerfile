FROM python:3.11-slim

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml .

RUN uv pip install --system .

COPY etl_sales.py .

COPY sales_data_sample.csv .

COPY . .

CMD ["uv", "run", "python", "etl_sales.py"]