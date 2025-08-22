FROM python:3.11-slim

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY etl/pyproject.toml .
COPY etl/etl_sales.py .
COPY sales_data_sample.csv .
RUN uv pip install --system .

COPY . .

CMD ["uv", "run", "python", "etl_sales.py"]