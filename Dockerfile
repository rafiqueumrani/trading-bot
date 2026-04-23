FROM python:3.11-slim 
WORKDIR /app 
RUN pip install --no-cache-dir -r requirements.txt 
COPY . . 
CMD ["python", "-c", "import main; main.main()"] 
