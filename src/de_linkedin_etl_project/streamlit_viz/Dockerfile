FROM python:3.11-slim

WORKDIR /app

#COPY ./src/de_linkedin_etl_project/streamlit_viz/requirements.txt .

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#COPY streamlit files to container
COPY . .


# Run Streamlit app
CMD ["streamlit", "run", "job_viz.py", "--server.enableCORS=false", "--server.port=9090"]
