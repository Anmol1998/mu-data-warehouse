FROM python:3

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port for the application
EXPOSE 5000

# Start the application
CMD ["python", "app.py"]