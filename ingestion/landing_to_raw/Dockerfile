#Using offical maven image as a parent image
FROM python

WORKDIR /WORKING_DIR

#Copy the current directory contents into the container at current directory
# COPY requirements.txt requirements.txt
COPY landing_to_raw.py landing_to_raw.py
# COPY general_functions.py general_functions.py

# #Install the mvn command for maven 
# RUN pip install s3fs

CMD  ["python", "landing_to_raw.py"]