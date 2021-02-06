FROM python:latest
# Download latest listing of available packages:
RUN apt-get -y update
# Upgrade already installed packages:
RUN apt-get -y upgrade
# Install required packages
RUN pip install numpy # cartopy build failing https://github.com/SciTools/cartopy/issues/1552
RUN apt-get -y install libproj-dev proj-data proj-bin libgeos-dev
# App
WORKDIR /code
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
#COPY app.py app.py
#CMD ["python", "-u", "app.py"]
#CMD ["sh", "-c", "echo $INPUT_FILE; echo $OUTPUT_FILE; python $INPUT_FILE $OUTPUT_FILE"]