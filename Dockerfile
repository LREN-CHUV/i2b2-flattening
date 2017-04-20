FROM hbpmip/python-base:a2b201e

MAINTAINER mirco.nasuti@chuv.ch

COPY i2b2_flattening/ /i2b2_flattening/
COPY data/ /data/
COPY requirements.txt /requirements.txt

RUN pip install -r requirements.txt

VOLUME /input_folder
VOLUME /output_folder

WORKDIR /
ENTRYPOINT ["python", "/i2b2_flattening/main.py", "/input_folder", "/output_folder"]
