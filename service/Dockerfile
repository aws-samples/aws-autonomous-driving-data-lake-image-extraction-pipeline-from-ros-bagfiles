FROM amazonlinux

RUN yum install -y python3-pip wget tar xz
RUN wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz
RUN tar -xvf ffmpeg-release-amd64-static.tar.xz
RUN ln -s `pwd`/ffmpeg-5.0.1-amd64-static/ffmpeg /usr/bin/ffmpeg
COPY ./app/requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

COPY ./app/main.py /app/main.py
COPY ./app/bagstream.py /app/bagstream.py

WORKDIR /app

ENTRYPOINT ["python3", "main.py"]