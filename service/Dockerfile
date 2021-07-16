FROM ros:melodic-perception-bionic

RUN rm /bin/sh; ln -s /bin/bash /bin/sh
RUN apt update && apt-get install python3-pip unzip curl vim ffmpeg -y 
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN sudo ./aws/install


RUN apt install git -y
RUN apt-get install ros-melodic-pcl-conversions -y
RUN apt-get install ros-melodic-pcl-ros -y


# Install Python3
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"

# modifications so we can install dependencies for efs-utils later
RUN sed 's/bionic-security main restricted/bionic-security main/' /etc/apt/sources.list > a
RUN mv a /etc/apt/sources.list

RUN apt-get update
RUN apt-get install -y wget && rm -rf /var/lib/apt/lists/*

COPY ./app/requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

# Build efs-utils from source - no package available for ubuntu
RUN sudo apt-get -y install git binutils
RUN git clone https://github.com/aws/efs-utils
WORKDIR ./efs-utils
RUN ./build-deb.sh
RUN apt-get update
RUN sudo apt-get -y install ./build/amazon-efs-utils*deb
#enable cloudwatch logs
RUN sudo sed -i -e '/\[cloudwatch-log\]/{N;s/# enabled = true/enabled = true/}' /etc/amazon/efs/efs-utils.conf
#RUN sudo sed -i  's/#region = us-east-1/region=eu-west-1/' /etc/amazon/efs/efs-utils.conf

COPY ./app/parse-file.sh /app/parse-file.sh
COPY ./app/engine.py /app/engine.py
COPY ./app/main.py /app/main.py
COPY ./app/export.launch /app/export.launch
COPY ./app/run.sh /app/run.sh

WORKDIR /app

ENTRYPOINT ["/app/run.sh"]
