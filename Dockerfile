FROM ubuntu:22.04
MAINTAINER Martin Dobias "martin.dobias@lutraconsulting.co.uk"

# this is to do choice of timezone upfront, because when "tzdata" package gets installed,
# it comes up with interactive command line prompt when package is being set up
ENV TZ=Europe/London
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && apt-get install -y \
    libsqlite3-mod-spatialite \
    python3-pip \
 && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip

WORKDIR /mergin-work-packages
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY mergin_work_packages.py .
COPY workpackages ./workpackages

ENTRYPOINT ["python3", "mergin_work_packages.py"]
