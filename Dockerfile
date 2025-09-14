FROM rockylinux:9

RUN dnf -y install epel-release \
    python3-pyyaml python3-pip

RUN groupadd -g 2000 scb && useradd -g 2000 -u 2000 scb

RUN mkdir /app && chown scb:scb /app
WORKDIR /app

USER scb

RUN pip3 install ccxt==4.4.90

COPY bot.py /app

CMD ["python3", "-u", "bot.py"]
