FROM hadoop-bda:3.3.6

HEALTHCHECK CMD curl -f http://localhost:8088/ || exit 1
USER root
ADD run.sh /run.sh
RUN chmod a+x run.sh

EXPOSE 8088

CMD ["./run.sh"]