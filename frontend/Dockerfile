# Usamos a imagem leve do Nginx para servir arquivos estáticos
FROM nginx:alpine

# 1. Definimos o diretório de trabalho do Nginx
WORKDIR /usr/share/nginx/html

# 2. Copiamos o seu arquivo HTML para o diretório do servidor
COPY index.html .

# 3. Criamos um script de entrada (entrypoint) diretamente no Dockerfile
# Este script substitui a string no HTML e depois inicia o Nginx
RUN echo '#!/bin/sh' > /entrypoint.sh && \
    echo 'sed -i "s|__PEERS_ENV__|${PEERS_LIST}|g" /usr/share/nginx/html/index.html' >> /entrypoint.sh && \
    echo 'exec nginx -g "daemon off;"' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

# 4. Expomos a porta 80
EXPOSE 80

# 5. Definimos uma variável de ambiente padrão (vazia ou exemplo)
ENV PEERS_LIST="NORTE=127.0.0.1:5000,SUL=127.0.0.1:5001"

# 6. Iniciamos através do script que faz a substituição
ENTRYPOINT ["/entrypoint.sh"]