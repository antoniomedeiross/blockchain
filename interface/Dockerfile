FROM nginx:alpine

# Copia o arquivo HTML da interface para o diretório raiz do Nginx
COPY dashbord.html /usr/share/nginx/html/index.html

# Copia o script de inicialização
COPY start.sh /start.sh
RUN chmod +x /start.sh

# A porta que o Nginx expõe
EXPOSE 80

# Executa o script que substitui as variáveis e inicia o servidor
CMD ["/start.sh"]