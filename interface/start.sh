#!/bin/sh

# Valores padrão caso não sejam passados no Docker Run
IP_NORTE=${PEER_NORTE:-localhost:8081}
IP_SUL=${PEER_SUL:-localhost:8082}
IP_LESTE=${PEER_LESTE:-localhost:8083}

echo "Substituindo IPs no Dashboard..."
echo "NORTE -> $IP_NORTE"
echo "SUL   -> $IP_SUL"
echo "LESTE -> $IP_LESTE"

# Substitui os placeholders pelos IPs reais
sed -i "s|__IP_NORTE__|${IP_NORTE}|g" /usr/share/nginx/html/index.html
sed -i "s|__IP_SUL__|${IP_SUL}|g" /usr/share/nginx/html/index.html
sed -i "s|__IP_LESTE__|${IP_LESTE}|g" /usr/share/nginx/html/index.html

echo "Iniciando Nginx na porta 80..."
exec nginx -g "daemon off;"