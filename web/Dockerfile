FROM node:22-alpine@sha256:0e1754758c2df4cfa3e68d59e2e1b7374bb7fd9ea6328eaaacf15cc934af8202 AS build
WORKDIR /build
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.29-alpine@sha256:0c79d56aee561a1d81c63f00eee5fb5fe29279560cdc55e91425133104c7fbe6
USER root
COPY --from=build /build/dist /usr/share/nginx/html
COPY nginx.conf.template /opt/dataslate/nginx.conf.template
COPY docker-entrypoint.sh /opt/dataslate/start.sh
RUN chmod 0555 /opt/dataslate/start.sh
USER 101:101
ENV DATASLATE_API_UPSTREAM=http://dataslate-api:8080
EXPOSE 8080
ENTRYPOINT ["/opt/dataslate/start.sh"]
