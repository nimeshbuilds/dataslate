FROM node:22-alpine AS build
WORKDIR /build
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.29-alpine
USER root
COPY --from=build /build/dist /usr/share/nginx/html
COPY nginx.conf.template /opt/dataslate/nginx.conf.template
COPY docker-entrypoint.sh /opt/dataslate/start.sh
RUN chmod 0555 /opt/dataslate/start.sh
USER 101:101
ENV DATASLATE_API_UPSTREAM=http://dataslate-api:8080
EXPOSE 8080
ENTRYPOINT ["/opt/dataslate/start.sh"]
