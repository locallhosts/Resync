# The API URL has to be baked in at build time (Vite inlines
# import.meta.env.* at build, not at container start), so it's a build
# ARG here rather than a runtime environment variable. Override it with
# --build-arg VITE_API_URL=... if the API isn't at localhost:8080.
FROM node:22-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ARG VITE_API_URL=http://localhost:8080
ENV VITE_API_URL=$VITE_API_URL
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /src/dist /usr/share/nginx/html
