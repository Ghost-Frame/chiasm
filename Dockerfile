FROM node:22-slim AS build

WORKDIR /app

# Backend deps
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev 2>/dev/null || npm install --omit=dev

# Frontend deps and build
COPY frontend/package.json frontend/package-lock.json* frontend/
RUN cd frontend && npm ci 2>/dev/null || cd frontend && npm install
COPY frontend/ frontend/
RUN cd frontend && npm run build

# Copy backend source
COPY src/ src/
COPY tsconfig.json ./

FROM node:22-slim

LABEL org.opencontainers.image.title="Chiasm" \
      org.opencontainers.image.description="Multi-agent task coordination" \
      org.opencontainers.image.url="https://github.com/zanfiel/chiasm" \
      org.opencontainers.image.source="https://github.com/zanfiel/chiasm" \
      org.opencontainers.image.documentation="https://github.com/zanfiel/chiasm#readme" \
      org.opencontainers.image.licenses="Elastic-2.0" \
      org.opencontainers.image.vendor="Syntheos"

WORKDIR /app

COPY --from=build /app/node_modules node_modules/
COPY --from=build /app/frontend/build frontend/build/
COPY --from=build /app/src src/
COPY --from=build /app/tsconfig.json ./
COPY --from=build /app/package.json ./

RUN mkdir -p /app/data
VOLUME /app/data

EXPOSE 4300

ENV PORT=4300
ENV HOST=0.0.0.0
ENV DB_PATH=/app/data/chiasm.db

CMD ["node", "--experimental-strip-types", "src/server.ts"]
