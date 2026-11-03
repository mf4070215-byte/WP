FROM node:20-bookworm-slim
# git is needed because some Baileys sub-dependencies resolve from git
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev && npm cache clean --force
COPY src ./src
ENV NODE_ENV=production \
    NODE_OPTIONS=--max-old-space-size=300
EXPOSE 8000
USER node
CMD ["node", "src/index.js"]
