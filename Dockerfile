FROM node:18-alpine

WORKDIR /app

# Package files copy and install
COPY package*.json ./
RUN npm install --production

# Copy all repository files
COPY . .

EXPOSE 8080

CMD ["npm", "start"]
