FROM node:18-alpine

# Install git so npm can fetch git-based dependencies
RUN apk add --no-cache git

WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install --production

# Copy the rest of the project files
COPY . .

EXPOSE 8080

CMD ["npm", "start"]
