FROM node:20-bookworm-slim

WORKDIR /app

ENV NODE_ENV=development

# Copy package files
COPY package.json ./

# Use linux-native optional deps (tailwind oxide) instead of reusing host lock metadata.
RUN npm install --include=optional

# Copy source code
COPY . .

# Expose port
EXPOSE 5173

# Start development server
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]
