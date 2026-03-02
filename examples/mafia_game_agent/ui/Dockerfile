FROM node:20-bookworm-slim

WORKDIR /app

ENV NODE_ENV=development

# Use linux-native optional deps (tailwind oxide) instead of reusing host lock metadata.
COPY package.json ./
RUN npm install --include=optional

COPY . .

EXPOSE 5173

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]
