FROM node:18-alpine

WORKDIR /app

ENV NODE_ENV=development

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 5173

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]
